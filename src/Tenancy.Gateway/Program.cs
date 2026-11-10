using System.Net;
using Tenancy.Gateway;

var builder = WebApplication.CreateSlimBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.WebHost.ConfigureKestrel(k => k.Limits.MaxConcurrentConnections = null);

var options = GatewayOptions.From(builder.Configuration);
if (string.IsNullOrWhiteSpace(options.ApiAssemblyPath) || !File.Exists(options.ApiAssemblyPath))
    throw new InvalidOperationException($"Scaling:ApiAssemblyPath inválido: '{options.ApiAssemblyPath}'");

builder.Services.AddSingleton(options);
builder.Services.AddSingleton<ReplicaManager>();
builder.Services.AddHostedService<Autoscaler>();
builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.TypeInfoResolverChain.Insert(0, GatewayJsonContext.Default));

var app = builder.Build();
var replicas = app.Services.GetRequiredService<ReplicaManager>();
await replicas.StartInitialAsync(options.InitialReplicas, app.Lifetime.ApplicationStopping);
app.Lifetime.ApplicationStopping.Register(() => replicas.DisposeAsync().AsTask().GetAwaiter().GetResult());

// Cliente do proxy: sem limite de conexões por réplica, conexões reaproveitadas.
var forwarder = new HttpMessageInvoker(new SocketsHttpHandler
{
    UseProxy = false,
    AllowAutoRedirect = false,
    AutomaticDecompression = DecompressionMethods.None,
    UseCookies = false,
    MaxConnectionsPerServer = int.MaxValue,
    PooledConnectionIdleTimeout = TimeSpan.FromSeconds(30),
    ConnectTimeout = TimeSpan.FromSeconds(5),
});

app.MapGet("/gateway/status", () =>
{
    var views = replicas.All.Select(r => new ReplicaView(r.Id, r.State.ToString(), r.Outstanding,
        r.LastMetrics?.InFlight, r.LastMetrics?.Queued, r.LastMetrics?.AvgServiceMs, r.LastMetrics?.Completed, r.LastMetrics?.Rejected)).ToArray();
    return Results.Json(new GatewayStatus(Math.Round(replicas.UptimeSeconds, 1), replicas.Ready.Length, replicas.CurrentCount,
        replicas.DesiredReplicas, views, replicas.Events.ToArray()), GatewayJsonContext.Default.GatewayStatus);
});

// Proxy reverso com balanceamento "menos requisições pendentes" (least outstanding requests).
app.Map("/api/{**path}", async (HttpContext http) =>
{
    var ready = replicas.Ready;
    if (ready.Length == 0)
    {
        http.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
        http.Response.Headers["X-Rejected-Reason"] = "no-replica";
        return;
    }

    var target = ready[0];
    for (var i = 1; i < ready.Length; i++)
        if (ready[i].Outstanding < target.Outstanding) target = ready[i];

    var uri = new Uri(target.BaseUri, http.Request.Path + http.Request.QueryString);
    using var request = new HttpRequestMessage(new HttpMethod(http.Request.Method), uri);
    foreach (var header in http.Request.Headers)
    {
        if (header.Key.StartsWith("X-", StringComparison.OrdinalIgnoreCase))
            request.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray());
    }

    target.Enter();
    try
    {
        using var response = await forwarder.SendAsync(request, http.RequestAborted);
        http.Response.StatusCode = (int)response.StatusCode;
        foreach (var header in response.Headers)
            if (header.Key.StartsWith("X-", StringComparison.OrdinalIgnoreCase) || header.Key == "Retry-After")
                http.Response.Headers[header.Key] = header.Value.ToArray();
        if (response.Content.Headers.ContentType is { } ct) http.Response.ContentType = ct.ToString();
        await response.Content.CopyToAsync(http.Response.Body, http.RequestAborted);
    }
    catch (HttpRequestException) when (!http.Response.HasStarted)
    {
        http.Response.StatusCode = StatusCodes.Status502BadGateway;
    }
    catch (OperationCanceledException) when (http.RequestAborted.IsCancellationRequested)
    {
        // o cliente desistiu (timeout do lado dele)
    }
    finally
    {
        target.Leave();
    }
});

app.Run();
