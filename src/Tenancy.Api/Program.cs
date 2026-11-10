using System.Diagnostics;
using Tenancy.Api;
using Tenancy.Core;

var builder = WebApplication.CreateSlimBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);

// Lido do IConfiguration final (inclui variáveis de ambiente e configurações de teste).
builder.Services.AddSingleton(sp => ReplicaOptions.From(sp.GetRequiredService<IConfiguration>()));
builder.Services.AddSingleton(sp =>
{
    var o = sp.GetRequiredService<ReplicaOptions>();
    return new TenantScheduler(o.Slots, o.QueueMode);
});
builder.Services.AddSingleton<TenantCatalog>();
builder.Services.AddSingleton<ReplicaStats>();
builder.Services.AddSingleton<IDownstreamService, SimulatedDownstreamService>();
builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonContext.Default));

var app = builder.Build();

app.MapGet("/health/ready", () => Results.Text("ok"));

app.MapGet("/metrics", (TenantScheduler scheduler, ReplicaStats stats, ReplicaOptions o) =>
    Results.Json(new ReplicaMetrics(
        o.ReplicaId, scheduler.InFlight, scheduler.Queued, scheduler.Slots,
        stats.AverageServiceMs, stats.Completed, stats.Rejected), AppJsonContext.Default.ReplicaMetrics));

// Endpoint de negócio: cotação de itens para uma rede (tenant).
app.MapGet("/api/cotacao", async (HttpContext http, TenantCatalog catalog, TenantScheduler scheduler,
    ReplicaStats stats, ReplicaOptions o, IDownstreamService downstream) =>
{
    var received = Stopwatch.GetTimestamp();

    // 1) Resolução do tenant: sem tenant válido, nada é processado.
    var tenantId = http.Request.Headers["X-Tenant-Id"].ToString();
    if (string.IsNullOrWhiteSpace(tenantId))
        return Results.Problem("Cabeçalho X-Tenant-Id é obrigatório.", statusCode: StatusCodes.Status400BadRequest);
    if (!catalog.TryGet(tenantId, out var tenant))
        return Results.Problem($"Tenant '{tenantId}' não encontrado.", statusCode: StatusCodes.Status404NotFound);

    // 2) Controle de admissão: recusa já se a resposta não cabe no orçamento de tempo.
    if (o.AdmissionEnabled)
    {
        var ahead = scheduler.EstimateQueuedAhead(tenant.Id);
        var avg = stats.AverageServiceMs > 0 ? stats.AverageServiceMs : o.DownstreamLatencyMs;
        if (!AdmissionControl.ShouldAdmit(ahead, scheduler.Slots, avg, o.BudgetMs))
        {
            stats.RecordRejected();
            http.Response.Headers["Retry-After"] = "1";
            http.Response.Headers["X-Rejected-Reason"] = "deadline";
            return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
        }
    }

    // 3) Espera um slot da réplica (FIFO ou justo por tenant).
    using var lease = await scheduler.AcquireAsync(tenant.Id, http.RequestAborted);
    var started = Stopwatch.GetTimestamp();

    // 4) Processamento: estoque/ERP externo (I/O) + cálculo de preços do tenant.
    var skus = Skus.Parse(http.Request.Query["skus"], tenant.ProductCount);
    var stock = await downstream.GetStockAsync(tenant.Id, skus, http.RequestAborted);
    var items = new CotacaoItem[skus.Length];
    decimal total = 0;
    for (var i = 0; i < skus.Length; i++)
    {
        var price = tenant.PriceOf(skus[i]);
        items[i] = new CotacaoItem(skus[i], price, stock[i]);
        total += price;
    }

    var serviceMs = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
    stats.RecordCompleted(serviceMs);

    http.Response.Headers["X-Replica-Id"] = o.ReplicaId;
    http.Response.Headers["X-Queue-Ms"] = ((int)Stopwatch.GetElapsedTime(received, started).TotalMilliseconds).ToString();
    return Results.Json(new Cotacao(tenant.Id, o.ReplicaId, items, total), AppJsonContext.Default.Cotacao);
});

app.Run();

public partial class Program;
