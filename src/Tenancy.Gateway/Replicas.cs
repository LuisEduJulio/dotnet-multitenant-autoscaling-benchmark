using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace Tenancy.Gateway;

public enum ReplicaState { Provisioning, Starting, Ready, Draining, Stopped }

public sealed class Replica
{
    private int _outstanding;

    public required string Id { get; init; }
    public required int Port { get; init; }
    public Uri BaseUri => new($"http://127.0.0.1:{Port}");
    public ReplicaState State { get; set; } = ReplicaState.Provisioning;
    public Process? Process { get; set; }
    public DateTimeOffset RequestedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? ReadyAt { get; set; }

    /// <summary>Requisições que o gateway enviou e ainda não voltaram.</summary>
    public int Outstanding => Volatile.Read(ref _outstanding);
    public void Enter() => Interlocked.Increment(ref _outstanding);
    public void Leave() => Interlocked.Decrement(ref _outstanding);

    public ReplicaMetrics? LastMetrics { get; set; }
}

public sealed record ReplicaMetrics(string ReplicaId, int InFlight, int Queued, int Slots, double AvgServiceMs, long Completed, long Rejected);

public sealed record GatewayOptions
{
    public int InitialReplicas { get; init; } = 1;
    public bool AutoscalingEnabled { get; init; }
    public int MinReplicas { get; init; } = 1;
    public int MaxReplicas { get; init; } = 10;
    public double TargetLoadPerReplica { get; init; } = 100;
    public int EvaluationIntervalSeconds { get; init; } = 5;
    public int ScaleDownStabilizationSeconds { get; init; } = 60;

    /// <summary>Tempo para conseguir um lugar para a nova réplica (agendar o pod, puxar a imagem).</summary>
    public int ProvisioningDelaySeconds { get; init; } = 10;

    public string ApiAssemblyPath { get; init; } = "";
    public int FirstReplicaPort { get; init; } = 5100;

    /// <summary>Configuração repassada a cada réplica (Replica:Slots, Queue:Mode, Admission:Enabled...).</summary>
    public Dictionary<string, string> ReplicaSettings { get; init; } = new();

    public static GatewayOptions From(IConfiguration c)
    {
        var settings = new Dictionary<string, string>();
        foreach (var child in c.GetSection("ReplicaSettings").AsEnumerable(makePathsRelative: true))
            if (child.Value is not null) settings[child.Key] = child.Value;

        return new GatewayOptions
        {
            InitialReplicas = c.GetValue("Scaling:InitialReplicas", 1),
            AutoscalingEnabled = c.GetValue("Scaling:Enabled", false),
            MinReplicas = c.GetValue("Scaling:MinReplicas", 1),
            MaxReplicas = c.GetValue("Scaling:MaxReplicas", 10),
            TargetLoadPerReplica = c.GetValue("Scaling:TargetLoadPerReplica", 100d),
            EvaluationIntervalSeconds = c.GetValue("Scaling:EvaluationIntervalSeconds", 5),
            ScaleDownStabilizationSeconds = c.GetValue("Scaling:ScaleDownStabilizationSeconds", 60),
            ProvisioningDelaySeconds = c.GetValue("Scaling:ProvisioningDelaySeconds", 10),
            ApiAssemblyPath = c["Scaling:ApiAssemblyPath"] ?? "",
            FirstReplicaPort = c.GetValue("Scaling:FirstReplicaPort", 5100),
            ReplicaSettings = settings,
        };
    }
}

public sealed record ScaleEvent(double AtSeconds, string Kind, string ReplicaId, int ReadyReplicas, int DesiredReplicas, string Reason);

/// <summary>
/// Faz o papel do orquestrador (um "mini Kubernetes"): sobe processos da API como réplicas,
/// espera ficarem prontos, coloca no balanceamento e drena antes de desligar.
/// </summary>
public sealed class ReplicaManager : IAsyncDisposable
{
    private readonly GatewayOptions _options;
    private readonly ILogger<ReplicaManager> _logger;
    private readonly ConcurrentDictionary<string, Replica> _replicas = new();
    private readonly ConcurrentQueue<ScaleEvent> _events = new();
    private readonly HttpClient _probe = new() { Timeout = TimeSpan.FromSeconds(2) };
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private int _sequence;

    public ReplicaManager(GatewayOptions options, ILogger<ReplicaManager> logger)
    {
        _options = options;
        _logger = logger;
    }

    public int DesiredReplicas { get; set; }
    public double UptimeSeconds => _clock.Elapsed.TotalSeconds;
    public IReadOnlyCollection<ScaleEvent> Events => _events.ToArray();

    public IEnumerable<Replica> All => _replicas.Values.OrderBy(r => r.Port);
    public Replica[] Ready => _replicas.Values.Where(r => r.State == ReplicaState.Ready).ToArray();

    /// <summary>Réplicas que contam como "atuais" para o autoscaler: prontas + a caminho.</summary>
    public int CurrentCount => _replicas.Values.Count(r => r.State is ReplicaState.Provisioning or ReplicaState.Starting or ReplicaState.Ready);

    public void Record(string kind, string replicaId, string reason)
        => _events.Enqueue(new ScaleEvent(Math.Round(UptimeSeconds, 1), kind, replicaId, Ready.Length, DesiredReplicas, reason));

    public async Task StartInitialAsync(int count, CancellationToken ct)
    {
        DesiredReplicas = count;
        // As réplicas iniciais já existem antes do teste: sem tempo de provisionamento.
        await Task.WhenAll(Enumerable.Range(0, count).Select(_ => AddReplicaAsync(provisioningDelay: TimeSpan.Zero, "inicial", ct)));
    }

    public Task AddReplicaAsync(TimeSpan provisioningDelay, string reason, CancellationToken ct)
    {
        var n = Interlocked.Increment(ref _sequence);
        var replica = new Replica { Id = $"replica-{n}", Port = _options.FirstReplicaPort + n };
        _replicas[replica.Id] = replica;
        Record("scale-up-requested", replica.Id, reason);
        return Task.Run(() => BootAsync(replica, provisioningDelay, ct), ct);
    }

    private async Task BootAsync(Replica replica, TimeSpan provisioningDelay, CancellationToken ct)
    {
        try
        {
            if (provisioningDelay > TimeSpan.Zero) await Task.Delay(provisioningDelay, ct);
            replica.State = ReplicaState.Starting;

            var psi = new ProcessStartInfo("dotnet")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            psi.ArgumentList.Add(_options.ApiAssemblyPath);
            psi.ArgumentList.Add("--urls");
            psi.ArgumentList.Add(replica.BaseUri.ToString());
            psi.Environment["Replica__Id"] = replica.Id;
            psi.Environment["ASPNETCORE_ENVIRONMENT"] = "Production";
            psi.Environment["DOTNET_gcServer"] = "0";
            foreach (var (key, value) in _options.ReplicaSettings)
                psi.Environment[key.Replace(":", "__")] = value;

            var process = Process.Start(psi) ?? throw new InvalidOperationException("Falha ao iniciar a réplica.");
            process.OutputDataReceived += (_, _) => { };
            process.ErrorDataReceived += (_, e) => { if (e.Data is not null) _logger.LogWarning("[{Id}] {Line}", replica.Id, e.Data); };
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            replica.Process = process;

            // Readiness probe: só entra no balanceamento quando responde.
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    using var res = await _probe.GetAsync(new Uri(replica.BaseUri, "/health/ready"), ct);
                    if (res.IsSuccessStatusCode) break;
                }
                catch (HttpRequestException) { }
                catch (TaskCanceledException) when (!ct.IsCancellationRequested) { }
                await Task.Delay(100, ct);
            }

            replica.ReadyAt = DateTimeOffset.UtcNow;
            replica.State = ReplicaState.Ready;
            Record("ready", replica.Id, $"pronta em {(replica.ReadyAt.Value - replica.RequestedAt).TotalSeconds:F1}s");
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha ao subir {Id}", replica.Id);
            replica.State = ReplicaState.Stopped;
        }
    }

    public async Task RemoveReplicaAsync(string reason, CancellationToken ct)
    {
        // Remove a mais nova primeiro, como o ReplicaSet faz com pods mais recentes.
        var victim = _replicas.Values.Where(r => r.State == ReplicaState.Ready).OrderByDescending(r => r.Port).FirstOrDefault();
        if (victim is null) return;

        victim.State = ReplicaState.Draining; // sai do balanceamento na hora
        Record("scale-down", victim.Id, reason);

        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        while (victim.Outstanding > 0 && DateTimeOffset.UtcNow < deadline && !ct.IsCancellationRequested)
            await Task.Delay(100, ct);

        Kill(victim);
    }

    public async Task RefreshMetricsAsync(CancellationToken ct)
    {
        await Task.WhenAll(Ready.Select(async r =>
        {
            try
            {
                r.LastMetrics = await _probe.GetFromJsonAsync(new Uri(r.BaseUri, "/metrics"), GatewayJsonContext.Default.ReplicaMetrics, ct);
            }
            catch (Exception) when (!ct.IsCancellationRequested) { r.LastMetrics = null; }
        }));
    }

    private static void Kill(Replica replica)
    {
        replica.State = ReplicaState.Stopped;
        try { if (replica.Process is { HasExited: false } p) p.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
    }

    public ValueTask DisposeAsync()
    {
        foreach (var r in _replicas.Values) Kill(r);
        _probe.Dispose();
        return ValueTask.CompletedTask;
    }
}

public sealed record ReplicaView(string Id, string State, int Outstanding, int? InFlight, int? Queued, double? AvgServiceMs, long? Completed, long? Rejected);
public sealed record GatewayStatus(double UptimeSeconds, int ReadyReplicas, int CurrentReplicas, int DesiredReplicas, ReplicaView[] Replicas, ScaleEvent[] Events);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(ReplicaMetrics))]
[JsonSerializable(typeof(GatewayStatus))]
public partial class GatewayJsonContext : JsonSerializerContext;
