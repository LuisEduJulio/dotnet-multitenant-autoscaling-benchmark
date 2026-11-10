using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;

// Gerador de carga em laço fechado: cada usuário virtual faz uma cotação, espera a resposta,
// "lê a tela" (think time) e faz a próxima. Assim 2.000 usuários = 2.000 pessoas de verdade.
if (args is ["--summarize", var resultsDir])
{
    Console.WriteLine(Summary.Markdown(resultsDir));
    return;
}

var opt = LoadOptions.Parse(args);
Console.WriteLine($"[{opt.Name}] perfil {string.Join(" → ", opt.Profile.Select(p => $"{p.AtSeconds}s:{p.Users}"))}, duração {opt.DurationSeconds}s, alvo {opt.Target}");

var handler = new SocketsHttpHandler
{
    UseProxy = false,
    MaxConnectionsPerServer = int.MaxValue,
    PooledConnectionIdleTimeout = TimeSpan.FromSeconds(60),
    ConnectTimeout = TimeSpan.FromSeconds(5),
    AutomaticDecompression = DecompressionMethods.None,
};
using var http = new HttpClient(handler) { BaseAddress = new Uri(opt.Target), Timeout = Timeout.InfiniteTimeSpan };

var samples = new ConcurrentQueue<Sample>();
var timeline = new ConcurrentQueue<ReplicaPoint>();
var clock = Stopwatch.StartNew();
var end = TimeSpan.FromSeconds(opt.DurationSeconds);
using var stop = new CancellationTokenSource(end + TimeSpan.FromSeconds(opt.RequestTimeoutSeconds + 5));

// Amostra o número de réplicas a cada segundo.
var poller = Task.Run(async () =>
{
    while (clock.Elapsed < end)
    {
        try
        {
            using var doc = JsonDocument.Parse(await http.GetStringAsync("/gateway/status"));
            timeline.Enqueue(new ReplicaPoint(Math.Round(clock.Elapsed.TotalSeconds, 1),
                doc.RootElement.GetProperty("readyReplicas").GetInt32(),
                doc.RootElement.GetProperty("currentReplicas").GetInt32()));
        }
        catch (Exception) { }
        await Task.Delay(1000);
    }
});

var users = opt.BuildUsers();
var tasks = users.Select((u, index) => Task.Run(() => RunUserAsync(u, index))).ToArray();
await Task.WhenAll(tasks);
await poller;

var report = Report.Build(opt, samples.ToArray(), timeline.ToArray());
Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(opt.Output))!);
await File.WriteAllTextAsync(opt.Output, JsonSerializer.Serialize(report, ReportJson.Default.Report));
Console.WriteLine(Report.Describe(report));

async Task RunUserAsync(VirtualUser user, int index)
{
    // Perfil de carga: o usuário de índice i só está "online" quando o perfil pede mais de i usuários.
    var random = new Random(index * 7919 + 17);
    while (clock.Elapsed < end)
    {
        if (index >= opt.ActiveUsersAt(clock.Elapsed.TotalSeconds))
        {
            await Task.Delay(200);
            continue;
        }

        var skus = string.Join(',', Enumerable.Range(0, 5).Select(_ => random.Next(0, 5000)));
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/api/cotacao?skus={skus}");
        request.Headers.Add("X-Tenant-Id", user.Tenant);

        var sentAt = clock.Elapsed;
        var status = 0;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(opt.RequestTimeoutSeconds));
        try
        {
            using var response = await http.SendAsync(request, HttpCompletionOption.ResponseContentRead, timeout.Token);
            status = (int)response.StatusCode;
        }
        catch (OperationCanceledException) { status = -1; } // o cliente desistiu (timeout)
        catch (HttpRequestException) { status = -2; }

        var latency = (clock.Elapsed - sentAt).TotalMilliseconds;
        samples.Enqueue(new Sample(sentAt.TotalSeconds, latency, status, user.Tenant, user.Kind));

        if (user.ThinkMs > 0)
        {
            var think = user.ThinkMs * (0.75 + random.NextDouble() * 0.5);
            await Task.Delay(TimeSpan.FromMilliseconds(think));
        }
        else if (status != 200)
        {
            await Task.Delay(50); // integração: pequena pausa só em caso de erro
        }
    }
}

public sealed record VirtualUser(string Tenant, string Kind, int ThinkMs);
public sealed record Sample(double SentAtSeconds, double LatencyMs, int Status, string Tenant, string Kind);
public sealed record ReplicaPoint(double AtSeconds, int Ready, int Current);

public sealed class LoadOptions
{
    public string Name { get; set; } = "cenario";
    public string Target { get; set; } = "http://127.0.0.1:8080";
    public int TotalUsers { get; set; } = 2000;
    public int RampSeconds { get; set; } = 30;
    public int DurationSeconds { get; set; } = 120;
    public int ThinkMs { get; set; } = 2000;
    public int Tenants { get; set; } = 10;
    public double SlaMs { get; set; } = 3000;
    public int RequestTimeoutSeconds { get; set; } = 10;
    public int SteadyFromSeconds { get; set; } = 60;
    public int? SteadyToSeconds { get; set; }
    public string Output { get; set; } = "results/cenario.json";

    /// <summary>Usuários ativos ao longo do tempo (interpolação linear entre os pontos).</summary>
    public List<(double AtSeconds, int Users)> Profile { get; set; } = new();

    public int ActiveUsersAt(double t)
    {
        if (t <= Profile[0].AtSeconds) return Profile[0].Users;
        for (var i = 1; i < Profile.Count; i++)
        {
            var (t1, u1) = Profile[i];
            if (t <= t1)
            {
                var (t0, u0) = Profile[i - 1];
                return (int)Math.Round(u0 + (u1 - u0) * (t - t0) / Math.Max(0.001, t1 - t0));
            }
        }
        return Profile[^1].Users;
    }

    /// <summary>Usuários de uma rede "barulhenta" que dispara sem pausa (ex.: integração em lote).</summary>
    public int NoisyUsers { get; set; }
    public string NoisyTenant { get; set; } = "rede-01";

    public VirtualUser[] BuildUsers()
    {
        var list = new List<VirtualUser>(TotalUsers);
        var regular = TotalUsers - NoisyUsers;
        var tenantIds = Enumerable.Range(1, Tenants).Select(t => $"rede-{t:00}").ToArray();
        var regularTenants = NoisyUsers > 0 ? tenantIds.Where(t => t != NoisyTenant).ToArray() : tenantIds;
        for (var i = 0; i < regular; i++) list.Add(new VirtualUser(regularTenants[i % regularTenants.Length], "usuario", ThinkMs));
        for (var i = 0; i < NoisyUsers; i++) list.Add(new VirtualUser(NoisyTenant, "integracao", 0));
        // Mistura as entradas na rampa para todos os tenants chegarem juntos.
        var shuffled = list.ToArray();
        new Random(42).Shuffle(shuffled);
        return shuffled;
    }

    public static LoadOptions Parse(string[] args)
    {
        var o = new LoadOptions();
        for (var i = 0; i < args.Length - 1; i += 2)
        {
            var v = args[i + 1];
            switch (args[i])
            {
                case "--name": o.Name = v; break;
                case "--target": o.Target = v; break;
                case "--users": o.TotalUsers = int.Parse(v); break;
                case "--ramp": o.RampSeconds = int.Parse(v); break;
                case "--duration": o.DurationSeconds = int.Parse(v); break;
                case "--think-ms": o.ThinkMs = int.Parse(v); break;
                case "--tenants": o.Tenants = int.Parse(v); break;
                case "--sla-ms": o.SlaMs = double.Parse(v); break;
                case "--timeout": o.RequestTimeoutSeconds = int.Parse(v); break;
                case "--steady-from": o.SteadyFromSeconds = int.Parse(v); break;
                case "--noisy-users": o.NoisyUsers = int.Parse(v); break;
                case "--noisy-tenant": o.NoisyTenant = v; break;
                case "--out": o.Output = v; break;
                case "--steady-to": o.SteadyToSeconds = int.Parse(v); break;
                case "--profile":
                    o.Profile = v.Split(',').Select(p => p.Split(':')).Select(p => (double.Parse(p[0]), int.Parse(p[1]))).ToList();
                    break;
                default: throw new ArgumentException($"Opção desconhecida: {args[i]}");
            }
        }
        if (o.Profile.Count == 0)
            o.Profile = [(0, o.RampSeconds > 0 ? 0 : o.TotalUsers), (o.RampSeconds, o.TotalUsers), (o.DurationSeconds, o.TotalUsers)];
        o.TotalUsers = Math.Max(o.TotalUsers, o.Profile.Max(p => p.Users));
        return o;
    }
}

public sealed record Stats(long Requests, long Ok, long Rejected503, long Timeouts, long OtherErrors,
    double OkWithinSlaPercent, double AllWithinSlaPercent, double P50Ms, double P95Ms, double P99Ms, double MaxMs, double Rps,
    double RejectedP95Ms);

public sealed record Bucket(int FromSecond, double Rps, double P95Ms, double WithinSlaPercent, int ReadyReplicas);

public sealed record Report(string Name, int Users, int NoisyUsers, double SlaMs, int DurationSeconds,
    Stats Overall, Stats SteadyState, Dictionary<string, Stats> ByKind, Dictionary<string, Stats> ByTenant,
    Bucket[] Timeline, double ReplicaSeconds, int MaxReadyReplicas, int SecondsBelowSla)
{
    public static Report Build(LoadOptions o, Sample[] samples, ReplicaPoint[] points)
    {
        var buckets = new List<Bucket>();
        for (var from = 0; from < o.DurationSeconds; from += 5)
        {
            var slice = samples.Where(s => s.SentAtSeconds >= from && s.SentAtSeconds < from + 5).ToArray();
            var ready = points.Where(p => p.AtSeconds >= from && p.AtSeconds < from + 5).Select(p => p.Ready).DefaultIfEmpty(0).Max();
            var st = Compute(slice, o.SlaMs, 5);
            buckets.Add(new Bucket(from, st.Rps, st.P95Ms, st.AllWithinSlaPercent, ready));
        }

        // Quanto tempo o sistema passou com o SLA quebrado: blocos de 5 s com menos de 95% das respostas em até 3 s.
        var secondsBelowSla = buckets.Count(b => b.WithinSlaPercent < 95) * 5;

        // Réplica-segundos: quanto de "pod ligado" o cenário consumiu (proxy do custo).
        var replicaSeconds = points.Length == 0 ? 0 : points.Sum(p => p.Current) * (o.DurationSeconds / (double)points.Length);

        var steadyTo = o.SteadyToSeconds ?? o.DurationSeconds;
        var steady = samples.Where(s => s.SentAtSeconds >= o.SteadyFromSeconds && s.SentAtSeconds < steadyTo).ToArray();
        return new Report(o.Name, o.TotalUsers, o.NoisyUsers, o.SlaMs, o.DurationSeconds,
            Compute(samples, o.SlaMs, o.DurationSeconds),
            Compute(steady, o.SlaMs, steadyTo - o.SteadyFromSeconds),
            samples.GroupBy(s => s.Kind).ToDictionary(g => g.Key, g => Compute(g.ToArray(), o.SlaMs, o.DurationSeconds)),
            samples.GroupBy(s => s.Tenant).OrderBy(g => g.Key).ToDictionary(g => g.Key, g => Compute(g.ToArray(), o.SlaMs, o.DurationSeconds)),
            buckets.ToArray(), Math.Round(replicaSeconds), points.Select(p => p.Ready).DefaultIfEmpty(0).Max(), secondsBelowSla);
    }

    public static Stats Compute(Sample[] s, double slaMs, double seconds)
    {
        if (s.Length == 0) return new Stats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        var rejected = s.Where(x => x.Status == 503).Select(x => x.LatencyMs).OrderBy(x => x).ToArray();
        var ok = s.Where(x => x.Status == 200).Select(x => x.LatencyMs).OrderBy(x => x).ToArray();
        double P(double q) => ok.Length == 0 ? 0 : Math.Round(ok[Math.Min(ok.Length - 1, (int)Math.Ceiling(q * ok.Length) - 1)], 0);
        var okWithin = ok.Count(x => x <= slaMs);
        return new Stats(s.Length, ok.Length,
            s.LongCount(x => x.Status == 503), s.LongCount(x => x.Status == -1), s.LongCount(x => x.Status is not (200 or 503 or -1)),
            ok.Length == 0 ? 0 : Math.Round(100.0 * okWithin / ok.Length, 1),
            Math.Round(100.0 * okWithin / s.Length, 1),
            P(0.50), P(0.95), P(0.99), ok.Length == 0 ? 0 : Math.Round(ok[^1], 0),
            Math.Round(s.Length / Math.Max(1, seconds), 1),
            rejected.Length == 0 ? 0 : Math.Round(rejected[Math.Min(rejected.Length - 1, (int)Math.Ceiling(0.95 * rejected.Length) - 1)], 0));
    }

    public static string Describe(Report r)
    {
        var o = r.Overall; var st = r.SteadyState;
        return $"""
            [{r.Name}] requisições {o.Requests:N0} | ok {o.Ok:N0} | 503 {o.Rejected503:N0} | timeouts {o.Timeouts:N0} | outros erros {o.OtherErrors:N0}
              geral:  p50 {o.P50Ms} ms | p95 {o.P95Ms} ms | p99 {o.P99Ms} ms | dentro de {r.SlaMs} ms: {o.AllWithinSlaPercent}% de todas
              regime estável: p95 {st.P95Ms} ms | dentro do SLA: {st.AllWithinSlaPercent}% | {st.Rps} req/s
              réplicas: máx {r.MaxReadyReplicas} | réplica-segundos {r.ReplicaSeconds} | tempo com SLA quebrado: {r.SecondsBelowSla}s | 503 em p95 de {o.RejectedP95Ms} ms
            """;
    }
}

[JsonSourceGenerationOptions(WriteIndented = true, PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(Report))]
public partial class ReportJson : JsonSerializerContext;

public static class Summary
{
    public static string Markdown(string dir)
    {
        var reports = Directory.GetFiles(dir, "v*.json")
            .Where(f => !f.EndsWith(".gateway-status.json", StringComparison.Ordinal))
            .OrderBy(f => f, StringComparer.Ordinal)
            .Select(f => JsonSerializer.Deserialize(File.ReadAllText(f), ReportJson.Default.Report)!)
            .ToArray();

        var sb = new System.Text.StringBuilder();
        sb.AppendLine("# Resultados");
        sb.AppendLine();
        sb.AppendLine($"Gerado em {DateTime.UtcNow:yyyy-MM-dd HH:mm} UTC em {Environment.ProcessorCount} núcleos ({System.Runtime.InteropServices.RuntimeInformation.OSDescription}).");
        sb.AppendLine();
        sb.AppendLine("## Visão geral (teste inteiro, incluindo a rampa)");
        sb.AppendLine();
        sb.AppendLine("| Cenário | Requisições | Dentro de 3 s | p50 | p95 | p99 | Recusadas (503) | Timeouts | Réplicas (máx) | Réplica-segundos | Tempo com SLA quebrado |");
        sb.AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        foreach (var r in reports)
        {
            var o = r.Overall;
            sb.AppendLine($"| {r.Name} | {o.Requests:N0} | {o.AllWithinSlaPercent}% | {o.P50Ms} ms | {o.P95Ms} ms | {o.P99Ms} ms | {o.Rejected503:N0} | {o.Timeouts:N0} | {r.MaxReadyReplicas} | {r.ReplicaSeconds:N0} | {r.SecondsBelowSla} s |");
        }

        sb.AppendLine();
        sb.AppendLine("## Regime estável (depois da rampa)");
        sb.AppendLine();
        sb.AppendLine("| Cenário | req/s | Dentro de 3 s | p95 | p99 |");
        sb.AppendLine("|---|---:|---:|---:|---:|");
        foreach (var r in reports)
        {
            var s = r.SteadyState;
            sb.AppendLine($"| {r.Name} | {s.Rps} | {s.AllWithinSlaPercent}% | {s.P95Ms} ms | {s.P99Ms} ms |");
        }

        foreach (var r in reports.Where(r => r.NoisyUsers > 0))
        {
            sb.AppendLine();
            sb.AppendLine($"## {r.Name}: por tipo de usuário");
            sb.AppendLine();
            sb.AppendLine("| Tipo | Requisições | Dentro de 3 s | p50 | p95 | p99 |");
            sb.AppendLine("|---|---:|---:|---:|---:|---:|");
            foreach (var (kind, s) in r.ByKind.OrderBy(k => k.Key))
                sb.AppendLine($"| {kind} | {s.Requests:N0} | {s.AllWithinSlaPercent}% | {s.P50Ms} ms | {s.P95Ms} ms | {s.P99Ms} ms |");
        }

        foreach (var r in reports)
        {
            sb.AppendLine();
            sb.AppendLine($"## {r.Name}: linha do tempo (blocos de 5 s)");
            sb.AppendLine();
            sb.AppendLine("| Segundo | req/s | p95 | Dentro de 3 s | Réplicas prontas |");
            sb.AppendLine("|---:|---:|---:|---:|---:|");
            foreach (var b in r.Timeline)
                sb.AppendLine($"| {b.FromSecond} | {b.Rps} | {b.P95Ms} ms | {b.WithinSlaPercent}% | {b.ReadyReplicas} |");
        }

        return sb.ToString();
    }
}
