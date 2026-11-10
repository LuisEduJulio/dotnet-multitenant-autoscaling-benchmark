using System.Text.Json.Serialization;
using Microsoft.Extensions.Primitives;
using Tenancy.Core;

namespace Tenancy.Api;

public sealed record ReplicaOptions
{
    public string ReplicaId { get; init; } = "replica-1";

    /// <summary>Processamentos simultâneos por réplica (a capacidade de um pod).</summary>
    public int Slots { get; init; } = 50;

    public QueueMode QueueMode { get; init; } = QueueMode.Fifo;
    public bool AdmissionEnabled { get; init; }
    public double BudgetMs { get; init; } = 3000;

    /// <summary>Latência da chamada externa (estoque/ERP) que cada cotação faz.</summary>
    public int DownstreamLatencyMs { get; init; } = 250;
    public int DownstreamJitterMs { get; init; } = 50;

    public static ReplicaOptions From(IConfiguration c) => new()
    {
        ReplicaId = c["Replica:Id"] ?? "replica-1",
        Slots = c.GetValue("Replica:Slots", 50),
        QueueMode = Enum.TryParse<QueueMode>(c["Queue:Mode"], ignoreCase: true, out var m) ? m : QueueMode.Fifo,
        AdmissionEnabled = c.GetValue("Admission:Enabled", false),
        BudgetMs = c.GetValue("Admission:BudgetMs", 3000d),
        DownstreamLatencyMs = c.GetValue("Downstream:LatencyMs", 250),
        DownstreamJitterMs = c.GetValue("Downstream:JitterMs", 50),
    };
}

public sealed class Tenant(string id, decimal[] prices)
{
    public string Id { get; } = id;
    public int ProductCount => prices.Length;
    public decimal PriceOf(int sku) => prices[sku];
}

/// <summary>10 redes de farmácia, cada uma com o seu catálogo e a sua tabela de preços.</summary>
public sealed class TenantCatalog
{
    public const int ProductsPerTenant = 5_000;
    private readonly Dictionary<string, Tenant> _tenants = new(StringComparer.Ordinal);

    public TenantCatalog()
    {
        for (var t = 1; t <= 10; t++)
        {
            var id = $"rede-{t:00}";
            var random = new Random(t);
            var prices = new decimal[ProductsPerTenant];
            for (var p = 0; p < prices.Length; p++) prices[p] = Math.Round((decimal)(random.NextDouble() * 150 + 2), 2);
            _tenants[id] = new Tenant(id, prices);
        }
    }

    public IReadOnlyCollection<string> Ids => _tenants.Keys;
    public bool TryGet(string id, out Tenant tenant) => _tenants.TryGetValue(id, out tenant!);
}

public static class Skus
{
    public static int[] Parse(StringValues raw, int productCount)
    {
        var parts = raw.ToString().Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var skus = new List<int>(5);
        foreach (var p in parts)
            if (int.TryParse(p, out var sku) && sku >= 0 && sku < productCount) skus.Add(sku);
        return skus.Count > 0 ? skus.ToArray() : [0];
    }
}

public interface IDownstreamService
{
    Task<int[]> GetStockAsync(string tenantId, int[] skus, CancellationToken cancellationToken);
}

/// <summary>
/// Chamada externa de estoque/ERP. É I/O: a thread fica livre enquanto espera,
/// mas o slot da réplica fica ocupado, como uma conexão de um pool.
/// </summary>
public sealed class SimulatedDownstreamService(ReplicaOptions options) : IDownstreamService
{
    public async Task<int[]> GetStockAsync(string tenantId, int[] skus, CancellationToken cancellationToken)
    {
        var jitter = options.DownstreamJitterMs > 0 ? Random.Shared.Next(-options.DownstreamJitterMs, options.DownstreamJitterMs + 1) : 0;
        await Task.Delay(Math.Max(1, options.DownstreamLatencyMs + jitter), cancellationToken);
        var stock = new int[skus.Length];
        for (var i = 0; i < skus.Length; i++) stock[i] = (skus[i] * 31 + tenantId.Length) % 200;
        return stock;
    }
}

public sealed class ReplicaStats
{
    private readonly object _gate = new();
    private double _ewmaServiceMs;
    private long _completed;
    private long _rejected;

    public double AverageServiceMs { get { lock (_gate) return Math.Round(_ewmaServiceMs, 1); } }
    public long Completed => Interlocked.Read(ref _completed);
    public long Rejected => Interlocked.Read(ref _rejected);

    public void RecordCompleted(double serviceMs)
    {
        Interlocked.Increment(ref _completed);
        lock (_gate) _ewmaServiceMs = _ewmaServiceMs == 0 ? serviceMs : _ewmaServiceMs * 0.95 + serviceMs * 0.05;
    }

    public void RecordRejected() => Interlocked.Increment(ref _rejected);
}

public sealed record CotacaoItem(int Sku, decimal Preco, int Estoque);
public sealed record Cotacao(string Tenant, string Replica, CotacaoItem[] Itens, decimal Total);
public sealed record ReplicaMetrics(string ReplicaId, int InFlight, int Queued, int Slots, double AvgServiceMs, long Completed, long Rejected);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(Cotacao))]
[JsonSerializable(typeof(ReplicaMetrics))]
[JsonSerializable(typeof(Microsoft.AspNetCore.Mvc.ProblemDetails))]
[JsonSerializable(typeof(Microsoft.AspNetCore.Http.HttpValidationProblemDetails))]
public partial class AppJsonContext : JsonSerializerContext;
