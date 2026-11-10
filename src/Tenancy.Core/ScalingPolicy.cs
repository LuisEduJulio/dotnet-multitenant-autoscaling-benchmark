namespace Tenancy.Core;

public sealed record ScalingOptions
{
    public int MinReplicas { get; init; } = 1;
    public int MaxReplicas { get; init; } = 10;

    /// <summary>Carga alvo por réplica: requisições em processamento + na fila.</summary>
    public double TargetLoadPerReplica { get; init; } = 100;

    /// <summary>Janela em que a escala para baixo usa a maior recomendação recente (igual ao HPA).</summary>
    public TimeSpan ScaleDownStabilization { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>Por rodada, sobe no máximo para o maior entre o dobro e +4 réplicas (padrão do HPA).</summary>
    public int ScaleUpMaxAddedPods { get; init; } = 4;
}

/// <summary>
/// Calcula quantas réplicas o sistema precisa, no mesmo espírito do Horizontal Pod Autoscaler:
/// desejado = teto(carga total / carga alvo por réplica), com limites de subida e
/// estabilização na descida para não ficar sobe-e-desce.
/// </summary>
public sealed class ScalingPolicy
{
    private readonly ScalingOptions _options;
    private readonly List<(DateTimeOffset At, int Desired)> _history = new();

    public ScalingPolicy(ScalingOptions options)
    {
        if (options.MinReplicas < 1 || options.MaxReplicas < options.MinReplicas)
            throw new ArgumentException("Limites de réplicas inválidos.", nameof(options));
        if (options.TargetLoadPerReplica <= 0)
            throw new ArgumentException("A carga alvo precisa ser positiva.", nameof(options));
        _options = options;
    }

    public static int RawDesired(double totalLoad, double targetPerReplica)
        => totalLoad <= 0 ? 0 : (int)Math.Ceiling(totalLoad / targetPerReplica);

    /// <param name="currentReplicas">Réplicas prontas + subindo.</param>
    /// <param name="totalLoad">Soma de (em processamento + na fila) das réplicas prontas.</param>
    public int Recommend(int currentReplicas, double totalLoad, DateTimeOffset now)
    {
        var raw = Math.Clamp(RawDesired(totalLoad, _options.TargetLoadPerReplica), _options.MinReplicas, _options.MaxReplicas);

        _history.Add((now, raw));
        _history.RemoveAll(h => now - h.At > _options.ScaleDownStabilization);

        if (raw >= currentReplicas)
        {
            var upLimit = Math.Max(currentReplicas * 2, currentReplicas + _options.ScaleUpMaxAddedPods);
            return Math.Clamp(Math.Min(raw, upLimit), _options.MinReplicas, _options.MaxReplicas);
        }

        // Escala para baixo: usa a maior recomendação da janela de estabilização.
        var stabilized = _history.Max(h => h.Desired);
        return Math.Clamp(Math.Min(currentReplicas, stabilized), _options.MinReplicas, _options.MaxReplicas);
    }
}
