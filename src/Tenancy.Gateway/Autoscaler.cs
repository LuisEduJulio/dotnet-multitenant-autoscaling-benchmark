using Tenancy.Core;

namespace Tenancy.Gateway;

/// <summary>
/// Laço de controle do autoscaler: a cada N segundos lê as métricas das réplicas prontas
/// (em processamento + na fila), pergunta à política quantas réplicas deveriam existir e ajusta.
/// </summary>
public sealed class Autoscaler(ReplicaManager replicas, GatewayOptions options, ILogger<Autoscaler> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.AutoscalingEnabled) return;

        var policy = new ScalingPolicy(new ScalingOptions
        {
            MinReplicas = options.MinReplicas,
            MaxReplicas = options.MaxReplicas,
            TargetLoadPerReplica = options.TargetLoadPerReplica,
            ScaleDownStabilization = TimeSpan.FromSeconds(options.ScaleDownStabilizationSeconds),
        });

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(options.EvaluationIntervalSeconds));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await replicas.RefreshMetricsAsync(stoppingToken);
                var ready = replicas.Ready;
                var load = ready.Sum(r => r.LastMetrics is { } m ? m.InFlight + m.Queued : r.Outstanding);
                var current = replicas.CurrentCount;
                var desired = policy.Recommend(current, load, DateTimeOffset.UtcNow);
                replicas.DesiredReplicas = desired;

                var reason = $"carga {load} em {ready.Length} prontas (alvo {options.TargetLoadPerReplica}/réplica)";
                for (var i = current; i < desired; i++)
                    _ = replicas.AddReplicaAsync(TimeSpan.FromSeconds(options.ProvisioningDelaySeconds), reason, stoppingToken);
                for (var i = desired; i < current; i++)
                    _ = replicas.RemoveReplicaAsync(reason, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception ex)
            {
                logger.LogError(ex, "Falha na avaliação do autoscaler");
            }
        }
    }
}
