using Tenancy.Core;

namespace Tenancy.Tests;

public class AdmissionControlTests
{
    [Fact]
    public void Fila_vazia_admite()
        => Assert.True(AdmissionControl.ShouldAdmit(queuedAhead: 0, slots: 50, avgServiceMs: 250, budgetMs: 3000));

    [Fact]
    public void Recusa_quando_a_espera_estimada_estoura_o_orcamento()
    {
        // 500 na frente, 50 slots, 250 ms cada: 10 rodadas = 2,5 s de fila + 250 ms de serviço = 2,75 s.
        // Com 300 ms de margem, o limite é 2,7 s: recusa.
        Assert.Equal(2500, AdmissionControl.EstimateWaitMs(500, 50, 250));
        Assert.False(AdmissionControl.ShouldAdmit(500, 50, 250, 3000));
        Assert.True(AdmissionControl.ShouldAdmit(450, 50, 250, 3000));
    }
}

public class ScalingPolicyTests
{
    private static readonly DateTimeOffset T0 = new(2026, 11, 9, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Desejado_e_a_carga_dividida_pelo_alvo_arredondada_para_cima()
    {
        Assert.Equal(6, ScalingPolicy.RawDesired(totalLoad: 222, targetPerReplica: 40));
        Assert.Equal(0, ScalingPolicy.RawDesired(0, 40));
    }

    [Fact]
    public void Subida_limitada_ao_dobro_ou_mais_quatro()
    {
        var policy = new ScalingPolicy(new ScalingOptions { MaxReplicas = 50, TargetLoadPerReplica = 10 });

        // Carga pede 40 réplicas, mas de 1 só pode ir para max(2, 1+4) = 5 nesta rodada.
        Assert.Equal(5, policy.Recommend(currentReplicas: 1, totalLoad: 400, T0));
        // De 10, pode dobrar para 20.
        Assert.Equal(20, policy.Recommend(currentReplicas: 10, totalLoad: 400, T0.AddSeconds(5)));
    }

    [Fact]
    public void Respeita_minimo_e_maximo()
    {
        var policy = new ScalingPolicy(new ScalingOptions { MinReplicas = 2, MaxReplicas = 4, TargetLoadPerReplica = 10 });
        Assert.Equal(2, policy.Recommend(2, 0, T0));
        Assert.Equal(4, policy.Recommend(4, 10_000, T0.AddSeconds(5)));
    }

    [Fact]
    public void Descida_espera_a_janela_de_estabilizacao()
    {
        var policy = new ScalingPolicy(new ScalingOptions
        {
            MaxReplicas = 10,
            TargetLoadPerReplica = 10,
            ScaleDownStabilization = TimeSpan.FromSeconds(60),
        });

        Assert.Equal(8, policy.Recommend(8, 80, T0));                    // pico
        Assert.Equal(8, policy.Recommend(8, 20, T0.AddSeconds(30)));     // caiu, mas o pico ainda está na janela
        Assert.Equal(2, policy.Recommend(8, 20, T0.AddSeconds(61)));     // janela passou: pode descer
    }
}
