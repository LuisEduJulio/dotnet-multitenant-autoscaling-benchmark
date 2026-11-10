namespace Tenancy.Core;

/// <summary>
/// Controle de admissão por prazo: se a requisição não vai conseguir responder dentro do
/// orçamento (ex.: 3 s), é melhor recusar agora, em milissegundos, do que deixar o cliente
/// esperar e estourar o tempo de qualquer jeito.
/// </summary>
public static class AdmissionControl
{
    /// <summary>Tempo estimado de espera na fila, em ms.</summary>
    public static double EstimateWaitMs(int queuedAhead, int slots, double avgServiceMs)
    {
        if (slots <= 0) throw new ArgumentOutOfRangeException(nameof(slots));
        if (queuedAhead <= 0 || avgServiceMs <= 0) return 0;
        // Com N slots, a fila anda N posições a cada "tempo médio de serviço".
        return Math.Ceiling((double)queuedAhead / slots) * avgServiceMs;
    }

    /// <summary>
    /// Admite se espera estimada + tempo de serviço couberem no orçamento,
    /// guardando uma margem para rede e gateway.
    /// </summary>
    public static bool ShouldAdmit(int queuedAhead, int slots, double avgServiceMs, double budgetMs, double safetyMarginMs = 300)
    {
        var expectedTotal = EstimateWaitMs(queuedAhead, slots, avgServiceMs) + avgServiceMs;
        return expectedTotal <= budgetMs - safetyMarginMs;
    }
}
