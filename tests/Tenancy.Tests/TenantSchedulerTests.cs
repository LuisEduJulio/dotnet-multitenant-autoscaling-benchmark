using Tenancy.Core;

namespace Tenancy.Tests;

public class TenantSchedulerTests
{
    [Fact]
    public async Task Nunca_passa_do_limite_de_slots()
    {
        var scheduler = new TenantScheduler(slots: 5, QueueMode.Fifo);
        var current = 0;
        var max = 0;

        await Task.WhenAll(Enumerable.Range(0, 200).Select(async i =>
        {
            using var lease = await scheduler.AcquireAsync($"rede-{i % 3}");
            var now = Interlocked.Increment(ref current);
            InterlockedMax(ref max, now);
            await Task.Delay(2);
            Interlocked.Decrement(ref current);
        }));

        Assert.Equal(5, max);
        Assert.Equal(0, scheduler.InFlight);
        Assert.Equal(0, scheduler.Queued);
    }

    [Fact]
    public async Task Fifo_atende_por_ordem_de_chegada_mesmo_com_um_tenant_dominando()
    {
        var order = await ServeOrder(QueueMode.Fifo);

        // A rede pequena chegou depois de 10 pedidos da grande: é a 11ª a ser atendida.
        Assert.Equal(10, order.IndexOf("pequena"));
    }

    [Fact]
    public async Task Fila_justa_intercala_tenants_e_a_rede_pequena_nao_espera_a_grande()
    {
        var order = await ServeOrder(QueueMode.FairPerTenant);

        // Rodízio: grande, pequena, grande, grande...
        Assert.Equal(1, order.IndexOf("pequena"));
    }

    [Fact]
    public async Task Cancelar_na_fila_libera_a_vez_e_nao_vaza_contagem()
    {
        var scheduler = new TenantScheduler(slots: 1, QueueMode.FairPerTenant);
        var first = await scheduler.AcquireAsync("rede-01");

        using var cts = new CancellationTokenSource();
        var cancelled = scheduler.AcquireAsync("rede-02", cts.Token).AsTask();
        var next = scheduler.AcquireAsync("rede-03").AsTask();
        Assert.Equal(2, scheduler.Queued);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled);
        Assert.Equal(1, scheduler.Queued);

        first.Dispose();
        using var lease = await next.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(0, scheduler.Queued);
        Assert.Equal(1, scheduler.InFlight);
    }

    [Fact]
    public void Estimativa_de_fila_na_fila_justa_considera_so_o_rodizio()
    {
        var scheduler = new TenantScheduler(slots: 1, QueueMode.FairPerTenant);
        _ = scheduler.AcquireAsync("grande");                     // ocupa o slot
        for (var i = 0; i < 50; i++) _ = scheduler.AcquireAsync("grande");

        // Uma nova requisição da rede pequena só espera uma rodada, não os 50 da grande.
        Assert.Equal(1, scheduler.EstimateQueuedAhead("pequena"));
        Assert.Equal(50, scheduler.EstimateQueuedAhead("grande"));

        var fifo = new TenantScheduler(slots: 1, QueueMode.Fifo);
        _ = fifo.AcquireAsync("grande");
        for (var i = 0; i < 50; i++) _ = fifo.AcquireAsync("grande");
        Assert.Equal(50, fifo.EstimateQueuedAhead("pequena"));
    }

    private static async Task<List<string>> ServeOrder(QueueMode mode)
    {
        var scheduler = new TenantScheduler(slots: 1, mode);
        var order = new List<string>();
        var gate = await scheduler.AcquireAsync("aquecimento"); // segura o único slot

        var waiting = new List<Task>();
        for (var i = 0; i < 10; i++) waiting.Add(Take("grande"));
        waiting.Add(Take("pequena"));

        gate.Dispose();
        await Task.WhenAll(waiting);
        return order;

        async Task Take(string tenant)
        {
            using var lease = await scheduler.AcquireAsync(tenant);
            lock (order) order.Add(tenant);
        }
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int seen;
        while ((seen = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, seen) != seen) { }
    }
}
