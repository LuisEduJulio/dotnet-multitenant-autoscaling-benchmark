namespace Tenancy.Core;

public enum QueueMode
{
    /// <summary>Uma fila única: quem chega primeiro é atendido primeiro, de qualquer tenant.</summary>
    Fifo,

    /// <summary>Uma fila por tenant, atendidas em rodízio (round-robin) quando um slot é liberado.</summary>
    FairPerTenant
}

/// <summary>
/// Limita quantos processamentos rodam ao mesmo tempo em uma réplica (os "slots" do pod)
/// e decide quem espera e quem é atendido quando um slot é liberado.
/// </summary>
public sealed class TenantScheduler
{
    private readonly object _gate = new();
    private readonly QueueMode _mode;

    // Modo FIFO
    private readonly LinkedList<Waiter> _fifo = new();

    // Modo justo por tenant
    private readonly Dictionary<string, Queue<Waiter>> _perTenant = new(StringComparer.Ordinal);
    private readonly LinkedList<string> _ring = new();

    private int _inFlight;
    private int _queued;

    public TenantScheduler(int slots, QueueMode mode)
    {
        if (slots <= 0) throw new ArgumentOutOfRangeException(nameof(slots));
        Slots = slots;
        _mode = mode;
    }

    public int Slots { get; }
    public QueueMode Mode => _mode;

    public int InFlight { get { lock (_gate) return _inFlight; } }
    public int Queued { get { lock (_gate) return _queued; } }

    /// <summary>Quantas requisições deste tenant estão na fila.</summary>
    public int QueuedFor(string tenant)
    {
        lock (_gate)
        {
            if (_mode == QueueMode.Fifo)
            {
                var count = 0;
                foreach (var w in _fifo) if (w.Tenant == tenant && !w.Tcs.Task.IsCompleted) count++;
                return count;
            }

            return _perTenant.TryGetValue(tenant, out var q) ? q.Count(w => !w.Tcs.Task.IsCompleted) : 0;
        }
    }

    /// <summary>Quantos tenants têm alguém esperando na fila.</summary>
    public int TenantsWaiting
    {
        get
        {
            lock (_gate)
            {
                if (_mode == QueueMode.FairPerTenant) return _ring.Count;
                return _fifo.Where(w => !w.Tcs.Task.IsCompleted).Select(w => w.Tenant).Distinct().Count();
            }
        }
    }

    /// <summary>
    /// Quantas requisições serão atendidas antes de uma nova requisição deste tenant.
    /// FIFO: a fila inteira. Justo: cada tenant que espera anda uma posição por rodada.
    /// </summary>
    public int EstimateQueuedAhead(string tenant)
    {
        lock (_gate)
        {
            if (_mode == QueueMode.Fifo) return _queued;

            var mine = _perTenant.TryGetValue(tenant, out var q) ? q.Count : 0;
            var ahead = 0;
            foreach (var (other, queue) in _perTenant)
            {
                ahead += other == tenant ? mine : Math.Min(queue.Count, mine + 1);
            }
            return ahead;
        }
    }

    public ValueTask<Lease> AcquireAsync(string tenant, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tenant);
        cancellationToken.ThrowIfCancellationRequested();

        Waiter waiter;
        lock (_gate)
        {
            if (_inFlight < Slots && _queued == 0)
            {
                _inFlight++;
                return ValueTask.FromResult(new Lease(this));
            }

            waiter = new Waiter(tenant);
            if (_mode == QueueMode.Fifo)
            {
                _fifo.AddLast(waiter);
            }
            else
            {
                if (!_perTenant.TryGetValue(tenant, out var q))
                {
                    q = new Queue<Waiter>();
                    _perTenant[tenant] = q;
                }
                if (q.Count == 0) _ring.AddLast(tenant);
                q.Enqueue(waiter);
            }
            _queued++;
        }

        if (cancellationToken.CanBeCanceled)
        {
            waiter.Registration = cancellationToken.Register(static state =>
            {
                var (scheduler, w, token) = ((TenantScheduler, Waiter, CancellationToken))state!;
                scheduler.Cancel(w, token);
            }, (this, waiter, cancellationToken));
        }

        return new ValueTask<Lease>(waiter.Tcs.Task);
    }

    private void Cancel(Waiter waiter, CancellationToken token)
    {
        lock (_gate)
        {
            if (waiter.Tcs.TrySetCanceled(token))
            {
                _queued--;
                // A remoção física acontece de forma preguiçosa na próxima liberação de slot.
            }
        }
    }

    private void Release()
    {
        Waiter? handedOver = null;
        lock (_gate)
        {
            while (TryDequeue(out var next))
            {
                if (next.Tcs.TrySetResult(new Lease(this)))
                {
                    _queued--;
                    handedOver = next; // o slot passa direto para quem estava esperando
                    break;
                }
            }

            if (handedOver is null) _inFlight--;
        }

        // Fora do lock: Dispose espera um callback de cancelamento em andamento, que também usa o lock.
        handedOver?.Registration.Dispose();
    }

    private bool TryDequeue(out Waiter waiter)
    {
        if (_mode == QueueMode.Fifo)
        {
            while (_fifo.First is { } node)
            {
                _fifo.RemoveFirst();
                if (!node.Value.Tcs.Task.IsCompleted) { waiter = node.Value; return true; }
            }
            waiter = null!;
            return false;
        }

        while (_ring.First is { } tenantNode)
        {
            var tenant = tenantNode.Value;
            _ring.RemoveFirst();
            var queue = _perTenant[tenant];

            Waiter? found = null;
            while (queue.Count > 0)
            {
                var candidate = queue.Dequeue();
                if (!candidate.Tcs.Task.IsCompleted) { found = candidate; break; }
            }

            if (queue.Count > 0) _ring.AddLast(tenant); // ainda tem gente: vai para o fim do rodízio
            else _perTenant.Remove(tenant);

            if (found is not null) { waiter = found; return true; }
        }

        waiter = null!;
        return false;
    }

    private sealed class Waiter(string tenant)
    {
        public string Tenant { get; } = tenant;
        public TaskCompletionSource<Lease> Tcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public CancellationTokenRegistration Registration { get; set; }
    }

    public sealed class Lease : IDisposable
    {
        private TenantScheduler? _owner;

        internal Lease(TenantScheduler owner) => _owner = owner;

        public void Dispose() => Interlocked.Exchange(ref _owner, null)?.Release();
    }
}
