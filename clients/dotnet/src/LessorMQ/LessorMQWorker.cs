namespace LessorMQ;

/// <summary>
/// Runs one message. Returning acks it; throwing fails it, as permanent when the
/// exception is (or wraps) a <see cref="PermanentFailureException"/>.
/// <paramref name="cancellationToken"/> is cancelled when the lease is lost (stop then,
/// the message is someone else's now) and when the worker is asked to stop.
/// </summary>
public delegate Task MessageHandler(Job job, CancellationToken cancellationToken);

/// <summary>What a handler gets: the message, its lease, and a way to keep the lease.</summary>
public sealed class Job
{
    private readonly LessorMQClient client;

    /// <summary>A job from its parts, for tests; the worker makes real ones.</summary>
    public Job(LessorMQClient client, Lease lease)
    {
        this.client = client;
        Lease = lease;
    }

    /// <summary>The claimed message.</summary>
    public Message Message => Lease.Message;

    /// <summary>The lease on it.</summary>
    public Lease Lease { get; }

    /// <summary>
    /// Extends the lease now. The worker already does this on a timer; call it only with
    /// <see cref="WorkerOptions.AutoExtend"/> off, or to ask for a longer lease at once
    /// (0: the queue's lease).
    /// </summary>
    /// <exception cref="LeaseLostException">The lease already ended.</exception>
    public Task ExtendAsync(int leaseSeconds = 0, CancellationToken cancellationToken = default) =>
        client.ExtendAsync(Lease, leaseSeconds, cancellationToken);
}

/// <summary>
/// How a worker claims and keeps its leases. What happens to a failed message is the
/// queue's business, so there is nothing about retries here.
/// </summary>
public sealed class WorkerOptions
{
    /// <summary>
    /// The worker's name, shown to administrators; host:pid by default, so the workers on
    /// one host are told apart.
    /// </summary>
    public string? Consumer { get; set; }

    /// <summary>The path this worker runs jobs through: a VPN exit, a proxy, an API account.</summary>
    public string? Route { get; set; }

    /// <summary>The long-poll wait per claim, at most 20. Default 20.</summary>
    public int WaitSeconds { get; set; } = 20;

    /// <summary>The lease per claim; 0 is the queue's.</summary>
    public int LeaseSeconds { get; set; }

    /// <summary>The time between automatic extends; a third of the remaining lease when null.</summary>
    public TimeSpan? ExtendEvery { get; set; }

    /// <summary>Extend the lease on a timer while a handler runs. Default true.</summary>
    public bool AutoExtend { get; set; } = true;

    /// <summary>Messages handled at the same time, each by its own loop with its own claims. Default 1.</summary>
    public int Concurrency { get; set; } = 1;

    /// <summary>Stop after this many messages; 0 is no limit.</summary>
    public int MaxMessages { get; set; }

    /// <summary>The wait after a claim failed (the server could not be reached, say). Default 1 s.</summary>
    public TimeSpan ErrorPause { get; set; } = TimeSpan.FromSeconds(1);
}

/// <summary>
/// Claims messages of one queue and runs a handler on each.
/// <code>
/// var worker = new LessorMQWorker(client, "mail", async (job, ct) =>
/// {
///     var mail = job.Message.Decode&lt;Mail&gt;() ?? throw new PermanentFailureException("no mail");
///     await mailer.SendAsync(mail, ct);
/// });
/// await worker.RunAsync(stoppingToken);
/// </code>
/// What happens after a failure (another attempt, when, or the dead-letter set) is
/// decided by the queue. While the handler runs, the lease is extended on a timer.
/// </summary>
public sealed class LessorMQWorker
{
    private readonly LessorMQClient client;
    private readonly MessageHandler handler;
    private readonly ClaimOptions claim;
    private readonly int leaseSeconds;
    private readonly TimeSpan? extendEvery;
    private readonly bool autoExtend;
    private readonly int concurrency;
    private readonly int maxMessages;
    private readonly TimeSpan errorPause;
    private int handled;

    // Messages taken on, so concurrent loops never pass MaxMessages.
    private int started;

    /// <summary>A worker for <paramref name="queue"/> on <paramref name="client"/>, running <paramref name="handler"/> on every message.</summary>
    public LessorMQWorker(LessorMQClient client, string queue, MessageHandler handler, WorkerOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(queue);
        ArgumentNullException.ThrowIfNull(handler);
        options ??= new WorkerOptions();

        this.client = client;
        this.handler = handler;
        Queue = queue;
        claim = new ClaimOptions
        {
            Consumer = string.IsNullOrEmpty(options.Consumer) ? DefaultConsumer() : options.Consumer,
            Route = options.Route,
            WaitSeconds = options.WaitSeconds,
            LeaseSeconds = options.LeaseSeconds,
        };
        leaseSeconds = options.LeaseSeconds;
        extendEvery = options.ExtendEvery;
        autoExtend = options.AutoExtend;
        concurrency = Math.Max(1, options.Concurrency);
        maxMessages = options.MaxMessages;
        errorPause = options.ErrorPause;
    }

    /// <summary>The queue this worker claims from.</summary>
    public string Queue { get; }

    /// <summary>The number of messages handled so far.</summary>
    public int Handled => Volatile.Read(ref handled);

    /// <summary>
    /// Claims and handles messages until <paramref name="stoppingToken"/> is cancelled or
    /// <see cref="WorkerOptions.MaxMessages"/> are handled, and returns once every message
    /// in hand is reported.
    /// </summary>
    /// <remarks>
    /// Stopping does not cut off a claim that is already waiting (that could leave a
    /// leased message to run out): it ends within <see cref="WorkerOptions.WaitSeconds"/>,
    /// and a message it brings is given back with a release. A running handler sees its
    /// cancellation token cancelled: when it gives up by throwing an
    /// <see cref="OperationCanceledException"/>, its message is released too (the attempt
    /// does not count); when it finishes, the message is acked or failed as usual.
    /// </remarks>
    public async Task RunAsync(CancellationToken stoppingToken = default)
    {
        if (stoppingToken.IsCancellationRequested)
        {
            return;
        }

        var loops = new Task[concurrency];
        for (var i = 0; i < loops.Length; i++)
        {
            loops[i] = Task.Run(
                async () =>
                {
                    while (!stoppingToken.IsCancellationRequested && !Done())
                    {
                        await RunOnceAsync(stoppingToken).ConfigureAwait(false);
                    }
                },
                CancellationToken.None);
        }

        await Task.WhenAll(loops).ConfigureAwait(false);
    }

    /// <summary>
    /// Makes one claim and runs the handler on what it got. True when a message was
    /// handled. Once <paramref name="stoppingToken"/> is cancelled, a message the claim
    /// brings is given back.
    /// </summary>
    public async Task<bool> RunOnceAsync(CancellationToken stoppingToken = default)
    {
        Lease? lease;
        try
        {
            // Not cancelled by stoppingToken: see RunAsync.
            lease = await client.ClaimAsync(Queue, claim, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            client.Log(LogSeverity.Error, e, "Claim from {Queue} failed: {Error}", Queue, e.Message);
            try
            {
                await Task.Delay(errorPause, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Asked to stop: no need to wait any longer.
            }

            return false;
        }

        if (lease is null)
        {
            return false;
        }

        if (stoppingToken.IsCancellationRequested || !Take())
        {
            // Asked to stop while the claim waited, or the other loops reached MaxMessages:
            // give the message back untouched.
            await ReportAsync(lease, "Release", () => client.ReleaseAsync(lease)).ConfigureAwait(false);
            return false;
        }

        await HandleAsync(lease, stoppingToken).ConfigureAwait(false);
        Interlocked.Increment(ref handled);
        return true;
    }

    private bool Done() => maxMessages > 0 && Volatile.Read(ref started) >= maxMessages;

    // Reserves one of MaxMessages for a claimed message.
    private bool Take()
    {
        if (maxMessages <= 0)
        {
            return true;
        }

        if (Interlocked.Increment(ref started) > maxMessages)
        {
            Interlocked.Decrement(ref started);
            return false;
        }

        return true;
    }

    private async Task HandleAsync(Lease lease, CancellationToken stoppingToken)
    {
        using var leaseLost = new CancellationTokenSource();
        using var handlerToken = CancellationTokenSource.CreateLinkedTokenSource(leaseLost.Token, stoppingToken);
        using var stopKeeping = new CancellationTokenSource();
        var keeper = autoExtend ? KeepAsync(lease, leaseLost, stopKeeping.Token) : Task.CompletedTask;

        Exception? error = null;
        try
        {
            await handler(new Job(client, lease), handlerToken.Token).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            error = e;
        }

        await stopKeeping.CancelAsync().ConfigureAwait(false);
        await keeper.ConfigureAwait(false);

        var id = lease.Message.Id;
        if (leaseLost.IsCancellationRequested)
        {
            client.Log(LogSeverity.Warning, null, "Lease on message {MessageId} lost while the handler ran; not reporting", id);
            return;
        }

        if (error is null)
        {
            await ReportAsync(lease, "Ack", () => client.AckAsync(lease)).ConfigureAwait(false);
            return;
        }

        if (error is OperationCanceledException && stoppingToken.IsCancellationRequested)
        {
            // The handler gave up because the worker is stopping: that says nothing about
            // the message, so it goes back without counting the attempt.
            await ReportAsync(lease, "Release", () => client.ReleaseAsync(lease)).ConfigureAwait(false);
            return;
        }

        await ReportAsync(lease, "Fail", async () =>
        {
            var result = await client.FailAsync(lease, error).ConfigureAwait(false);
            client.Log(
                LogSeverity.Information,
                error,
                "Message {MessageId} failed: {Error}; the server says {Outcome} (attempt {Attempt} of {MaxAttempts})",
                id,
                error.Message,
                result.Outcome,
                result.Attempt,
                result.MaxAttempts);
        }).ConfigureAwait(false);
    }

    // Extends the lease every interval until stopped; a lost lease cancels the handler's
    // token. Never throws.
    private async Task KeepAsync(Lease lease, CancellationTokenSource lost, CancellationToken stop)
    {
        while (true)
        {
            try
            {
                await Task.Delay(ExtendInterval(lease), stop).ConfigureAwait(false);
                await client.ExtendAsync(lease, leaseSeconds, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
            catch (LeaseLostException)
            {
                await lost.CancelAsync().ConfigureAwait(false);
                return;
            }
            catch (Exception e)
            {
                client.Log(LogSeverity.Warning, e, "Extend of message {MessageId} failed; trying again: {Error}", lease.Message.Id, e.Message);
            }
        }
    }

    // ExtendEvery, or a third of what is left of the lease: at least a second, and at most
    // an hour, so a lease that seems to end far away (a clock that is off) still fits
    // Task.Delay.
    private TimeSpan ExtendInterval(Lease lease)
    {
        if (extendEvery is { } every && every > TimeSpan.Zero)
        {
            return every;
        }

        var third = lease.Remaining / 3;
        return third < TimeSpan.FromSeconds(1) ? TimeSpan.FromSeconds(1) : third > TimeSpan.FromHours(1) ? TimeSpan.FromHours(1) : third;
    }

    // Acks, fails or releases, keeping the worker alive whatever happens: a lost lease
    // means the message is someone else's now; any other failure means the lease will
    // expire and the server will make the message available again.
    private async Task ReportAsync(Lease lease, string what, Func<Task> call)
    {
        try
        {
            await call().ConfigureAwait(false);
        }
        catch (LeaseLostException)
        {
            client.Log(LogSeverity.Warning, null, "{Action}: lease on message {MessageId} lost; it is no longer ours", what, lease.Message.Id);
        }
        catch (Exception e)
        {
            client.Log(
                LogSeverity.Error,
                e,
                "{Action} of message {MessageId} failed; the lease will expire and the message come back: {Error}",
                what,
                lease.Message.Id,
                e.Message);
        }
    }

    // The host and the process, or empty when the host is unknown.
    private static string DefaultConsumer()
    {
        try
        {
            return Environment.MachineName + ":" + Environment.ProcessId;
        }
        catch (InvalidOperationException)
        {
            return "";
        }
    }
}
