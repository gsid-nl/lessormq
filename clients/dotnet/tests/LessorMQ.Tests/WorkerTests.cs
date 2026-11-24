using System.Collections.Concurrent;

namespace LessorMQ.Tests;

public sealed class WorkerTests
{
    private const string Claims = "/v1/queues/mail/claims";

    // An empty long poll: a short wait, as the server would make.
    private static readonly Answer Empty = new(204, DelayMs: 20);

    private static FakeServer Fake() => new FakeServer()
        .On("/v1/messages/m1/ack", new Answer(204))
        .On("/v1/messages/m1/fail", new Answer(200, FakeServer.FailJson))
        .On("/v1/messages/m1/release", new Answer(204))
        .On("/v1/messages/m1/extend", new Answer(200, FakeServer.ExtendedJson));

    private static WorkerOptions Once(Action<WorkerOptions>? configure = null)
    {
        var options = new WorkerOptions { MaxMessages = 1, WaitSeconds = 1, ErrorPause = TimeSpan.FromMilliseconds(10) };
        configure?.Invoke(options);
        return options;
    }

    [Fact]
    public async Task AHandlerThatReturnsAcksTheMessage()
    {
        var fake = Fake().On(Claims, new Answer(200, FakeServer.LeaseJson), Empty);
        using var client = fake.Client();
        Message? seen = null;
        var worker = new LessorMQWorker(client, "mail", (job, _) =>
        {
            seen = job.Message;
            return Task.CompletedTask;
        }, Once());

        await worker.RunAsync();

        Assert.Equal("m1", seen?.Id);
        Assert.Equal(1, worker.Handled);
        Assert.Equal("""{"receipt":"r1"}""", Assert.Single(fake.Calls("/v1/messages/m1/ack")).Body!.ToJsonString());
        Assert.Empty(fake.Calls("/v1/messages/m1/fail"));
        var claim = fake.Calls(Claims)[0].Body!;
        Assert.Equal(Environment.MachineName + ":" + Environment.ProcessId, (string?)claim["consumer"]);
        Assert.Equal(1, (int?)claim["wait_seconds"]);
    }

    [Fact]
    public async Task AHandlerThatThrowsFailsTheMessageAndTheServerDecides()
    {
        var fake = Fake().On(Claims, new Answer(200, FakeServer.LeaseJson), Empty);
        var events = new ConcurrentQueue<LogEvent>();
        using var client = fake.Client(o => o.Log = events.Enqueue);
        var worker = new LessorMQWorker(client, "mail", (_, _) => throw new InvalidOperationException("smtp down"), Once());

        await worker.RunAsync();

        var fail = Assert.Single(fake.Calls("/v1/messages/m1/fail")).Body!;
        Assert.Equal("smtp down", (string?)fail["error"]!["message"]);
        Assert.Equal("System.InvalidOperationException", (string?)fail["error"]!["type"]);
        Assert.False((bool)fail["permanent"]!);
        Assert.Empty(fake.Calls("/v1/messages/m1/ack"));
        Assert.Contains(events, e => e.Severity == LogSeverity.Information
            && e.ToString() == "Message m1 failed: smtp down; the server says retrying (attempt 1 of 7)");
    }

    [Fact]
    public async Task APermanentFailureIsReportedAsPermanent()
    {
        var fake = Fake().On(Claims, new Answer(200, FakeServer.LeaseJson), Empty);
        using var client = fake.Client();
        var worker = new LessorMQWorker(
            client,
            "mail",
            async (_, _) =>
            {
                await Task.Yield();
                throw new PermanentFailureException(new FormatException("no recipient"));
            },
            Once());

        await worker.RunAsync();

        var fail = Assert.Single(fake.Calls("/v1/messages/m1/fail")).Body!;
        Assert.True((bool)fail["permanent"]!);
        Assert.Equal("System.FormatException", (string?)fail["error"]!["type"]);
    }

    [Fact]
    public async Task TheLeaseIsExtendedWhileTheHandlerRuns()
    {
        var fake = Fake().On(Claims, new Answer(200, FakeServer.LeaseJson), Empty);
        using var client = fake.Client();
        var worker = new LessorMQWorker(
            client,
            "mail",
            (_, cancellationToken) => Task.Delay(400, cancellationToken),
            Once(o =>
            {
                o.ExtendEvery = TimeSpan.FromMilliseconds(50);
                o.LeaseSeconds = 60;
            }));

        await worker.RunAsync();

        var extends = fake.Calls("/v1/messages/m1/extend");
        Assert.InRange(extends.Count, 3, 9);
        Assert.Equal("""{"receipt":"r1","lease_seconds":60}""", extends[0].Body!.ToJsonString());
        Assert.Single(fake.Calls("/v1/messages/m1/ack"));
    }

    [Fact]
    public async Task WithoutAutoExtendTheLeaseIsLeftAlone()
    {
        var fake = Fake().On(Claims, new Answer(200, FakeServer.LeaseJson), Empty);
        using var client = fake.Client();
        var worker = new LessorMQWorker(
            client,
            "mail",
            (_, cancellationToken) => Task.Delay(200, cancellationToken),
            Once(o =>
            {
                o.ExtendEvery = TimeSpan.FromMilliseconds(20);
                o.AutoExtend = false;
            }));

        await worker.RunAsync();

        Assert.Empty(fake.Calls("/v1/messages/m1/extend"));
        Assert.Single(fake.Calls("/v1/messages/m1/ack"));
    }

    [Fact]
    public async Task ALostLeaseCancelsTheHandlerAndNothingIsReported()
    {
        var fake = Fake()
            .On(Claims, new Answer(200, FakeServer.LeaseJson), Empty)
            .On("/v1/messages/m1/extend", new Answer(409, FakeServer.Problem(409, "lease_lost")));
        using var client = fake.Client();
        var cancelled = false;
        var worker = new LessorMQWorker(
            client,
            "mail",
            async (_, cancellationToken) =>
            {
                try
                {
                    await Task.Delay(5000, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    cancelled = true;
                    throw;
                }
            },
            Once(o => o.ExtendEvery = TimeSpan.FromMilliseconds(30)));

        await worker.RunAsync();

        Assert.True(cancelled);
        Assert.Empty(fake.Calls("/v1/messages/m1/ack"));
        Assert.Empty(fake.Calls("/v1/messages/m1/fail"));
        Assert.Empty(fake.Calls("/v1/messages/m1/release"));
    }

    [Fact]
    public async Task AFailedExtendIsTriedAgainAndDoesNotFailTheMessage()
    {
        var fake = Fake()
            .On(Claims, new Answer(200, FakeServer.LeaseJson), Empty)
            .On("/v1/messages/m1/extend", new Answer(0, Drop: true), new Answer(0, Drop: true), new Answer(200, FakeServer.ExtendedJson));
        using var client = fake.Client(o => o.Retries = 0);
        var worker = new LessorMQWorker(
            client,
            "mail",
            (_, cancellationToken) => Task.Delay(300, cancellationToken),
            Once(o => o.ExtendEvery = TimeSpan.FromMilliseconds(40)));

        await worker.RunAsync();

        Assert.True(fake.Calls("/v1/messages/m1/extend").Count >= 3);
        Assert.Single(fake.Calls("/v1/messages/m1/ack"));
    }

    [Fact]
    public async Task StoppingLetsAWaitingClaimFinishAndGivesItsMessageBack()
    {
        var fake = Fake().On(Claims, new Answer(200, FakeServer.LeaseJson, DelayMs: 300), Empty);
        using var client = fake.Client();
        var ran = false;
        var worker = new LessorMQWorker(client, "mail", (_, _) =>
        {
            ran = true;
            return Task.CompletedTask;
        });
        using var stop = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await worker.RunAsync(stop.Token);

        Assert.False(ran);
        Assert.Equal(0, worker.Handled);
        Assert.Single(fake.Calls(Claims));
        Assert.Single(fake.Calls("/v1/messages/m1/release"));
    }

    [Fact]
    public async Task AHandlerThatGivesUpOnStopHasItsMessageReleased()
    {
        var fake = Fake().On(Claims, new Answer(200, FakeServer.LeaseJson), Empty);
        using var client = fake.Client();
        using var stop = new CancellationTokenSource();
        var worker = new LessorMQWorker(client, "mail", async (_, cancellationToken) =>
        {
            await stop.CancelAsync();
            await Task.Delay(5000, cancellationToken);
        });

        await worker.RunAsync(stop.Token);

        Assert.Single(fake.Calls("/v1/messages/m1/release"));
        Assert.Empty(fake.Calls("/v1/messages/m1/fail"));
        Assert.Empty(fake.Calls("/v1/messages/m1/ack"));
    }

    [Fact]
    public async Task AHandlerThatFinishesAfterAStopIsAcked()
    {
        var fake = Fake().On(Claims, new Answer(200, FakeServer.LeaseJson), Empty);
        using var client = fake.Client();
        using var stop = new CancellationTokenSource();
        var worker = new LessorMQWorker(client, "mail", async (_, _) =>
        {
            await stop.CancelAsync();
            await Task.Delay(100);
        });

        await worker.RunAsync(stop.Token);

        Assert.Single(fake.Calls("/v1/messages/m1/ack"));
        Assert.Equal(1, worker.Handled);
        Assert.Single(fake.Calls(Claims));
    }

    [Fact]
    public async Task ConcurrentLoopsStopAtMaxMessages()
    {
        var fake = Fake().On(Claims, new Answer(200, FakeServer.LeaseJson, DelayMs: 5));
        using var client = fake.Client();
        var running = 0;
        var most = 0;
        var worker = new LessorMQWorker(
            client,
            "mail",
            async (_, _) =>
            {
                var now = Interlocked.Increment(ref running);
                InterlockedMax(ref most, now);
                await Task.Delay(30);
                Interlocked.Decrement(ref running);
            },
            new WorkerOptions { Concurrency = 4, MaxMessages = 10 });

        await worker.RunAsync();

        Assert.Equal(10, worker.Handled);
        Assert.Equal(10, fake.Calls("/v1/messages/m1/ack").Count);
        Assert.InRange(most, 2, 4);
        // A loop that claimed after the tenth was taken on gives its message back.
        Assert.Equal(fake.Calls(Claims).Count - 10, fake.Calls("/v1/messages/m1/release").Count);
    }

    [Fact]
    public async Task AFailedClaimIsLoggedAndTheWorkerGoesOn()
    {
        var fake = Fake().On(Claims, new Answer(401, FakeServer.Problem(401, "unauthorized")), new Answer(200, FakeServer.LeaseJson), Empty);
        var events = new ConcurrentQueue<LogEvent>();
        using var client = fake.Client(o => o.Log = events.Enqueue);
        var worker = new LessorMQWorker(client, "mail", (_, _) => Task.CompletedTask, Once());

        await worker.RunAsync();

        Assert.Equal(1, worker.Handled);
        var error = Assert.Single(events, e => e.Severity == LogSeverity.Error);
        Assert.IsType<UnauthorizedException>(error.Exception);
        Assert.StartsWith("Claim from mail failed", error.ToString());
    }

    [Fact]
    public async Task AReportThatFailsDoesNotStopTheWorker()
    {
        var fake = Fake()
            .On(Claims, new Answer(200, FakeServer.LeaseJson))
            .On("/v1/messages/m1/ack", new Answer(500, FakeServer.Problem(500, "internal")));
        var events = new ConcurrentQueue<LogEvent>();
        using var client = fake.Client(o =>
        {
            o.Retries = 0;
            o.Log = events.Enqueue;
        });
        var worker = new LessorMQWorker(client, "mail", (_, _) => Task.CompletedTask, new WorkerOptions { MaxMessages = 2 });

        await worker.RunAsync();

        Assert.Equal(2, worker.Handled);
        Assert.Equal(2, events.Count(e => e.Severity == LogSeverity.Error && e.ToString().StartsWith("Ack of message m1 failed")));
    }

    [Fact]
    public async Task ALeaseLostBeforeTheAckIsLoggedAndDropped()
    {
        var fake = Fake()
            .On(Claims, new Answer(200, FakeServer.LeaseJson), Empty)
            .On("/v1/messages/m1/ack", new Answer(409, FakeServer.Problem(409, "lease_lost")));
        var events = new ConcurrentQueue<LogEvent>();
        using var client = fake.Client(o => o.Log = events.Enqueue);
        var worker = new LessorMQWorker(client, "mail", (_, _) => Task.CompletedTask, Once());

        await worker.RunAsync();

        Assert.Single(fake.Calls("/v1/messages/m1/ack"));
        Assert.Contains(events, e => e.Severity == LogSeverity.Warning && e.ToString() == "Ack: lease on message m1 lost; it is no longer ours");
    }

    [Fact]
    public async Task RunOnceHandlesOneMessage()
    {
        var fake = Fake().On(Claims, new Answer(200, FakeServer.LeaseJson), new Answer(204));
        using var client = fake.Client();
        var worker = new LessorMQWorker(client, "mail", (_, _) => Task.CompletedTask, new WorkerOptions { Consumer = "me", Route = "vpn-3", WaitSeconds = 0 });

        Assert.True(await worker.RunOnceAsync());
        Assert.False(await worker.RunOnceAsync());

        Assert.Equal("""{"consumer":"me","route":"vpn-3"}""", fake.Calls(Claims)[0].Body!.ToJsonString());
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int seen;
        while ((seen = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, seen) != seen)
        {
        }
    }
}
