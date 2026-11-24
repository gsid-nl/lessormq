using System.Text.Json;
using System.Text.Json.Nodes;

namespace LessorMQ.Tests;

public sealed class ClientTests
{
    private sealed record Mail(string To);

    private static Lease LeaseOf(string id = "m1", string receipt = "r1") =>
        new(receipt, DateTimeOffset.UtcNow, new Message { Id = id });

    [Fact]
    public async Task PublishSendsTheKeyTheBodyAndTheOptions()
    {
        var fake = new FakeServer().On("/v1/queues/mail/messages", new Answer(201, FakeServer.PublishedJson));
        using var client = fake.Client();

        var published = await client.PublishAsync("mail", new Mail("ada@example.com"), new PublishOptions
        {
            Headers = new Dictionary<string, string> { ["type"] = "Welcome" },
            DelaySeconds = 600,
            MaxAttempts = 3,
        });

        Assert.Equal("m1", published.Id);
        Assert.Equal("pending", published.Status);
        Assert.False(published.Duplicate);
        Assert.Equal(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero), published.CreatedAt);

        var call = Assert.Single(fake.Calls("/v1/queues/mail/messages"));
        Assert.Equal("POST", call.Method);
        Assert.Equal("Bearer lmq_key", call.Authorization);
        Assert.Equal(LessorMQClient.DefaultUserAgent, call.UserAgent);
        Assert.Equal("application/json", call.ContentType);
        Assert.Equal("ada@example.com", (string?)call.Body!["body"]!["to"]);
        Assert.Equal("Welcome", (string?)call.Body["headers"]!["type"]);
        Assert.Equal(600, (int?)call.Body["delay_seconds"]);
        Assert.Equal(3, (int?)call.Body["max_attempts"]);
        Assert.Matches("^lmq-auto-[0-9a-f]{24}$", (string?)call.Body["dedup_key"]);
        Assert.Null(call.Body["available_at"]);
    }

    [Fact]
    public async Task PublishTakesAnyJsonAndAnAvailableAt()
    {
        var fake = new FakeServer().On("/v1/queues/mail/messages", new Answer(200, FakeServer.PublishedJson.Replace("false", "true")));
        using var client = fake.Client();

        var published = await client.PublishAsync("mail", JsonNode.Parse("[1, \"two\"]"), new PublishOptions
        {
            DedupKey = "invoice-1042",
            AvailableAt = new DateTimeOffset(2030, 1, 1, 2, 0, 0, TimeSpan.FromHours(2)),
        });
        await client.PublishAsync("mail", "a plain string");
        await client.PublishAsync<object?>("mail", null);

        Assert.True(published.Duplicate);
        var calls = fake.Calls("/v1/queues/mail/messages");
        Assert.Equal("[1,\"two\"]", calls[0].Body!["body"]!.ToJsonString());
        Assert.Equal("invoice-1042", (string?)calls[0].Body!["dedup_key"]);
        Assert.Equal("2030-01-01T00:00:00Z", (string?)calls[0].Body!["available_at"]);
        Assert.Equal("a plain string", (string?)calls[1].Body!["body"]);
        Assert.True(calls[2].Body!.AsObject().ContainsKey("body"));
        Assert.Null(calls[2].Body!["body"]);
    }

    [Fact]
    public async Task APublishIsRetriedWithTheSameDedupKey()
    {
        var fake = new FakeServer().On(
            "/v1/queues/mail/messages",
            new Answer(503, FakeServer.Problem(503, "unavailable")),
            new Answer(0, Drop: true),
            new Answer(201, FakeServer.PublishedJson));
        using var client = fake.Client();

        await client.PublishAsync("mail", new { n = 1 });

        var calls = fake.Calls("/v1/queues/mail/messages");
        Assert.Equal(3, calls.Count);
        Assert.Single(calls.Select(c => (string?)c.Body!["dedup_key"]).Distinct());
    }

    [Fact]
    public async Task APublishWithoutDedupKeyIsNeverRepeated()
    {
        var fake = new FakeServer().On("/v1/queues/mail/messages", new Answer(503, FakeServer.Problem(503, "unavailable")));
        using var client = fake.Client(o => o.AutoDedupKey = false);

        var error = await Assert.ThrowsAsync<ServerException>(() => client.PublishAsync("mail", new { n = 1 }));
        Assert.Equal(503, error.Status);
        Assert.Equal("unavailable", error.Code);
        Assert.Single(fake.Calls("/v1/queues/mail/messages"));
        Assert.Null(fake.Calls("/v1/queues/mail/messages")[0].Body!["dedup_key"]);

        var dropped = new FakeServer().On("/v1/queues/mail/messages", new Answer(0, Drop: true));
        using var other = dropped.Client(o => o.AutoDedupKey = false);
        var transport = await Assert.ThrowsAsync<TransportException>(() => other.PublishAsync("mail", new { n = 1 }));
        Assert.Equal("POST", transport.Method);
        Assert.Equal("/v1/queues/mail/messages", transport.Path);
        Assert.IsType<HttpRequestException>(transport.InnerException);
        Assert.Single(dropped.Calls("/v1/queues/mail/messages"));
    }

    [Fact]
    public async Task AClaimReturnsTheLeaseAndIgnoresFieldsItDoesNotKnow()
    {
        var fake = new FakeServer().On("/v1/queues/mail/claims", new Answer(200, FakeServer.LeaseJson));
        using var client = fake.Client();

        var lease = await client.ClaimAsync("mail", new ClaimOptions { Consumer = "host:1", Route = "vpn-3", WaitSeconds = 5, LeaseSeconds = 60 });

        Assert.NotNull(lease);
        Assert.Equal("r1", lease.Receipt);
        Assert.Equal(new DateTimeOffset(2030, 1, 1, 0, 0, 30, TimeSpan.Zero), lease.Until);
        Assert.Equal("m1", lease.Message.Id);
        Assert.Equal("mail", lease.Message.Queue);
        Assert.Equal("Welcome", lease.Message.Headers["type"]);
        Assert.Equal(1, lease.Message.Attempt);
        Assert.Equal(7, lease.Message.MaxAttempts);
        Assert.False(lease.Message.IsLastAttempt);
        Assert.Equal("ada@example.com", lease.Message.Decode<Mail>()!.To);
        Assert.Equal("ada@example.com", lease.Message.Body.GetProperty("to").GetString());

        var body = Assert.Single(fake.Calls("/v1/queues/mail/claims")).Body!;
        Assert.Equal("""{"consumer":"host:1","route":"vpn-3","wait_seconds":5,"lease_seconds":60}""", body.ToJsonString());
    }

    [Fact]
    public async Task AnEmptyClaimIsNull()
    {
        var fake = new FakeServer().On("/v1/queues/mail/claims", new Answer(204));
        using var client = fake.Client();

        Assert.Null(await client.ClaimAsync("mail"));
        Assert.Equal("{}", fake.Calls("/v1/queues/mail/claims")[0].Body!.ToJsonString());
    }

    [Fact]
    public async Task AQueueNameIsEscapedInThePath()
    {
        var fake = new FakeServer().On("/v1/queues/a%20b/claims", new Answer(204));
        using var client = fake.Client();

        Assert.Null(await client.ClaimAsync("a b"));
    }

    [Fact]
    public async Task ABatchClaimReturnsEveryLease()
    {
        var second = FakeServer.LeaseJson.Replace("\"r1\"", "\"r2\"").Replace("\"m1\"", "\"m2\"");
        var fake = new FakeServer().On(
            "/v1/queues/mail/claims",
            new Answer(200, $$"""{"items":[{{FakeServer.LeaseJson}},{{second}}]}"""),
            new Answer(204));
        using var client = fake.Client();

        var leases = await client.ClaimBatchAsync("mail", 10, new ClaimOptions { WaitSeconds = 2 });
        var none = await client.ClaimBatchAsync("mail", 10);

        Assert.Equal(["r1", "r2"], leases.Select(l => l.Receipt));
        Assert.Equal(["m1", "m2"], leases.Select(l => l.Message.Id));
        Assert.Empty(none);
        Assert.Equal(10, (int?)fake.Calls("/v1/queues/mail/claims")[0].Body!["max_messages"]);
    }

    [Fact]
    public async Task AnAckBatchGivesAnOutcomePerLease()
    {
        var fake = new FakeServer().On(
            "/v1/acks",
            new Answer(200, """{"items":[{"message":"m1","outcome":"acked"},{"message":"m2","outcome":"lease_lost"}]}"""));
        using var client = fake.Client();

        var outcomes = await client.AckBatchAsync([LeaseOf("m1", "r1"), LeaseOf("m2", "r2")]);

        Assert.Equal([AckOutcome.Acked, AckOutcome.LeaseLost], outcomes);
        var body = Assert.Single(fake.Calls("/v1/acks")).Body!;
        Assert.Equal("""{"leases":[{"message":"m1","receipt":"r1"},{"message":"m2","receipt":"r2"}]}""", body.ToJsonString());
    }

    [Fact]
    public async Task ExtendMovesTheLease()
    {
        var fake = new FakeServer().On("/v1/messages/m1/extend", new Answer(200, FakeServer.ExtendedJson));
        using var client = fake.Client();
        var lease = LeaseOf();

        await client.ExtendAsync(lease, 300);

        Assert.Equal(new DateTimeOffset(2030, 1, 1, 0, 5, 0, TimeSpan.Zero), lease.Until);
        Assert.Equal("""{"receipt":"r1","lease_seconds":300}""", fake.Calls("/v1/messages/m1/extend")[0].Body!.ToJsonString());
    }

    [Fact]
    public async Task ALostLeaseIsNotRetried()
    {
        var fake = new FakeServer().On("/v1/messages/m1/ack", new Answer(409, FakeServer.Problem(409, "lease_lost", "not the current lease")));
        using var client = fake.Client();

        var error = await Assert.ThrowsAsync<LeaseLostException>(() => client.AckAsync(LeaseOf()));

        Assert.Equal(409, error.Status);
        Assert.Equal("LessorMQ: not the current lease", error.Message);
        Assert.Single(fake.Calls("/v1/messages/m1/ack"));
        Assert.Equal("""{"receipt":"r1"}""", fake.Calls("/v1/messages/m1/ack")[0].Body!.ToJsonString());
    }

    [Fact]
    public async Task FailReportsTheExceptionItsTypeAndWhetherItIsPermanent()
    {
        var fake = new FakeServer().On("/v1/messages/m1/fail", new Answer(200, FakeServer.FailJson));
        using var client = fake.Client();

        var result = await client.FailAsync(LeaseOf(), Thrown(new InvalidOperationException("smtp down")));
        await client.FailAsync(LeaseOf(), new PermanentFailureException(new FormatException("no recipient")));
        await client.FailAsync(LeaseOf(), new InvalidOperationException("wrapped", new PermanentFailureException("bad input")));
        await client.FailAsync(LeaseOf(), new PermanentFailureException("said in words"));

        Assert.Equal(FailOutcome.Retrying, result.Outcome);
        Assert.Equal(new DateTimeOffset(2030, 1, 1, 0, 0, 10, TimeSpan.Zero), result.AvailableAt);
        Assert.Equal(1, result.Attempt);
        Assert.Equal(7, result.MaxAttempts);

        var calls = fake.Calls("/v1/messages/m1/fail").Select(c => c.Body!).ToList();
        Assert.Equal("r1", (string?)calls[0]["receipt"]);
        Assert.Equal("smtp down", (string?)calls[0]["error"]!["message"]);
        Assert.Equal("System.InvalidOperationException", (string?)calls[0]["error"]!["type"]);
        Assert.Contains("Thrown", (string?)calls[0]["error"]!["details"]!["exception"]);
        Assert.False((bool)calls[0]["permanent"]!);

        Assert.Equal("no recipient", (string?)calls[1]["error"]!["message"]);
        Assert.Equal("System.FormatException", (string?)calls[1]["error"]!["type"]);
        Assert.True((bool)calls[1]["permanent"]!);

        Assert.Equal("System.InvalidOperationException", (string?)calls[2]["error"]!["type"]);
        Assert.True((bool)calls[2]["permanent"]!);

        Assert.Equal("LessorMQ.PermanentFailureException", (string?)calls[3]["error"]!["type"]);
        Assert.True((bool)calls[3]["permanent"]!);
    }

    [Fact]
    public async Task FailOptionsOverrideWhatTheExceptionSays()
    {
        var fake = new FakeServer().On("/v1/messages/m1/fail", new Answer(200, FakeServer.FailJson));
        using var client = fake.Client();

        await client.FailAsync(
            LeaseOf(),
            new PermanentFailureException("bad"),
            new FailOptions { Permanent = false, Type = "BadInput", Details = new { code = 7 } });
        await client.FailAsync(LeaseOf(), new string('x', 70000) + "\U0001F600", new FailOptions { Permanent = true });
        await client.FailAsync(LeaseOf(), "");

        var calls = fake.Calls("/v1/messages/m1/fail").Select(c => c.Body!).ToList();
        Assert.False((bool)calls[0]["permanent"]!);
        Assert.Equal("BadInput", (string?)calls[0]["error"]!["type"]);
        Assert.Equal("""{"code":7}""", calls[0]["error"]!["details"]!.ToJsonString());

        Assert.Equal(65535, ((string?)calls[1]["error"]!["message"])!.Length);
        Assert.True((bool)calls[1]["permanent"]!);
        Assert.Null(calls[1]["error"]!["type"]);
        Assert.Null(calls[1]["error"]!["details"]);

        Assert.Equal("(no message)", (string?)calls[2]["error"]!["message"]);
    }

    [Fact]
    public async Task ReleaseGivesTheMessageBack()
    {
        var fake = new FakeServer().On("/v1/messages/m1/release", new Answer(204));
        using var client = fake.Client();

        await client.ReleaseAsync(LeaseOf(), 30);
        await client.ReleaseAsync(LeaseOf());

        var calls = fake.Calls("/v1/messages/m1/release");
        Assert.Equal("""{"receipt":"r1","delay_seconds":30}""", calls[0].Body!.ToJsonString());
        Assert.Equal("""{"receipt":"r1"}""", calls[1].Body!.ToJsonString());
    }

    [Fact]
    public async Task RateLimitsAreWaitedOutThenThrown()
    {
        var fake = new FakeServer().On(
            "/v1/queues/mail/messages",
            new Answer(429, FakeServer.Problem(429, "rate_limited"), RetryAfter: 0));
        using var client = fake.Client(o =>
        {
            o.AutoDedupKey = false;
            o.Retries = 2;
        });

        var error = await Assert.ThrowsAsync<RateLimitedException>(() => client.PublishAsync("mail", new { }));

        // A 429 was not processed, so even a publish without a dedup key is sent again.
        Assert.Equal(3, fake.Calls("/v1/queues/mail/messages").Count);
        Assert.Equal(TimeSpan.Zero, error.RetryAfter);
        Assert.Equal("rate_limited", error.Code);
    }

    [Fact]
    public async Task ServerErrorsAndNetworkErrorsAreRetried()
    {
        var fake = new FakeServer()
            .On("/v1/messages/m1/ack", new Answer(502, "<html>bad gateway</html>"))
            .On("/v1/messages/m1/release", new Answer(0, Drop: true), new Answer(204));
        using var client = fake.Client();

        var error = await Assert.ThrowsAsync<ServerException>(() => client.AckAsync(LeaseOf()));
        await client.ReleaseAsync(LeaseOf());

        Assert.Equal(502, error.Status);
        Assert.Equal("internal", error.Code);
        Assert.Equal(4, fake.Calls("/v1/messages/m1/ack").Count);
        Assert.Equal(2, fake.Calls("/v1/messages/m1/release").Count);
    }

    [Theory]
    [InlineData(400, "validation_failed", typeof(ValidationFailedException))]
    [InlineData(401, "unauthorized", typeof(UnauthorizedException))]
    [InlineData(403, "forbidden", typeof(ForbiddenException))]
    [InlineData(403, "tenant_suspended", typeof(ForbiddenException))]
    [InlineData(403, "quota_exceeded", typeof(ApiException))]
    [InlineData(404, "not_found", typeof(NotFoundException))]
    [InlineData(409, "invalid_state", typeof(InvalidStateException))]
    [InlineData(413, "payload_too_large", typeof(PayloadTooLargeException))]
    public async Task ProblemCodesBecomeTypedExceptions(int status, string code, Type expected)
    {
        var fake = new FakeServer().On("/v1/queues/mail/messages", new Answer(status, FakeServer.Problem(status, code)));
        using var client = fake.Client();

        var error = await Assert.ThrowsAnyAsync<ApiException>(() => client.PublishAsync("mail", new { }));

        Assert.IsType(expected, error);
        Assert.Equal(code, error.Code);
        Assert.Equal(status, error.Status);
        Assert.Single(fake.Calls("/v1/queues/mail/messages"));
    }

    [Fact]
    public async Task AValidationErrorNamesTheFields()
    {
        var problem = """{"code":"validation_failed","status":400,"detail":"invalid","errors":[{"field":"max_messages","message":"at most 100"}]}""";
        var fake = new FakeServer()
            .On("/v1/queues/mail/claims", new Answer(400, problem))
            .On("/v1/queues/other/claims", new Answer(400, "not json"));
        using var client = fake.Client();

        var error = await Assert.ThrowsAsync<ValidationFailedException>(() => client.ClaimBatchAsync("mail", 101));
        var bare = await Assert.ThrowsAsync<ValidationFailedException>(() => client.ClaimAsync("other"));

        Assert.Equal([new FieldError("max_messages", "at most 100")], error.Errors);
        Assert.Equal("LessorMQ: invalid", error.Message);
        Assert.Empty(bare.Errors);
        Assert.Equal("LessorMQ: error 400 (validation_failed)", bare.Message);
    }

    [Fact]
    public async Task CancellingIsNotATransportError()
    {
        var fake = new FakeServer().On("/v1/queues/mail/claims", new Answer(204, DelayMs: 5000));
        using var client = fake.Client();
        using var cancel = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.ClaimAsync("mail", null, cancel.Token));
        Assert.Single(fake.Calls("/v1/queues/mail/claims"));
    }

    [Fact]
    public async Task ARequestWithoutAnswerTimesOutAndAClaimGetsItsWaitOnTop()
    {
        var fake = new FakeServer()
            .On("/v1/messages/m1/ack", new Answer(204, DelayMs: 5000))
            .On("/v1/queues/mail/claims", new Answer(204, DelayMs: 500));
        using var client = fake.Client(o =>
        {
            o.Timeout = TimeSpan.FromMilliseconds(200);
            o.Retries = 1;
        });

        var error = await Assert.ThrowsAsync<TransportException>(() => client.AckAsync(LeaseOf()));
        var lease = await client.ClaimAsync("mail", new ClaimOptions { WaitSeconds = 1 });

        Assert.IsType<TimeoutException>(error.InnerException);
        Assert.Equal(2, fake.Calls("/v1/messages/m1/ack").Count);
        Assert.Null(lease);
    }

    [Fact]
    public async Task StatsAreTheQueuesCounts()
    {
        var fake = new FakeServer().On(
            "/v1/queues/mail",
            new Answer(200, """{"name":"mail","settings":{},"stats":{"ready":3,"delayed":1,"processing":2,"completed":9,"failed":4,"oldest_ready_seconds":12,"oldest_processing_seconds":5,"workers":2}}"""));
        using var client = fake.Client();

        var stats = await client.GetStatsAsync("mail");

        Assert.Equal(3, stats.Ready);
        Assert.Equal(1, stats.Delayed);
        Assert.Equal(2, stats.Processing);
        Assert.Equal(9, stats.Completed);
        Assert.Equal(4, stats.Failed);
        Assert.Equal(12, stats.OldestReadySeconds);
        Assert.Equal(5, stats.OldestProcessingSeconds);
        Assert.Equal(2, stats.Workers);
        Assert.Equal("GET", fake.Calls("/v1/queues/mail")[0].Method);
    }

    [Fact]
    public async Task RetriesAreLogged()
    {
        var events = new List<LogEvent>();
        var fake = new FakeServer().On("/v1/messages/m1/ack", new Answer(503, FakeServer.Problem(503, "unavailable", "database down")), new Answer(204));
        using var client = fake.Client(o => o.Log = events.Add);

        await client.AckAsync(LeaseOf());

        var logged = Assert.Single(events);
        Assert.Equal(LogSeverity.Warning, logged.Severity);
        Assert.Equal("POST", logged.Arguments[0]);
        Assert.Equal(
            "LessorMQ request POST /v1/messages/m1/ack failed, retrying in 00:00:00 (attempt 1): LessorMQ: database down",
            logged.ToString());
    }

    [Fact]
    public void TheClientNeedsAUrlAndAKeyAndNeverShowsTheKey()
    {
        Assert.Throws<ArgumentException>(() => new LessorMQClient("mq.example.com", "lmq_key"));
        Assert.Throws<ArgumentException>(() => new LessorMQClient("ftp://mq.example.com", "lmq_key"));
        Assert.Throws<ArgumentException>(() => new LessorMQClient("https://mq.example.com", " "));

        using var client = new LessorMQClient("https://mq.example.com/", "lmq_secret");
        Assert.Equal("LessorMQClient(https://mq.example.com)", client.ToString());
    }

    [Fact]
    public void AMessageDecodesWithTheOptionsGiven()
    {
        var message = new Message { Body = JsonDocument.Parse("""{"to":"ada@example.com"}""").RootElement.Clone(), Attempt = 7, MaxAttempts = 7 };

        Assert.Equal("ada@example.com", message.Decode<Mail>()!.To);
        Assert.Null(message.Decode<Mail>(new JsonSerializerOptions())!.To);
        Assert.True(message.IsLastAttempt);
        Assert.Empty(message.Headers);
    }

    private static Exception Thrown(Exception exception)
    {
        try
        {
            throw exception;
        }
        catch (Exception e)
        {
            return e;
        }
    }
}
