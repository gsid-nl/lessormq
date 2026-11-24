using System.Collections.Concurrent;
using LessorMQ.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LessorMQ.Tests;

public sealed class HostingTests
{
    private const string Claims = "/v1/queues/mail/claims";

    private static readonly Answer Empty = new(204, DelayMs: 20);

    private static FakeServer Fake() => new FakeServer()
        .On("/v1/messages/m1/ack", new Answer(204))
        .On("/v1/messages/m1/fail", new Answer(200, FakeServer.FailJson))
        .On("/v1/messages/m1/release", new Answer(204))
        .On(Claims, new Answer(200, FakeServer.LeaseJson), Empty);

    private static HostApplicationBuilder Builder(Dictionary<string, string?>? settings = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(settings ?? new Dictionary<string, string?>
        {
            ["LessorMQ:Url"] = "http://lessormq.test",
            ["LessorMQ:ApiKey"] = "lmq_key",
            ["LessorMQ:RetryDelay"] = "00:00:00",
            ["LessorMQ:Workers:mail:WaitSeconds"] = "1",
        });
        return builder;
    }

    private static async Task Until(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "timed out waiting");
            await Task.Delay(10);
        }
    }

    [Fact]
    public void OptionsComeFromConfiguration()
    {
        var builder = Builder(new Dictionary<string, string?>
        {
            ["LessorMQ:Url"] = "https://mq.example.com",
            ["LessorMQ:ApiKey"] = "lmq_key",
            ["LessorMQ:Retries"] = "5",
            ["LessorMQ:Timeout"] = "00:00:30",
            ["LessorMQ:AutoDedupKey"] = "false",
        });
        builder.Services.AddLogging();
        builder.Services.AddLessorMQ(builder.Configuration.GetSection("LessorMQ"));
        using var provider = builder.Services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<LessorMQClientOptions>>().Value;
        Assert.Equal("https://mq.example.com", options.Url);
        Assert.Equal(5, options.Retries);
        Assert.Equal(TimeSpan.FromSeconds(30), options.Timeout);
        Assert.False(options.AutoDedupKey);
        // Connected to ILogger.
        Assert.NotNull(options.Log);
        Assert.Same(provider.GetRequiredService<LessorMQClient>(), provider.GetRequiredService<LessorMQClient>());
    }

    [Fact]
    public async Task AMissingKeyStopsTheHostAtStart()
    {
        var builder = Builder(new Dictionary<string, string?> { ["LessorMQ:Url"] = "https://mq.example.com" });
        builder.Services.AddLessorMQ(builder.Configuration.GetSection("LessorMQ"));
        using var host = builder.Build();

        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync());
        Assert.Contains("LessorMQ: ApiKey is required.", error.Message);
    }

    [Fact]
    public async Task AHostedWorkerRunsAHandlerFromItsOwnScopeAndAcks()
    {
        var fake = Fake();
        var builder = Builder();
        builder.Services.AddSingleton<Seen>();
        builder.Services.AddScoped<PerMessage>();
        builder.Services.AddLessorMQ(builder.Configuration.GetSection("LessorMQ"))
            .UseHttpClient(_ => fake.HttpClient())
            .AddWorker<RecordingHandler>("mail", o => o.Route = "vpn-3");
        using var host = builder.Build();

        await host.StartAsync();
        await Until(() => fake.Calls("/v1/messages/m1/ack").Count == 1);
        await host.StopAsync();

        var seen = host.Services.GetRequiredService<Seen>();
        Assert.Equal("m1", Assert.Single(seen.Messages).Id);
        Assert.Single(seen.Scopes);
        Assert.True(Assert.Single(seen.Scopes).Disposed);
        var claim = fake.Calls(Claims)[0].Body!;
        Assert.Equal("vpn-3", (string?)claim["route"]);
        Assert.Equal(1, (int?)claim["wait_seconds"]);
    }

    [Fact]
    public async Task AHandlerExceptionIsARetryableFailAndPermanentFailureExceptionAPermanentOne()
    {
        var fake = Fake().On(Claims, new Answer(200, FakeServer.LeaseJson), new Answer(200, FakeServer.LeaseJson), Empty);
        var logs = new LogSink();
        var builder = Builder();
        builder.Logging.AddProvider(logs);
        var thrown = new ConcurrentQueue<Exception>([new InvalidOperationException("smtp down"), new PermanentFailureException("no recipient")]);
        builder.Services.AddLessorMQ(builder.Configuration.GetSection("LessorMQ"))
            .UseHttpClient(_ => fake.HttpClient())
            .AddWorker("mail", (_, _, _) => thrown.TryDequeue(out var e) ? throw e : Task.CompletedTask);
        using var host = builder.Build();

        await host.StartAsync();
        await Until(() => fake.Calls("/v1/messages/m1/fail").Count == 2);
        await host.StopAsync();

        var fails = fake.Calls("/v1/messages/m1/fail").Select(c => c.Body!).ToList();
        Assert.False((bool)fails[0]["permanent"]!);
        Assert.Equal("System.InvalidOperationException", (string?)fails[0]["error"]!["type"]);
        Assert.True((bool)fails[1]["permanent"]!);
        Assert.Contains(logs.Lines, l => l == "LessorMQ Information: Message m1 failed: smtp down; the server says retrying (attempt 1 of 7)");
    }

    [Fact]
    public async Task StoppingTheHostReleasesAMessageWhoseHandlerGivesUp()
    {
        var fake = Fake();
        var started = new TaskCompletionSource();
        var builder = Builder();
        builder.Services.AddLessorMQ(o =>
        {
            o.Url = "http://lessormq.test";
            o.ApiKey = "lmq_key";
        })
            .UseHttpClient(_ => fake.HttpClient())
            .AddWorker(
                "mail",
                async (_, _, cancellationToken) =>
                {
                    started.SetResult();
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                },
                o => o.WaitSeconds = 1);
        using var host = builder.Build();

        await host.StartAsync();
        await started.Task;
        await host.StopAsync();

        Assert.Single(fake.Calls("/v1/messages/m1/release"));
        Assert.Empty(fake.Calls("/v1/messages/m1/fail"));
    }

    [Fact]
    public void EveryQueueGetsAHostedServiceOfItsOwn()
    {
        var builder = Builder();
        builder.Services.AddLessorMQ(builder.Configuration.GetSection("LessorMQ"))
            .AddWorker<RecordingHandler>("mail")
            .AddWorker<RecordingHandler>("invoices");

        Assert.Equal(2, builder.Services.Count(s => s.ServiceType == typeof(IHostedService)));
        Assert.Single(builder.Services, s => s.ServiceType == typeof(RecordingHandler) && s.Lifetime == ServiceLifetime.Scoped);
    }

    private sealed class Seen
    {
        public ConcurrentQueue<Message> Messages { get; } = [];

        public ConcurrentQueue<PerMessage> Scopes { get; } = [];
    }

    private sealed class PerMessage : IDisposable
    {
        public bool Disposed { get; private set; }

        public void Dispose() => Disposed = true;
    }

    private sealed class RecordingHandler(Seen seen, PerMessage scoped) : IMessageHandler
    {
        public Task HandleAsync(Job job, CancellationToken cancellationToken)
        {
            seen.Messages.Enqueue(job.Message);
            seen.Scopes.Enqueue(scoped);
            return Task.CompletedTask;
        }
    }

    private sealed class LogSink : ILoggerProvider, ILogger
    {
        public ConcurrentQueue<string> Lines { get; } = [];

        public ILogger CreateLogger(string categoryName) => categoryName == "LessorMQ" ? this : Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Enqueue($"LessorMQ {logLevel}: {formatter(state, exception)}");

        public void Dispose()
        {
        }
    }
}
