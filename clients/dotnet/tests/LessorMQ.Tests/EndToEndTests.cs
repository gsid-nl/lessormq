using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json.Nodes;
using LessorMQ.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace LessorMQ.Tests;

/// <summary>
/// A hosted worker against a real server: runs only with LESSORMQ_URL and
/// LESSORMQ_OPERATOR_KEY set, in a tenant of its own that it removes afterwards.
/// </summary>
public sealed class EndToEndTests
{
    private static readonly string? Url = Environment.GetEnvironmentVariable("LESSORMQ_URL");
    private static readonly string? OperatorKey = Environment.GetEnvironmentVariable("LESSORMQ_OPERATOR_KEY");

    [Fact]
    public async Task AHostedWorkerAcksRetriesAndDeadLettersAsTheQueueSays()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Url) || string.IsNullOrEmpty(OperatorKey), "needs LESSORMQ_URL and LESSORMQ_OPERATOR_KEY");

        using var http = new HttpClient { BaseAddress = new Uri(Url!.TrimEnd('/') + "/") };
        var tenant = "dotnet-e2e-" + Guid.NewGuid().ToString("N")[..12];
        var created = await Send(http, OperatorKey!, HttpMethod.Post, "v1/tenants", new { id = tenant, name = "LessorMQ .NET end-to-end" });
        var key = (string)created["admin_key"]!["secret"]!;
        try
        {
            const string queue = "jobs";
            await Send(http, key, HttpMethod.Patch, "v1/queues/" + queue, new { max_attempts = 2, backoff = new { initial_seconds = 1, jitter = 0 } });

            using var client = new LessorMQClient(Url!, key);
            var ok = await client.PublishAsync(queue, new { kind = "ok" });
            var failing = await client.PublishAsync(queue, new { kind = "fail" });
            var bad = await client.PublishAsync(queue, new { kind = "bad" });

            var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
            builder.Services.AddSingleton<Attempts>();
            builder.Services.AddLessorMQ(o =>
            {
                o.Url = Url!;
                o.ApiKey = key;
            }).AddWorker<Jobs>(queue, o =>
            {
                o.WaitSeconds = 1;
                o.Concurrency = 2;
            });
            using var host = builder.Build();
            await host.StartAsync();
            var attempts = host.Services.GetRequiredService<Attempts>();

            var deadline = DateTime.UtcNow.AddSeconds(30);
            JsonObject okMessage, failingMessage, badMessage;
            while (true)
            {
                okMessage = await Send(http, key, HttpMethod.Get, "v1/messages/" + ok.Id);
                failingMessage = await Send(http, key, HttpMethod.Get, "v1/messages/" + failing.Id);
                badMessage = await Send(http, key, HttpMethod.Get, "v1/messages/" + bad.Id);
                if ((string?)okMessage["status"] == "completed" && (string?)failingMessage["status"] == "failed" && (string?)badMessage["status"] == "failed")
                {
                    break;
                }

                Assert.True(DateTime.UtcNow < deadline, $"not done: {okMessage["status"]}, {failingMessage["status"]}, {badMessage["status"]}");
                await Task.Delay(200);
            }

            await host.StopAsync();

            // Acked at once.
            Assert.Equal([1], attempts.Of("ok").Select(a => a.Attempt));

            // A retryable failure came back after the queue's backoff with the server's
            // attempt count, and the last one went to the dead-letter set.
            var tries = attempts.Of("fail");
            Assert.Equal([1, 2], tries.Select(a => a.Attempt));
            Assert.True(tries[1].At - tries[0].At >= TimeSpan.FromSeconds(0.9), $"came back after {tries[1].At - tries[0].At}");
            Assert.Equal(2, (int?)failingMessage["attempts"]);
            Assert.Equal("System.InvalidOperationException", (string?)failingMessage["last_error"]!["type"]);
            Assert.False((bool)failingMessage["last_error"]!["permanent"]!);

            // A permanent failure skipped the remaining attempt.
            Assert.Equal([1], attempts.Of("bad").Select(a => a.Attempt));
            Assert.Equal(1, (int?)badMessage["attempts"]);
            Assert.True((bool)badMessage["last_error"]!["permanent"]!);
            Assert.Equal("System.FormatException", (string?)badMessage["last_error"]!["type"]);
        }
        finally
        {
            await Send(http, OperatorKey!, HttpMethod.Patch, "v1/tenants/" + tenant, new { status = "suspended" });
            await Send(http, OperatorKey!, HttpMethod.Delete, "v1/tenants/" + tenant);
        }
    }

    private static async Task<JsonObject> Send(HttpClient http, string key, HttpMethod method, string path, object? body = null)
    {
        using var request = new HttpRequestMessage(method, path);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", key);
        if (body is not null)
        {
            request.Content = JsonContent.Create(body);
        }

        using var response = await http.SendAsync(request);
        var text = await response.Content.ReadAsStringAsync();
        Assert.True(response.IsSuccessStatusCode, $"{method} {path}: {(int)response.StatusCode} {text}");
        return text.Length == 0 ? [] : JsonNode.Parse(text)!.AsObject();
    }

    private sealed record Try(int Attempt, DateTimeOffset At);

    private sealed class Attempts
    {
        private readonly ConcurrentDictionary<string, ConcurrentQueue<Try>> seen = new();

        public void Add(string kind, int attempt) =>
            seen.GetOrAdd(kind, _ => new ConcurrentQueue<Try>()).Enqueue(new Try(attempt, DateTimeOffset.UtcNow));

        public List<Try> Of(string kind) => seen.TryGetValue(kind, out var list) ? [.. list] : [];
    }

    private sealed class Jobs(Attempts attempts) : IMessageHandler
    {
        public Task HandleAsync(Job job, CancellationToken cancellationToken)
        {
            var kind = job.Message.Body.GetProperty("kind").GetString()!;
            attempts.Add(kind, job.Message.Attempt);
            return kind switch
            {
                "fail" => throw new InvalidOperationException("upstream refused"),
                "bad" => throw new PermanentFailureException(new FormatException("no recipient")),
                _ => Task.CompletedTask,
            };
        }
    }
}
