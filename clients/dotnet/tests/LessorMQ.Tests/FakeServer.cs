using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;

namespace LessorMQ.Tests;

/// <summary>One scripted response; <see cref="Drop"/> fails the connection instead.</summary>
internal sealed record Answer(int Status, string? Body = null, int? RetryAfter = null, bool Drop = false, int DelayMs = 0);

/// <summary>A request as the fake server saw it.</summary>
internal sealed record Recorded(string Method, string Path, string? Authorization, string? UserAgent, string? ContentType, JsonNode? Body);

/// <summary>
/// A scripted LessorMQ server behind a fake HttpMessageHandler: answers queued per path
/// (the last one repeats), every request recorded.
/// </summary>
internal sealed class FakeServer : HttpMessageHandler
{
    public const string LeaseJson =
        """{"receipt":"r1","lease_until":"2030-01-01T00:00:30Z","message":{"id":"m1","queue":"mail","body":{"to":"ada@example.com"},"headers":{"type":"Welcome"},"attempt":1,"max_attempts":7,"created_at":"2030-01-01T00:00:00Z","new_field":true},"also_new":1}""";

    public const string FailJson = """{"outcome":"retrying","available_at":"2030-01-01T00:00:10Z","attempt":1,"max_attempts":7}""";

    public const string PublishedJson =
        """{"id":"m1","queue":"mail","status":"pending","created_at":"2030-01-01T00:00:00Z","available_at":"2030-01-01T00:00:00Z","duplicate":false}""";

    public const string ExtendedJson = """{"lease_until":"2030-01-01T00:05:00Z"}""";

    private readonly object gate = new();
    private readonly Dictionary<string, List<Answer>> answers = [];
    private readonly List<Recorded> requests = [];

    /// <summary>Runs before each answer, with the path.</summary>
    public Func<string, Task>? OnRequest { get; set; }

    public static string Problem(int status, string code, string detail = "") =>
        $$"""{"type":"urn:lessormq:problem:{{code}}","title":"{{code}}","status":{{status}},"code":"{{code}}","detail":"{{detail}}"}""";

    /// <summary>Scripts the answers for a path, in place of any scripted before.</summary>
    public FakeServer On(string path, params Answer[] scripted)
    {
        lock (gate)
        {
            answers[path] = [.. scripted];
        }

        return this;
    }

    public List<Recorded> Calls(string path)
    {
        lock (gate)
        {
            return requests.Where(r => r.Path == path).ToList();
        }
    }

    public int Count
    {
        get
        {
            lock (gate)
            {
                return requests.Count;
            }
        }
    }

    public LessorMQClient Client(Action<LessorMQClientOptions>? configure = null)
    {
        var options = new LessorMQClientOptions
        {
            Url = "http://lessormq.test/",
            ApiKey = "lmq_key",
            RetryDelay = TimeSpan.Zero,
        };
        configure?.Invoke(options);
        return new LessorMQClient(options, HttpClient());
    }

    public HttpClient HttpClient() => new(this, disposeHandler: false) { Timeout = Timeout.InfiniteTimeSpan };

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var path = request.RequestUri!.AbsolutePath;
        JsonNode? body = null;
        string? contentType = null;
        if (request.Content is not null)
        {
            body = JsonNode.Parse(await request.Content.ReadAsStringAsync(cancellationToken));
            contentType = request.Content.Headers.ContentType?.ToString();
        }

        Answer answer;
        lock (gate)
        {
            requests.Add(new Recorded(
                request.Method.Method,
                path,
                request.Headers.Authorization?.ToString(),
                request.Headers.UserAgent.ToString(),
                contentType,
                body));
            if (answers.TryGetValue(path, out var list) && list.Count > 0)
            {
                answer = list[0];
                if (list.Count > 1)
                {
                    list.RemoveAt(0);
                }
            }
            else
            {
                answer = new Answer(404, Problem(404, "not_found", "nothing scripted for " + path));
            }
        }

        if (OnRequest is not null)
        {
            await OnRequest(path);
        }

        if (answer.DelayMs > 0)
        {
            await Task.Delay(answer.DelayMs, cancellationToken);
        }

        if (answer.Drop)
        {
            throw new HttpRequestException("connection reset by fake");
        }

        var response = new HttpResponseMessage((HttpStatusCode)answer.Status)
        {
            Content = new StringContent(answer.Body ?? "", Encoding.UTF8, answer.Status >= 400 ? "application/problem+json" : "application/json"),
        };
        if (answer.RetryAfter is { } seconds)
        {
            response.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromSeconds(seconds));
        }

        return response;
    }
}
