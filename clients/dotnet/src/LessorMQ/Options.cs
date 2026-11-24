using System.Text.Json;

namespace LessorMQ;

/// <summary>Where the server is, the key to act with, and how the client treats its own HTTP calls.</summary>
public sealed class LessorMQClientOptions
{
    /// <summary>The server's base URL, such as <c>https://mq.example.com</c>.</summary>
    public string Url { get; set; } = "";

    /// <summary>The API key, sent as a bearer token on every request.</summary>
    public string ApiKey { get; set; } = "";

    /// <summary>
    /// Retries after a network error, a 5xx or a 429, for calls that are safe to repeat
    /// (all but a publish without a dedup key). Default 3.
    /// </summary>
    public int Retries { get; set; } = 3;

    /// <summary>The base delay between retries, times the attempt number; a 429's <c>Retry-After</c> wins. Default 100 ms.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>The deadline per request; a claim gets its wait on top. Default 10 s.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Give every publish without a dedup key one of its own, so it can be retried
    /// without creating a second message. Default true; without it, such a publish is
    /// never retried.
    /// </summary>
    public bool AutoDedupKey { get; set; } = true;

    /// <summary>The User-Agent header.</summary>
    public string UserAgent { get; set; } = LessorMQClient.DefaultUserAgent;

    /// <summary>
    /// How message bodies (and failure details) are written, and read by
    /// <see cref="Message.Decode{T}"/>. <see cref="JsonSerializerDefaults.Web"/> when null.
    /// </summary>
    public JsonSerializerOptions? SerializerOptions { get; set; }

    /// <summary>
    /// Receives the retries of the client and the troubles of a worker. Nothing is logged
    /// when null. LessorMQ.Extensions.Hosting connects it to <c>ILogger</c>.
    /// </summary>
    public Action<LogEvent>? Log { get; set; }
}

/// <summary>The optional parts of a publish.</summary>
public sealed class PublishOptions
{
    /// <summary>Free string metadata, passed to the worker untouched.</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; set; }

    /// <summary>
    /// Keeps one open message per key: a publish with the key of an open message returns
    /// that one (<see cref="Published.Duplicate"/>).
    /// </summary>
    public string? DedupKey { get; set; }

    /// <summary>Make the message available this many seconds from now. Not together with <see cref="AvailableAt"/>.</summary>
    public int DelaySeconds { get; set; }

    /// <summary>Make the message available at this time.</summary>
    public DateTimeOffset? AvailableAt { get; set; }

    /// <summary>Attempts for this message, within the queue's <c>max_attempts_limit</c>; 0 is the queue's.</summary>
    public int MaxAttempts { get; set; }
}

/// <summary>What a consumer may pass when it claims; the queue's limits apply.</summary>
public sealed class ClaimOptions
{
    /// <summary>The worker's name, shown to administrators.</summary>
    public string? Consumer { get; set; }

    /// <summary>The path this worker runs jobs through: a VPN exit, a proxy, an API account.</summary>
    public string? Route { get; set; }

    /// <summary>Wait up to this long for a message (at most 20); 0 answers at once.</summary>
    public int WaitSeconds { get; set; }

    /// <summary>The lease for this claim; 0 is the queue's.</summary>
    public int LeaseSeconds { get; set; }
}

/// <summary>The optional parts of a failure report.</summary>
public sealed class FailOptions
{
    /// <summary>
    /// The input itself cannot be processed. By default, whether the exception is (or
    /// wraps) a <see cref="PermanentFailureException"/>.
    /// </summary>
    public bool? Permanent { get; set; }

    /// <summary>The error type, used to filter the dead-letter set; by default the exception's type.</summary>
    public string? Type { get; set; }

    /// <summary>Any JSON value with more context; by default the exception's stack trace.</summary>
    public object? Details { get; set; }
}
