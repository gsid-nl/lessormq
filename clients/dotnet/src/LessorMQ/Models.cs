using System.Globalization;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace LessorMQ;

/// <summary>A delivered message: what a handler runs.</summary>
public sealed class Message
{
    private IReadOnlyDictionary<string, string> headers = Empty;

    private static readonly IReadOnlyDictionary<string, string> Empty = new Dictionary<string, string>();

    /// <summary>The message id. Opaque.</summary>
    public string Id { get; init; } = "";

    /// <summary>The queue it came from.</summary>
    public string Queue { get; init; } = "";

    /// <summary>The JSON value as published; <see cref="Decode{T}"/> deserializes it.</summary>
    public JsonElement Body { get; init; }

    /// <summary>The headers as published; empty when there were none.</summary>
    public IReadOnlyDictionary<string, string> Headers
    {
        get => headers;
        init => headers = value ?? Empty;
    }

    /// <summary>Which attempt this is, counting this one.</summary>
    public int Attempt { get; init; }

    /// <summary>The attempts the queue allows this message.</summary>
    public int MaxAttempts { get; init; }

    /// <summary>When it was published.</summary>
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>Whether this attempt is the last one the queue allows.</summary>
    [JsonIgnore]
    public bool IsLastAttempt => Attempt >= MaxAttempts;

    // The client's body options, so Decode reads what Publish wrote.
    [JsonIgnore]
    internal JsonSerializerOptions? SerializerOptions { get; set; }

    /// <summary>
    /// Deserializes the body, with the client's <see cref="LessorMQClientOptions.SerializerOptions"/>
    /// unless <paramref name="options"/> says otherwise.
    /// </summary>
    public T? Decode<T>(JsonSerializerOptions? options = null) =>
        Body.Deserialize<T>(options ?? SerializerOptions ?? Wire.BodyDefaults);
}

/// <summary>
/// A claimed message and the proof of holding it. Extend, ack, fail and release take the
/// lease; only its receipt is accepted, and only until <see cref="Until"/>, which an
/// extend moves. Safe to use from several threads.
/// </summary>
public sealed class Lease
{
    private long untilTicks;

    /// <summary>A lease from its parts, for adapters and tests; a claim returns real ones.</summary>
    public Lease(string receipt, DateTimeOffset until, Message message)
    {
        Receipt = receipt;
        Message = message;
        untilTicks = until.UtcTicks;
    }

    /// <summary>The receipt of this claim. Opaque.</summary>
    public string Receipt { get; }

    /// <summary>The claimed message.</summary>
    public Message Message { get; }

    /// <summary>When the lease ends unless it is extended.</summary>
    public DateTimeOffset Until => new(Interlocked.Read(ref untilTicks), TimeSpan.Zero);

    /// <summary>The time until the lease ends; negative once it has.</summary>
    public TimeSpan Remaining => Until - DateTimeOffset.UtcNow;

    internal void ExtendedTo(DateTimeOffset until) => Interlocked.Exchange(ref untilTicks, until.UtcTicks);
}

/// <summary>The answer to a publish: the message, or the open one with the same dedup key.</summary>
public sealed class Published
{
    /// <summary>The message id.</summary>
    public string Id { get; init; } = "";

    /// <summary>The queue.</summary>
    public string Queue { get; init; } = "";

    /// <summary>The message's status: <c>pending</c>, or another for a duplicate.</summary>
    public string Status { get; init; } = "";

    /// <summary>When it was published.</summary>
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>When it becomes available to workers.</summary>
    public DateTimeOffset AvailableAt { get; init; }

    /// <summary>True when an open message with the same dedup key was returned instead of a new one.</summary>
    public bool Duplicate { get; init; }
}

/// <summary>What the server did with a failed attempt, as <see cref="FailResult.Outcome"/> says it.</summary>
public static class FailOutcome
{
    /// <summary>The message is pending again from <see cref="FailResult.AvailableAt"/>.</summary>
    public const string Retrying = "retrying";

    /// <summary>The message is in the dead-letter set (status <c>failed</c>).</summary>
    public const string DeadLettered = "dead_lettered";

    /// <summary>The message is deleted (the queue's <c>on_exhausted</c> is <c>discard</c>).</summary>
    public const string Discarded = "discarded";
}

/// <summary>What the server did with a failed attempt. For logging: a client never acts on it.</summary>
public sealed class FailResult
{
    /// <summary><see cref="FailOutcome.Retrying"/>, <see cref="FailOutcome.DeadLettered"/> or <see cref="FailOutcome.Discarded"/>.</summary>
    public string Outcome { get; init; } = "";

    /// <summary>When a retrying message comes back; null otherwise.</summary>
    public DateTimeOffset? AvailableAt { get; init; }

    /// <summary>The attempt that failed.</summary>
    public int Attempt { get; init; }

    /// <summary>The attempts the queue allows this message.</summary>
    public int MaxAttempts { get; init; }
}

/// <summary>What became of one lease of an ack batch.</summary>
public static class AckOutcome
{
    /// <summary>The message is completed (also when this lease was acked before).</summary>
    public const string Acked = "acked";

    /// <summary>The receipt is not the current lease; stop reporting on it.</summary>
    public const string LeaseLost = "lease_lost";

    /// <summary>No such message.</summary>
    public const string NotFound = "not_found";

    /// <summary>The key may not use the message's queue.</summary>
    public const string Forbidden = "forbidden";
}

/// <summary>A queue's counts, for monitoring. Not part of the protocol.</summary>
public sealed class QueueStats
{
    /// <summary>Pending and available now.</summary>
    public int Ready { get; init; }

    /// <summary>Pending, available later (backoff or delay).</summary>
    public int Delayed { get; init; }

    /// <summary>Leased to a worker.</summary>
    public int Processing { get; init; }

    /// <summary>Completed and not yet removed by retention.</summary>
    public int Completed { get; init; }

    /// <summary>The dead-letter set.</summary>
    public int Failed { get; init; }

    /// <summary>How long the oldest ready message has been waiting.</summary>
    public int OldestReadySeconds { get; init; }

    /// <summary>How long the oldest leased message has been running.</summary>
    public int OldestProcessingSeconds { get; init; }

    /// <summary>Workers that claimed from the queue in the last minute or hold one of its messages.</summary>
    public int Workers { get; init; }
}

/// <summary>How the protocol's own JSON is written and read.</summary>
internal static class Wire
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        // Non-ASCII text goes out as UTF-8 rather than \u escapes: shorter, and readable
        // as it was written. Nothing here ends up in HTML.
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    public static readonly JsonSerializerOptions BodyDefaults = new(JsonSerializerDefaults.Web);

    public static string Format(DateTimeOffset time) =>
        time.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.FFFFFFF'Z'", CultureInfo.InvariantCulture);
}
