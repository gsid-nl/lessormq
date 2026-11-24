using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace LessorMQ;

/// <summary>
/// Speaks the LessorMQ protocol: publish, claim, extend, ack, fail, release. Safe for
/// concurrent use; make one per server and key, and keep it.
/// <code>
/// using var client = new LessorMQClient("https://mq.example.com", apiKey);
/// await client.PublishAsync("mail", new { to = "ada@example.com" });
/// </code>
/// The client never decides what happens to a message: no retry of a failed job, no
/// delay, no dead-lettering. It reports, and the queue decides. It does retry its own
/// HTTP calls when that is safe: network errors, 5xx and 429, for every call that can be
/// repeated (all of them, given the automatic dedup key on publish).
/// </summary>
public sealed class LessorMQClient : IDisposable
{
    /// <summary>The User-Agent sent unless <see cref="LessorMQClientOptions.UserAgent"/> says otherwise.</summary>
    public const string DefaultUserAgent = "lessormq-dotnet/1";

    private const int MaxErrorMessage = 65535;
    private const int MaxErrorType = 255;
    private const int MaxDetails = 8000;

    private readonly string baseUrl;
    private readonly string apiKey;
    private readonly HttpClient http;
    private readonly bool ownsHttp;
    private readonly int retries;
    private readonly TimeSpan retryDelay;
    private readonly TimeSpan timeout;
    private readonly bool autoDedupKey;
    private readonly string userAgent;
    private readonly JsonSerializerOptions bodyOptions;
    private readonly Action<LogEvent>? log;

    /// <summary>A client for the server at <paramref name="url"/>, acting with <paramref name="apiKey"/>.</summary>
    public LessorMQClient(string url, string apiKey)
        : this(new LessorMQClientOptions { Url = url, ApiKey = apiKey })
    {
    }

    /// <summary>
    /// A client with all its options. Without <paramref name="httpClient"/> it makes and
    /// owns one; yours (proxies, TLS, handlers) is not disposed with the client, and its
    /// <see cref="HttpClient.Timeout"/> should be infinite or above 30 seconds: the client
    /// sets a deadline per request itself.
    /// </summary>
    public LessorMQClient(LessorMQClientOptions options, HttpClient? httpClient = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!Uri.TryCreate(options.Url, UriKind.Absolute, out var url) || (url.Scheme != Uri.UriSchemeHttp && url.Scheme != Uri.UriSchemeHttps))
        {
            throw new ArgumentException("LessorMQ: Url must be an absolute http or https URL, such as https://mq.example.com.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.ApiKey))
        {
            throw new ArgumentException("LessorMQ: ApiKey is required.", nameof(options));
        }

        baseUrl = options.Url.TrimEnd('/');
        apiKey = options.ApiKey;
        retries = Math.Max(0, options.Retries);
        retryDelay = options.RetryDelay;
        timeout = options.Timeout;
        autoDedupKey = options.AutoDedupKey;
        userAgent = options.UserAgent;
        bodyOptions = options.SerializerOptions ?? Wire.BodyDefaults;
        log = options.Log;

        if (httpClient is null)
        {
            // Connections are renewed now and then, so a long-lived client follows DNS changes.
            http = new HttpClient(new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(5) })
            {
                Timeout = System.Threading.Timeout.InfiniteTimeSpan,
            };
            ownsHttp = true;
        }
        else
        {
            http = httpClient;
        }
    }

    /// <summary>Publishes a message. The body is any value System.Text.Json writes (a <see cref="JsonElement"/> or <see cref="JsonNode"/> as it is).</summary>
    /// <exception cref="ValidationFailedException">A value is out of range.</exception>
    /// <exception cref="PayloadTooLargeException">Body and headers exceed the queue's <c>max_message_bytes</c>.</exception>
    /// <exception cref="RateLimitedException">Still rate limited after the retries.</exception>
    public async Task<Published> PublishAsync<T>(string queue, T body, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new PublishOptions();
        var dedupKey = options.DedupKey;
        if (string.IsNullOrEmpty(dedupKey) && autoDedupKey)
        {
            dedupKey = "lmq-auto-" + Convert.ToHexString(RandomNumberGenerator.GetBytes(12)).ToLowerInvariant();
        }

        var payload = new JsonObject { ["body"] = JsonSerializer.SerializeToNode(body, bodyOptions) };
        if (options.Headers is { Count: > 0 } headers)
        {
            var map = new JsonObject();
            foreach (var (name, value) in headers)
            {
                map[name] = value;
            }

            payload["headers"] = map;
        }

        if (!string.IsNullOrEmpty(dedupKey))
        {
            payload["dedup_key"] = dedupKey;
        }

        if (options.DelaySeconds > 0)
        {
            payload["delay_seconds"] = options.DelaySeconds;
        }

        if (options.AvailableAt is { } availableAt)
        {
            payload["available_at"] = Wire.Format(availableAt);
        }

        if (options.MaxAttempts > 0)
        {
            payload["max_attempts"] = options.MaxAttempts;
        }

        // Without a dedup key a repeated publish could create a second message.
        var call = new Call(HttpMethod.Post, QueuePath(queue, "messages"), payload, Repeatable: !string.IsNullOrEmpty(dedupKey));
        return Decode<Published>(await SendAsync(call, cancellationToken).ConfigureAwait(false), call);
    }

    /// <summary>
    /// Leases the next message of a queue, waiting up to <see cref="ClaimOptions.WaitSeconds"/>
    /// for one. Null when nothing became available, or the queue is paused.
    /// </summary>
    /// <remarks>
    /// Cancelling while a claim waits can leave a message leased on the server, which then
    /// runs out and counts as an attempt; the worker therefore lets its claims finish.
    /// </remarks>
    public async Task<Lease?> ClaimAsync(string queue, ClaimOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new ClaimOptions();
        // A claim whose answer got lost leaves a lease that simply expires and counts as an
        // attempt, so claims are retried like the rest.
        var call = new Call(HttpMethod.Post, QueuePath(queue, "claims"), ClaimPayload(options), Repeatable: true, Wait(options));
        var answer = await SendAsync(call, cancellationToken).ConfigureAwait(false);
        return answer.Status == 204 ? null : ToLease(Decode<WireLease>(answer, call));
    }

    /// <summary>
    /// Leases up to <paramref name="maxMessages"/> (1 to 100) ready messages of a queue at
    /// once, oldest first, each with a receipt of its own. Waits up to
    /// <see cref="ClaimOptions.WaitSeconds"/> only while there is none; empty when nothing
    /// became available. Report each lease as usual, or ack them together with
    /// <see cref="AckBatchAsync"/>.
    /// </summary>
    public async Task<IReadOnlyList<Lease>> ClaimBatchAsync(
        string queue,
        int maxMessages,
        ClaimOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new ClaimOptions();
        var payload = ClaimPayload(options);
        payload["max_messages"] = maxMessages;
        var call = new Call(HttpMethod.Post, QueuePath(queue, "claims"), payload, Repeatable: true, Wait(options));
        var answer = await SendAsync(call, cancellationToken).ConfigureAwait(false);
        if (answer.Status == 204)
        {
            return [];
        }

        return Decode<WireLeaseBatch>(answer, call).Items.Select(ToLease).ToList();
    }

    /// <summary>
    /// Reports success for up to 100 leases in one request. The outcomes
    /// (<see cref="AckOutcome"/>) are in the order of <paramref name="leases"/>; one that
    /// cannot be acked does not hold up the others.
    /// </summary>
    public async Task<IReadOnlyList<string>> AckBatchAsync(IReadOnlyList<Lease> leases, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(leases);
        var items = new JsonArray();
        foreach (var lease in leases)
        {
            items.Add(new JsonObject { ["message"] = lease.Message.Id, ["receipt"] = lease.Receipt });
        }

        var call = new Call(HttpMethod.Post, "/v1/acks", new JsonObject { ["leases"] = items }, Repeatable: true);
        var answered = Decode<WireAckBatch>(await SendAsync(call, cancellationToken).ConfigureAwait(false), call).Items;
        var outcomes = new string[leases.Count];
        for (var i = 0; i < outcomes.Length; i++)
        {
            outcomes[i] = i < answered.Count ? answered[i].Outcome : AckOutcome.NotFound;
        }

        return outcomes;
    }

    /// <summary>Moves the end of the lease to now plus <paramref name="leaseSeconds"/> (0: the queue's lease), and updates <see cref="Lease.Until"/>.</summary>
    /// <exception cref="LeaseLostException">The lease already ended.</exception>
    public async Task ExtendAsync(Lease lease, int leaseSeconds = 0, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        var payload = new JsonObject { ["receipt"] = lease.Receipt };
        if (leaseSeconds > 0)
        {
            payload["lease_seconds"] = leaseSeconds;
        }

        var call = new Call(HttpMethod.Post, MessagePath(lease, "extend"), payload, Repeatable: true);
        lease.ExtendedTo(Decode<WireExtended>(await SendAsync(call, cancellationToken).ConfigureAwait(false), call).LeaseUntil);
    }

    /// <summary>Reports success.</summary>
    /// <exception cref="LeaseLostException">The lease already ended.</exception>
    public async Task AckAsync(Lease lease, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        var payload = new JsonObject { ["receipt"] = lease.Receipt };
        await SendAsync(new Call(HttpMethod.Post, MessagePath(lease, "ack"), payload, Repeatable: true), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reports failure with an exception: its message, its type, and whether it is
    /// permanent (<see cref="PermanentFailureException"/>). The server decides what
    /// follows and says so in the result, which is for logging only.
    /// </summary>
    /// <exception cref="LeaseLostException">The lease already ended.</exception>
    public Task<FailResult> FailAsync(Lease lease, Exception exception, FailOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(exception);
        var details = options?.Details is { } given ? JsonSerializer.SerializeToNode(given, bodyOptions) : DetailsOf(exception);
        return SendFailAsync(
            lease,
            exception.Message,
            options?.Type ?? TypeOf(exception),
            details,
            options?.Permanent ?? PermanentFailureException.IsPermanent(exception),
            cancellationToken);
    }

    /// <summary>Reports failure with a message of your own; see the overload that takes an exception.</summary>
    /// <exception cref="LeaseLostException">The lease already ended.</exception>
    public Task<FailResult> FailAsync(Lease lease, string message, FailOptions? options = null, CancellationToken cancellationToken = default)
    {
        var details = options?.Details is { } given ? JsonSerializer.SerializeToNode(given, bodyOptions) : null;
        return SendFailAsync(lease, message, options?.Type, details, options?.Permanent ?? false, cancellationToken);
    }

    /// <summary>Gives the message back without an outcome; the attempt does not count. It is available again after <paramref name="delaySeconds"/>.</summary>
    /// <exception cref="LeaseLostException">The lease already ended.</exception>
    public async Task ReleaseAsync(Lease lease, int delaySeconds = 0, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        var payload = new JsonObject { ["receipt"] = lease.Receipt };
        if (delaySeconds > 0)
        {
            payload["delay_seconds"] = delaySeconds;
        }

        await SendAsync(new Call(HttpMethod.Post, MessagePath(lease, "release"), payload, Repeatable: true), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>A queue's counts. Needs the consume or admin scope. Not part of the protocol; for monitoring.</summary>
    public async Task<QueueStats> GetStatsAsync(string queue, CancellationToken cancellationToken = default)
    {
        var call = new Call(HttpMethod.Get, "/v1/queues/" + Uri.EscapeDataString(queue), null, Repeatable: true);
        return Decode<WireQueue>(await SendAsync(call, cancellationToken).ConfigureAwait(false), call).Stats ?? new QueueStats();
    }

    /// <summary>Disposes the HttpClient the client made; one you passed in stays yours.</summary>
    public void Dispose()
    {
        if (ownsHttp)
        {
            http.Dispose();
        }
    }

    /// <summary>Never shows the key.</summary>
    public override string ToString() => $"LessorMQClient({baseUrl})";

    internal void Log(LogSeverity severity, Exception? exception, string template, params object?[] arguments)
    {
        if (log is null)
        {
            return;
        }

        try
        {
            log(new LogEvent(severity, template, arguments, exception));
        }
        catch (Exception)
        {
            // A logger that fails must not take a message or a worker down with it.
        }
    }

    // The error type for the report: the exception's type, looking through the permanent
    // marker, whose inner exception says what went wrong.
    internal static string TypeOf(Exception exception)
    {
        if (exception is PermanentFailureException { InnerException: { } inner })
        {
            exception = inner;
        }

        var type = exception.GetType();
        return type.IsGenericType ? $"{type.Namespace}.{type.Name}" : type.FullName ?? type.Name;
    }

    private static JsonNode? DetailsOf(Exception exception)
    {
        // The whole exception as .NET writes it: type, message, inner exceptions, stack.
        var text = exception.ToString();
        return new JsonObject { ["exception"] = Truncate(text, MaxDetails) };
    }

    private async Task<FailResult> SendFailAsync(
        Lease lease,
        string message,
        string? type,
        JsonNode? details,
        bool permanent,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        var report = new JsonObject
        {
            ["message"] = string.IsNullOrEmpty(message) ? "(no message)" : Truncate(message, MaxErrorMessage),
        };
        if (!string.IsNullOrEmpty(type))
        {
            report["type"] = Truncate(type, MaxErrorType);
        }

        if (details is not null)
        {
            report["details"] = details;
        }

        var payload = new JsonObject { ["receipt"] = lease.Receipt, ["error"] = report, ["permanent"] = permanent };
        var call = new Call(HttpMethod.Post, MessagePath(lease, "fail"), payload, Repeatable: true);
        return Decode<FailResult>(await SendAsync(call, cancellationToken).ConfigureAwait(false), call);
    }

    private async Task<Answer> SendAsync(Call call, CancellationToken cancellationToken)
    {
        var content = call.Payload is null ? null : Encoding.UTF8.GetBytes(call.Payload.ToJsonString(Wire.Options));
        for (var attempt = 1; ; attempt++)
        {
            Answer answer;
            try
            {
                answer = await DoAsync(call, content, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (!cancellationToken.IsCancellationRequested && e is HttpRequestException or TimeoutException or IOException)
            {
                if (call.Repeatable && attempt <= retries)
                {
                    await PauseAsync(call, attempt, null, e.Message, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                throw new TransportException(call.Method.Method, call.Path, e);
            }

            if (answer.Status < 400)
            {
                return answer;
            }

            var error = ApiException.From(answer.Status, answer.Body, answer.RetryAfter);
            // A 429 was not processed at all, so it is always safe to send again.
            var retry = error is RateLimitedException || (call.Repeatable && answer.Status >= 500);
            if (retry && attempt <= retries)
            {
                await PauseAsync(call, attempt, answer.RetryAfter, error.Message, cancellationToken).ConfigureAwait(false);
                continue;
            }

            throw error;
        }
    }

    private async Task<Answer> DoAsync(Call call, byte[]? content, CancellationToken cancellationToken)
    {
        var limit = timeout + call.Extra;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(limit);

        using var request = new HttpRequestMessage(call.Method, baseUrl + call.Path);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
        request.Headers.TryAddWithoutValidation("Accept", "application/json");
        request.Headers.TryAddWithoutValidation("User-Agent", userAgent);
        if (content is not null)
        {
            request.Content = new ByteArrayContent(content);
            request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        }

        try
        {
            using var response = await http.SendAsync(request, HttpCompletionOption.ResponseContentRead, deadline.Token).ConfigureAwait(false);
            var body = await response.Content.ReadAsByteArrayAsync(deadline.Token).ConfigureAwait(false);
            return new Answer((int)response.StatusCode, body, response.Headers.RetryAfter?.Delta);
        }
        catch (OperationCanceledException e) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"no answer within {limit.TotalSeconds:0.###} s", e);
        }
    }

    private async Task PauseAsync(Call call, int attempt, TimeSpan? retryAfter, string reason, CancellationToken cancellationToken)
    {
        var delay = retryAfter ?? retryDelay * attempt;
        Log(
            LogSeverity.Warning,
            null,
            "LessorMQ request {Method} {Path} failed, retrying in {Delay} (attempt {Attempt}): {Reason}",
            call.Method.Method,
            call.Path,
            delay,
            attempt,
            reason);
        if (delay > TimeSpan.Zero)
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
    }

    private static T Decode<T>(Answer answer, Call call)
        where T : class
    {
        try
        {
            return JsonSerializer.Deserialize<T>(answer.Body, Wire.Options) ?? throw new JsonException("the answer is empty");
        }
        catch (JsonException e)
        {
            throw new LessorMQException($"LessorMQ: could not read the answer to {call.Method.Method} {call.Path}: {e.Message}", e);
        }
    }

    private Lease ToLease(WireLease wire)
    {
        wire.Message.SerializerOptions = bodyOptions;
        return new Lease(wire.Receipt, wire.LeaseUntil, wire.Message);
    }

    private static JsonObject ClaimPayload(ClaimOptions options)
    {
        var payload = new JsonObject();
        if (!string.IsNullOrEmpty(options.Consumer))
        {
            payload["consumer"] = options.Consumer;
        }

        if (!string.IsNullOrEmpty(options.Route))
        {
            payload["route"] = options.Route;
        }

        if (options.WaitSeconds > 0)
        {
            payload["wait_seconds"] = options.WaitSeconds;
        }

        if (options.LeaseSeconds > 0)
        {
            payload["lease_seconds"] = options.LeaseSeconds;
        }

        return payload;
    }

    private static TimeSpan Wait(ClaimOptions options) => TimeSpan.FromSeconds(Math.Max(0, options.WaitSeconds));

    private static string QueuePath(string queue, string what)
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);
        return "/v1/queues/" + Uri.EscapeDataString(queue) + "/" + what;
    }

    private static string MessagePath(Lease lease, string action) =>
        "/v1/messages/" + Uri.EscapeDataString(lease.Message.Id) + "/" + action;

    // Cuts at a character count without splitting a surrogate pair.
    private static string Truncate(string text, int max)
    {
        if (text.Length <= max)
        {
            return text;
        }

        var cut = char.IsHighSurrogate(text[max - 1]) ? max - 1 : max;
        return text[..cut];
    }

    private sealed record Call(HttpMethod Method, string Path, JsonNode? Payload, bool Repeatable, TimeSpan Extra = default);

    private readonly record struct Answer(int Status, byte[] Body, TimeSpan? RetryAfter);

    private sealed class WireLease
    {
        public string Receipt { get; set; } = "";

        public DateTimeOffset LeaseUntil { get; set; }

        public Message Message { get; set; } = new();
    }

    private sealed class WireLeaseBatch
    {
        public List<WireLease> Items { get; set; } = [];
    }

    private sealed class WireAckBatch
    {
        public List<WireAckItem> Items { get; set; } = [];
    }

    private sealed class WireAckItem
    {
        public string Outcome { get; set; } = "";
    }

    private sealed class WireExtended
    {
        public DateTimeOffset LeaseUntil { get; set; }
    }

    private sealed class WireQueue
    {
        public QueueStats? Stats { get; set; }
    }
}
