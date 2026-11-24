using System.Text.Json;
using System.Text.Json.Serialization;

namespace LessorMQ;

/// <summary>Everything this library throws.</summary>
public class LessorMQException : Exception
{
    /// <summary>An error of this library.</summary>
    public LessorMQException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}

/// <summary>The server could not be reached, or did not answer in time, after the retries.</summary>
public sealed class TransportException : LessorMQException
{
    /// <summary>A request that never got an answer.</summary>
    public TransportException(string method, string path, Exception innerException)
        : base($"LessorMQ: {method} {path} failed: {innerException.Message}", innerException)
    {
        Method = method;
        Path = path;
    }

    /// <summary>The HTTP method of the request.</summary>
    public string Method { get; }

    /// <summary>The path of the request, such as /v1/messages/{id}/ack.</summary>
    public string Path { get; }
}

/// <summary>One invalid field of a rejected request.</summary>
/// <param name="Field">The field, such as <c>max_messages</c> or <c>leases[0]</c>.</param>
/// <param name="Message">What is wrong with it.</param>
public sealed record FieldError(string Field, string Message);

/// <summary>
/// An error answer of the server (RFC 9457 problem details). <see cref="Code"/> is the
/// stable problem code to branch on; the subclasses cover the ones a client acts on.
/// </summary>
public class ApiException : LessorMQException
{
    /// <summary>An error answer with a problem code.</summary>
    public ApiException(int status, string code, string? detail = null, IReadOnlyList<FieldError>? errors = null)
        : base(string.IsNullOrEmpty(detail) ? $"LessorMQ: error {status} ({code})" : "LessorMQ: " + detail)
    {
        Status = status;
        Code = code;
        Detail = detail;
        Errors = errors ?? [];
    }

    /// <summary>The HTTP status.</summary>
    public int Status { get; }

    /// <summary>The problem code: <c>lease_lost</c>, <c>validation_failed</c>, <c>quota_exceeded</c>, ...</summary>
    public string Code { get; }

    /// <summary>The server's explanation, when it gave one.</summary>
    public string? Detail { get; }

    /// <summary>The invalid fields of a <c>validation_failed</c>; empty otherwise.</summary>
    public IReadOnlyList<FieldError> Errors { get; }

    internal static ApiException From(int status, byte[] body, TimeSpan? retryAfter)
    {
        Problem? problem = null;
        try
        {
            problem = JsonSerializer.Deserialize<Problem>(body, Wire.Options);
        }
        catch (JsonException)
        {
            // A proxy's error page, say, does not decode: the status says enough.
        }

        var code = string.IsNullOrEmpty(problem?.Code) ? CodeFor(status) : problem.Code;
        var detail = problem?.Detail;
        var errors = problem?.Errors;
        return code switch
        {
            "lease_lost" => new LeaseLostException(status, code, detail, errors),
            "validation_failed" => new ValidationFailedException(status, code, detail, errors),
            "unauthorized" => new UnauthorizedException(status, code, detail, errors),
            "forbidden" or "tenant_suspended" => new ForbiddenException(status, code, detail, errors),
            "not_found" => new NotFoundException(status, code, detail, errors),
            "invalid_state" => new InvalidStateException(status, code, detail, errors),
            "payload_too_large" => new PayloadTooLargeException(status, code, detail, errors),
            "rate_limited" => new RateLimitedException(status, code, detail, errors, retryAfter),
            _ when status >= 500 => new ServerException(status, code, detail, errors),
            _ => new ApiException(status, code, detail, errors),
        };
    }

    private static string CodeFor(int status) => status switch
    {
        401 => "unauthorized",
        403 => "forbidden",
        404 => "not_found",
        413 => "payload_too_large",
        429 => "rate_limited",
        >= 500 => "internal",
        _ => "validation_failed",
    };

    private sealed class Problem
    {
        [JsonPropertyName("code")]
        public string? Code { get; set; }

        [JsonPropertyName("detail")]
        public string? Detail { get; set; }

        [JsonPropertyName("errors")]
        public List<FieldError>? Errors { get; set; }
    }
}

/// <summary>
/// The receipt is not the current lease of the message: it expired, was released, or the
/// message was claimed again. Stop reporting on it; the server has already made it
/// available again.
/// </summary>
public sealed class LeaseLostException(int status, string code, string? detail = null, IReadOnlyList<FieldError>? errors = null)
    : ApiException(status, code, detail, errors);

/// <summary>The request was rejected as invalid; <see cref="ApiException.Errors"/> names each field.</summary>
public sealed class ValidationFailedException(int status, string code, string? detail = null, IReadOnlyList<FieldError>? errors = null)
    : ApiException(status, code, detail, errors);

/// <summary>The API key is missing, unknown or revoked.</summary>
public sealed class UnauthorizedException(int status, string code, string? detail = null, IReadOnlyList<FieldError>? errors = null)
    : ApiException(status, code, detail, errors);

/// <summary>The key lacks the scope or the queue (<c>forbidden</c>), or its tenant is suspended (<c>tenant_suspended</c>).</summary>
public sealed class ForbiddenException(int status, string code, string? detail = null, IReadOnlyList<FieldError>? errors = null)
    : ApiException(status, code, detail, errors);

/// <summary>No such queue or message for this tenant.</summary>
public sealed class NotFoundException(int status, string code, string? detail = null, IReadOnlyList<FieldError>? errors = null)
    : ApiException(status, code, detail, errors);

/// <summary>The message is in a state that does not allow this (<c>invalid_state</c>).</summary>
public sealed class InvalidStateException(int status, string code, string? detail = null, IReadOnlyList<FieldError>? errors = null)
    : ApiException(status, code, detail, errors);

/// <summary>Body and headers exceed the queue's <c>max_message_bytes</c>.</summary>
public sealed class PayloadTooLargeException(int status, string code, string? detail = null, IReadOnlyList<FieldError>? errors = null)
    : ApiException(status, code, detail, errors);

/// <summary>The tenant exceeded a rate limit or its open-message quota, after the retries.</summary>
public sealed class RateLimitedException(
    int status,
    string code,
    string? detail = null,
    IReadOnlyList<FieldError>? errors = null,
    TimeSpan? retryAfter = null)
    : ApiException(status, code, detail, errors)
{
    /// <summary>The server's <c>Retry-After</c>, when it sent one.</summary>
    public TimeSpan? RetryAfter { get; } = retryAfter;
}

/// <summary>The server failed (5xx), after the retries.</summary>
public sealed class ServerException(int status, string code, string? detail = null, IReadOnlyList<FieldError>? errors = null)
    : ApiException(status, code, detail, errors);

/// <summary>
/// Throw from a handler when the message itself cannot be processed: it cannot be
/// decoded, it fails validation, so another attempt would only fail again. It is
/// reported with <c>permanent: true</c>, a fact for the server: the queue's
/// <c>on_permanent</c> decides what it means. Wrap the exception that says why, and its
/// type is reported:
/// <code>throw new PermanentFailureException(new FormatException("no recipient"));</code>
/// </summary>
public sealed class PermanentFailureException : Exception
{
    /// <summary>A permanent failure without further explanation.</summary>
    public PermanentFailureException()
        : base("permanent failure")
    {
    }

    /// <summary>A permanent failure, said in words.</summary>
    public PermanentFailureException(string message)
        : base(message)
    {
    }

    /// <summary>A permanent failure, said in words, caused by another exception.</summary>
    public PermanentFailureException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>A permanent failure that is the other exception: its message and type are reported.</summary>
    public PermanentFailureException(Exception innerException)
        : base(innerException.Message, innerException)
    {
    }

    /// <summary>
    /// Whether an exception, or one it wraps (<see cref="Exception.InnerException"/>), is
    /// a <see cref="PermanentFailureException"/>.
    /// </summary>
    public static bool IsPermanent(Exception? exception)
    {
        for (var e = exception; e is not null; e = e.InnerException)
        {
            if (e is PermanentFailureException)
            {
                return true;
            }
        }

        return false;
    }
}
