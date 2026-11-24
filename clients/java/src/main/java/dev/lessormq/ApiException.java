package dev.lessormq;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * An error answer of the server (RFC 9457 problem details). {@link #code()} is the stable
 * problem code to branch on; the subclasses cover the ones a client acts on, and anything
 * else (such as {@code quota_exceeded}) is an ApiException itself.
 */
public class ApiException extends LessorMQException {
    private static final long serialVersionUID = 1L;

    private final int status;
    private final String code;
    private final String detail;
    private final List<FieldError> errors;
    private final @Nullable Integer retryAfterSeconds;

    public ApiException(int status, String code, String detail, List<FieldError> errors,
            @Nullable Integer retryAfterSeconds) {
        super(detail.isEmpty() ? "LessorMQ error " + status + " (" + code + ")" : detail);
        this.status = status;
        this.code = code;
        this.detail = detail;
        this.errors = List.copyOf(errors);
        this.retryAfterSeconds = retryAfterSeconds;
    }

    /** The exception for a problem code: a subclass when there is one for it. */
    public static ApiException of(int status, String code, String detail, List<FieldError> errors,
            @Nullable Integer retryAfterSeconds) {
        return switch (code) {
            case "lease_lost" -> new LeaseLostException(status, code, detail, errors, retryAfterSeconds);
            case "validation_failed" -> new ValidationException(status, code, detail, errors, retryAfterSeconds);
            case "unauthorized" -> new UnauthorizedException(status, code, detail, errors, retryAfterSeconds);
            case "forbidden", "tenant_suspended" ->
                    new ForbiddenException(status, code, detail, errors, retryAfterSeconds);
            case "not_found" -> new NotFoundException(status, code, detail, errors, retryAfterSeconds);
            case "invalid_state" -> new ConflictException(status, code, detail, errors, retryAfterSeconds);
            case "payload_too_large" -> new PayloadTooLargeException(status, code, detail, errors, retryAfterSeconds);
            case "rate_limited" -> new RateLimitedException(status, code, detail, errors, retryAfterSeconds);
            default -> status >= 500
                    ? new ServerException(status, code, detail, errors, retryAfterSeconds)
                    : new ApiException(status, code, detail, errors, retryAfterSeconds);
        };
    }

    /** The code for an answer whose body names none, such as a proxy's error page. */
    static String codeFor(int status) {
        if (status == 401) {
            return "unauthorized";
        }
        if (status == 403) {
            return "forbidden";
        }
        if (status == 404) {
            return "not_found";
        }
        if (status == 413) {
            return "payload_too_large";
        }
        if (status == 429) {
            return "rate_limited";
        }
        return status >= 500 ? "internal" : "validation_failed";
    }

    /** The HTTP status. */
    public int status() {
        return status;
    }

    /** The stable problem code, such as {@code lease_lost}. */
    public String code() {
        return code;
    }

    /** The server's explanation; empty when it gave none. */
    public String detail() {
        return detail;
    }

    /** The invalid fields of a {@code validation_failed}; empty otherwise. */
    public List<FieldError> errors() {
        return errors;
    }

    /** The Retry-After of the answer in seconds, or null when it had none. */
    public @Nullable Integer retryAfterSeconds() {
        return retryAfterSeconds;
    }
}
