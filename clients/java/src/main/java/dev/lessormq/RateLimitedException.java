package dev.lessormq;

import java.util.List;
import org.jspecify.annotations.Nullable;

/** The tenant exceeded a quota or rate limit, after the configured retries. */
public final class RateLimitedException extends ApiException {
    private static final long serialVersionUID = 1L;

    public RateLimitedException(int status, String code, String detail, List<FieldError> errors,
            @Nullable Integer retryAfterSeconds) {
        super(status, code, detail, errors, retryAfterSeconds);
    }
}
