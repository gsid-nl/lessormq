package dev.lessormq;

import java.util.List;
import org.jspecify.annotations.Nullable;

/** The API key is missing, unknown or revoked. */
public final class UnauthorizedException extends ApiException {
    private static final long serialVersionUID = 1L;

    public UnauthorizedException(int status, String code, String detail, List<FieldError> errors,
            @Nullable Integer retryAfterSeconds) {
        super(status, code, detail, errors, retryAfterSeconds);
    }
}
