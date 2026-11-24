package dev.lessormq;

import java.util.List;
import org.jspecify.annotations.Nullable;

/** The server failed (5xx), after the configured retries. */
public final class ServerException extends ApiException {
    private static final long serialVersionUID = 1L;

    public ServerException(int status, String code, String detail, List<FieldError> errors,
            @Nullable Integer retryAfterSeconds) {
        super(status, code, detail, errors, retryAfterSeconds);
    }
}
