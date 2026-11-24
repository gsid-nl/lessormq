package dev.lessormq;

import java.util.List;
import org.jspecify.annotations.Nullable;

/** The key lacks the scope or the queue, or its tenant is suspended (code {@code tenant_suspended}). */
public final class ForbiddenException extends ApiException {
    private static final long serialVersionUID = 1L;

    public ForbiddenException(int status, String code, String detail, List<FieldError> errors,
            @Nullable Integer retryAfterSeconds) {
        super(status, code, detail, errors, retryAfterSeconds);
    }
}
