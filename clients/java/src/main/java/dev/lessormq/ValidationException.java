package dev.lessormq;

import java.util.List;
import org.jspecify.annotations.Nullable;

/** The request was rejected as invalid; {@link #errors()} names each field. */
public final class ValidationException extends ApiException {
    private static final long serialVersionUID = 1L;

    public ValidationException(int status, String code, String detail, List<FieldError> errors,
            @Nullable Integer retryAfterSeconds) {
        super(status, code, detail, errors, retryAfterSeconds);
    }
}
