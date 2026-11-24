package dev.lessormq;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * The receipt is not the current lease of the message. The lease expired, was released,
 * or the message was claimed again. Stop reporting on it: the server has already made it
 * available again.
 */
public final class LeaseLostException extends ApiException {
    private static final long serialVersionUID = 1L;

    public LeaseLostException(int status, String code, String detail, List<FieldError> errors,
            @Nullable Integer retryAfterSeconds) {
        super(status, code, detail, errors, retryAfterSeconds);
    }
}
