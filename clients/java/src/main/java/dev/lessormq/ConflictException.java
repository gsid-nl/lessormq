package dev.lessormq;

import java.util.List;
import org.jspecify.annotations.Nullable;

/** The message is in a state that does not allow this ({@code invalid_state}). */
public final class ConflictException extends ApiException {
    private static final long serialVersionUID = 1L;

    public ConflictException(int status, String code, String detail, List<FieldError> errors,
            @Nullable Integer retryAfterSeconds) {
        super(status, code, detail, errors, retryAfterSeconds);
    }
}
