package dev.lessormq;

import java.util.List;
import org.jspecify.annotations.Nullable;

/** Body and headers exceed the queue's {@code max_message_bytes}. */
public final class PayloadTooLargeException extends ApiException {
    private static final long serialVersionUID = 1L;

    public PayloadTooLargeException(int status, String code, String detail, List<FieldError> errors,
            @Nullable Integer retryAfterSeconds) {
        super(status, code, detail, errors, retryAfterSeconds);
    }
}
