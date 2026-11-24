package dev.lessormq;

import java.util.List;
import org.jspecify.annotations.Nullable;

/** No such queue or message for this tenant. */
public final class NotFoundException extends ApiException {
    private static final long serialVersionUID = 1L;

    public NotFoundException(int status, String code, String detail, List<FieldError> errors,
            @Nullable Integer retryAfterSeconds) {
        super(status, code, detail, errors, retryAfterSeconds);
    }
}
