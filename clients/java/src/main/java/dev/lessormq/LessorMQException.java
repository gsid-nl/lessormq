package dev.lessormq;

import org.jspecify.annotations.Nullable;

/** Everything this library throws. Unchecked, so Kotlin and lambdas need no ceremony. */
public class LessorMQException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public LessorMQException(String message) {
        super(message);
    }

    public LessorMQException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
