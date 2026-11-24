package dev.lessormq;

import org.jspecify.annotations.Nullable;

/**
 * Throw from a handler when the message itself cannot be processed: it cannot be decoded,
 * it fails validation, so retrying it would only fail again.
 *
 * <pre>{@code
 * throw new PermanentFailure("no recipient");
 * throw new PermanentFailure(e);   // reported with the type and text of e
 * }</pre>
 *
 * <p>A fact for the server; the queue's {@code on_permanent} decides what it means. Your
 * own exceptions can say the same by implementing {@link Permanent}.
 */
public class PermanentFailure extends RuntimeException implements Permanent {
    private static final long serialVersionUID = 1L;

    public PermanentFailure(String message) {
        super(message);
    }

    public PermanentFailure(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    /** Wraps an exception, keeping its text; the report names the cause's type. */
    public PermanentFailure(Throwable cause) {
        super(cause.getMessage(), cause);
    }

    /**
     * Whether an exception, or one in its chain of causes, is permanent: a {@link Permanent}
     * whose {@link Permanent#isPermanent()} says so.
     */
    public static boolean isPermanent(@Nullable Throwable error) {
        // Bounded: a chain of causes can loop.
        Throwable e = error;
        for (int depth = 0; e != null && depth < 32; depth++, e = e.getCause()) {
            if (e instanceof Permanent permanent && permanent.isPermanent()) {
                return true;
            }
        }
        return false;
    }
}
