package dev.lessormq;

/**
 * Marks an exception of your own as permanent: the message itself cannot be processed (it
 * cannot be decoded, it fails validation), so another attempt would only fail again.
 *
 * <pre>{@code
 * class NoRecipient extends IllegalArgumentException implements Permanent { ... }
 * }</pre>
 *
 * <p>A fact for the server, not an instruction: the queue's {@code on_permanent} decides
 * what it means. {@link PermanentFailure} is a ready-made one.
 */
public interface Permanent {
    /** Whether this exception is permanent after all; true unless overridden. */
    default boolean isPermanent() {
        return true;
    }
}
