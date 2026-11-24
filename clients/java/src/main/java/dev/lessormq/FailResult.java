package dev.lessormq;

import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * What the server did with a failed attempt. For logging: a client never acts on it.
 *
 * @param outcome {@link #RETRYING}, {@link #DEAD_LETTERED} or {@link #DISCARDED}
 * @param availableAt when a retrying message comes back; null otherwise
 */
public record FailResult(String outcome, @Nullable Instant availableAt, int attempt, int maxAttempts) {
    /** Pending again from {@code availableAt}. */
    public static final String RETRYING = "retrying";
    /** In the dead-letter set (status failed). */
    public static final String DEAD_LETTERED = "dead_lettered";
    /** Deleted: the queue's {@code on_exhausted} is {@code discard}. */
    public static final String DISCARDED = "discarded";
}
