package dev.lessormq;

/**
 * Runs one message. Returning acks it; throwing fails it, as permanent when
 * {@link PermanentFailure#isPermanent(Throwable)}. What follows a failure is the queue's
 * decision.
 */
@FunctionalInterface
public interface Handler {
    void handle(Job job) throws Exception;
}
