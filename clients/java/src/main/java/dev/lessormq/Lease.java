package dev.lessormq;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import tools.jackson.databind.node.NullNode;

/**
 * A claimed message and the proof of holding it. Extend, ack, fail and release take the
 * lease; only its receipt is accepted, and only until {@link #until()}, which an extend
 * moves. Safe to use from several threads.
 */
public final class Lease {
    private final String receipt;
    private final Message message;
    private volatile Instant until;

    /** A lease from its parts, for adapters and tests; claims return real ones. */
    public Lease(String receipt, Instant until, Message message) {
        this.receipt = receipt;
        this.until = until;
        this.message = message;
    }

    /**
     * A lease known only by its message id and receipt, for reporting on a message
     * claimed elsewhere (another process, before a restart). Its message has no body.
     */
    public static Lease of(String messageId, String receipt) {
        Instant now = Instant.now();
        return new Lease(receipt, now, new Message(messageId, "", NullNode.getInstance(), Map.of(), 0, 0, now));
    }

    public String receipt() {
        return receipt;
    }

    public Message message() {
        return message;
    }

    /** When the lease ends unless it is extended. */
    public Instant until() {
        return until;
    }

    /** The time until the lease ends; negative once it has. */
    public Duration remaining() {
        return Duration.between(Instant.now(), until);
    }

    void extendedTo(Instant until) {
        this.until = until;
    }

    @Override
    public String toString() {
        return "Lease(" + message.id() + ", until " + until + ")";
    }
}
