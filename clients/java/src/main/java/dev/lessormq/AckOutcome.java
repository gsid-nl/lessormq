package dev.lessormq;

/** What became of one lease of {@link LessorMQClient#ackBatch}. */
public enum AckOutcome {
    /** The message is completed (also when this lease was acked before). */
    ACKED("acked"),
    /** The receipt is not the current lease; stop reporting on it. */
    LEASE_LOST("lease_lost"),
    /** No such message. */
    NOT_FOUND("not_found"),
    /** The key may not use the message's queue. */
    FORBIDDEN("forbidden");

    private final String value;

    AckOutcome(String value) {
        this.value = value;
    }

    /** The outcome as the protocol writes it. */
    public String value() {
        return value;
    }

    /** The outcome for a protocol value; one this version does not know reads as NOT_FOUND. */
    static AckOutcome of(String value) {
        for (AckOutcome outcome : values()) {
            if (outcome.value.equals(value)) {
                return outcome;
            }
        }
        return NOT_FOUND;
    }
}
