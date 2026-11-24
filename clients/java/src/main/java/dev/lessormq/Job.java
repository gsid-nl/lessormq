package dev.lessormq;

/** What a handler gets: the message, its lease, and a way to keep the lease. */
public final class Job {
    private final LessorMQClient client;
    private final Lease lease;
    private volatile boolean leaseLost;

    Job(LessorMQClient client, Lease lease) {
        this.client = client;
        this.lease = lease;
    }

    public Message message() {
        return lease.message();
    }

    public Lease lease() {
        return lease;
    }

    /**
     * Extends the lease now, by the queue's lease. The worker already does this on a timer;
     * call it only with auto-extend off, or to ask for a longer lease at once.
     *
     * @throws LeaseLostException when the lease already ended
     */
    public void extend() {
        extend(0);
    }

    /** {@link #extend()} for leaseSeconds (0: the queue's lease). */
    public void extend(int leaseSeconds) {
        try {
            client.extend(lease, leaseSeconds);
        } catch (LeaseLostException e) {
            leaseLost = true;
            throw e;
        }
    }

    /**
     * Whether the lease was lost while the handler ran: the message is someone else's now.
     * A long handler can check this and stop early; its outcome is not reported either way.
     */
    public boolean isLeaseLost() {
        return leaseLost;
    }

    void markLeaseLost() {
        leaseLost = true;
    }

    @Override
    public String toString() {
        return "Job(" + lease.message() + ")";
    }
}
