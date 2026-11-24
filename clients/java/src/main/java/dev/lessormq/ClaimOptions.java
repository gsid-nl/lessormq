package dev.lessormq;

import org.jspecify.annotations.Nullable;

/** The settings a consumer may pass when it claims; the queue's limits apply. */
public final class ClaimOptions {
    /** None of them: no wait, the queue's lease. */
    public static final ClaimOptions NONE = builder().build();

    private final @Nullable String consumer;
    private final @Nullable String route;
    private final int waitSeconds;
    private final int leaseSeconds;

    private ClaimOptions(Builder builder) {
        this.consumer = builder.consumer;
        this.route = builder.route;
        this.waitSeconds = builder.waitSeconds;
        this.leaseSeconds = builder.leaseSeconds;
    }

    public static Builder builder() {
        return new Builder();
    }

    public @Nullable String consumer() {
        return consumer;
    }

    public @Nullable String route() {
        return route;
    }

    public int waitSeconds() {
        return waitSeconds;
    }

    public int leaseSeconds() {
        return leaseSeconds;
    }

    /** Builds {@link ClaimOptions}. */
    public static final class Builder {
        private @Nullable String consumer;
        private @Nullable String route;
        private int waitSeconds;
        private int leaseSeconds;

        private Builder() {
        }

        /** Shown to administrators in the queue's list of workers. */
        public Builder consumer(@Nullable String consumer) {
            this.consumer = consumer;
            return this;
        }

        /** The path this worker runs jobs through (a VPN exit, a proxy, an API account). */
        public Builder route(@Nullable String route) {
            this.route = route;
            return this;
        }

        /** Wait up to this long for a message (at most 20). */
        public Builder waitSeconds(int waitSeconds) {
            this.waitSeconds = waitSeconds;
            return this;
        }

        /** The lease for this claim; 0 is the queue's. */
        public Builder leaseSeconds(int leaseSeconds) {
            this.leaseSeconds = leaseSeconds;
            return this;
        }

        public ClaimOptions build() {
            return new ClaimOptions(this);
        }
    }
}
