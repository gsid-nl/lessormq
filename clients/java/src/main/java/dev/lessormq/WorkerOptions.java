package dev.lessormq;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import org.jspecify.annotations.Nullable;

/**
 * How a worker claims and keeps its leases. What happens to a failed message is the
 * queue's business, so there is nothing about retries here.
 */
public final class WorkerOptions {
    /** The defaults: one handler at a time, claims that wait 20 seconds, the lease kept. */
    public static final WorkerOptions DEFAULTS = builder().build();

    private final String consumer;
    private final @Nullable String route;
    private final int waitSeconds;
    private final int leaseSeconds;
    private final @Nullable Duration extendEvery;
    private final boolean autoExtend;
    private final int concurrency;
    private final int maxMessages;
    private final Duration errorPause;
    private final @Nullable ThreadFactory threadFactory;
    private final boolean stopOnShutdown;

    private WorkerOptions(Builder builder) {
        this.consumer = builder.consumer != null ? builder.consumer : defaultConsumer();
        this.route = builder.route;
        this.waitSeconds = builder.waitSeconds;
        this.leaseSeconds = builder.leaseSeconds;
        this.extendEvery = builder.extendEvery;
        this.autoExtend = builder.autoExtend;
        this.concurrency = builder.concurrency;
        this.maxMessages = builder.maxMessages;
        this.errorPause = builder.errorPause;
        this.threadFactory = builder.threadFactory;
        this.stopOnShutdown = builder.stopOnShutdown;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String consumer() {
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

    public @Nullable Duration extendEvery() {
        return extendEvery;
    }

    public boolean autoExtend() {
        return autoExtend;
    }

    public int concurrency() {
        return concurrency;
    }

    public int maxMessages() {
        return maxMessages;
    }

    public Duration errorPause() {
        return errorPause;
    }

    public @Nullable ThreadFactory threadFactory() {
        return threadFactory;
    }

    public boolean stopOnShutdown() {
        return stopOnShutdown;
    }

    /** Builds {@link WorkerOptions}. */
    public static final class Builder {
        private @Nullable String consumer;
        private @Nullable String route;
        private int waitSeconds = 20;
        private int leaseSeconds;
        private @Nullable Duration extendEvery;
        private boolean autoExtend = true;
        private int concurrency = 1;
        private int maxMessages;
        private Duration errorPause = Duration.ofSeconds(1);
        private @Nullable ThreadFactory threadFactory;
        private boolean stopOnShutdown = true;

        private Builder() {
        }

        /**
         * Shown to administrators in the queue's list of workers; host:pid by default, so the
         * workers on one host are told apart.
         */
        public Builder consumer(@Nullable String consumer) {
            this.consumer = consumer;
            return this;
        }

        /** The path this worker runs jobs through (a VPN exit, a proxy, an API account). */
        public Builder route(@Nullable String route) {
            this.route = route;
            return this;
        }

        /** The long-poll wait per claim, at most 20. Default 20. */
        public Builder waitSeconds(int waitSeconds) {
            this.waitSeconds = waitSeconds;
            return this;
        }

        /** The lease per claim, and per automatic extend; 0 is the queue's. */
        public Builder leaseSeconds(int leaseSeconds) {
            this.leaseSeconds = leaseSeconds;
            return this;
        }

        /** The time between automatic extends; a third of the lease when null. */
        public Builder extendEvery(@Nullable Duration extendEvery) {
            this.extendEvery = extendEvery;
            return this;
        }

        /** Whether the lease is extended while a handler runs. Default true. */
        public Builder autoExtend(boolean autoExtend) {
            this.autoExtend = autoExtend;
            return this;
        }

        /**
         * The number of messages handled at the same time, each on its own thread with its
         * own claims. Default 1.
         */
        public Builder concurrency(int concurrency) {
            if (concurrency < 1) {
                throw new IllegalArgumentException("concurrency must be 1 or more, not " + concurrency);
            }
            this.concurrency = concurrency;
            return this;
        }

        /** Stop after this many messages; 0 is no limit. */
        public Builder maxMessages(int maxMessages) {
            this.maxMessages = maxMessages;
            return this;
        }

        /** The wait after a claim failed (the server could not be reached, say). Default 1 s. */
        public Builder errorPause(Duration errorPause) {
            this.errorPause = errorPause;
            return this;
        }

        /**
         * Makes the threads that claim and run handlers; on Java 21,
         * {@code Thread.ofVirtual().factory()} runs them on virtual threads. Default:
         * platform threads named lessormq-QUEUE-N.
         */
        public Builder threadFactory(@Nullable ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * Whether {@link Worker#run()} stops the worker when the JVM shuts down (SIGTERM,
         * Ctrl+C), holding the shutdown until the current messages are reported. Default
         * true. {@link Worker#start()} leaves stopping to its caller.
         */
        public Builder stopOnShutdown(boolean stopOnShutdown) {
            this.stopOnShutdown = stopOnShutdown;
            return this;
        }

        public WorkerOptions build() {
            return new WorkerOptions(this);
        }
    }

    /** host:pid, or empty when the host is unknown. */
    static String defaultConsumer() {
        return DefaultConsumer.VALUE;
    }

    private static final class DefaultConsumer {
        static final String VALUE = find();

        private static String find() {
            String host = host();
            return host.isEmpty() ? "" : host + ":" + ProcessHandle.current().pid();
        }

        /** The machine's name without a DNS lookup where possible: that can hang. */
        private static String host() {
            for (String name : new String[] {"HOSTNAME", "COMPUTERNAME"}) {
                String value = System.getenv(name);
                if (value != null && !value.isBlank()) {
                    return value.trim();
                }
            }
            try {
                return Files.readString(Path.of("/proc/sys/kernel/hostname")).trim();
            } catch (Exception e) {
                // Not Linux.
            }
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                return "";
            }
        }
    }
}
