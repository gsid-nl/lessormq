package dev.lessormq.spring;

import java.time.Duration;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The lessormq.* properties. There is nothing about retries, backoff or dead-lettering:
 * those are the queue's settings ({@code PATCH /v1/queues/{queue}}).
 */
@ConfigurationProperties("lessormq")
public class LessorMQProperties {
    /** The server, such as https://mq.example.com. */
    private @Nullable String url;

    /** The API key: publish and/or consume scope. */
    private @Nullable String key;

    /** Retries of a request after a network error, a 5xx or a 429, when it is safe to repeat. */
    private int httpRetries = 3;

    /** The base delay between those retries, times the attempt number. */
    private Duration retryDelay = Duration.ofMillis(100);

    /** The deadline per request; a claim gets its wait on top. */
    private Duration timeout = Duration.ofSeconds(10);

    /** Give every publish without a dedup key one of its own, so it can be retried safely. */
    private boolean autoDedupKey = true;

    private final Worker worker = new Worker();

    public @Nullable String getUrl() {
        return url;
    }

    public void setUrl(@Nullable String url) {
        this.url = url;
    }

    public @Nullable String getKey() {
        return key;
    }

    public void setKey(@Nullable String key) {
        this.key = key;
    }

    public int getHttpRetries() {
        return httpRetries;
    }

    public void setHttpRetries(int httpRetries) {
        this.httpRetries = httpRetries;
    }

    public Duration getRetryDelay() {
        return retryDelay;
    }

    public void setRetryDelay(Duration retryDelay) {
        this.retryDelay = retryDelay;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public boolean isAutoDedupKey() {
        return autoDedupKey;
    }

    public void setAutoDedupKey(boolean autoDedupKey) {
        this.autoDedupKey = autoDedupKey;
    }

    public Worker getWorker() {
        return worker;
    }

    /** The workers of @LessorMQListener methods; each annotation can override these. */
    public static class Worker {
        /** Start the workers with the application context. Off: publish only, or start them yourself. */
        private boolean autoStartup = true;

        /** Messages handled at the same time per listener, each on its own thread. */
        private int concurrency = 1;

        /** Shown to administrators in the queue's list of workers; host:pid when empty. */
        private @Nullable String consumer;

        /** The path the workers run jobs through (a VPN exit, a proxy, an API account). */
        private @Nullable String route;

        /**
         * How long a claim waits for a message (at most 20). Stopping waits for a claim in
         * flight, so this bounds the shutdown of an idle worker.
         */
        private int waitSeconds = 5;

        /** The lease per claim and per extend; the queue's when 0. */
        private int leaseSeconds;

        /** Extend the lease while a handler runs. */
        private boolean autoExtend = true;

        /** The time between extends; a third of the lease when not set. */
        private @Nullable Duration extendEvery;

        /** The wait after a claim failed (the server could not be reached, say). */
        private Duration errorPause = Duration.ofSeconds(1);

        public boolean isAutoStartup() {
            return autoStartup;
        }

        public void setAutoStartup(boolean autoStartup) {
            this.autoStartup = autoStartup;
        }

        public int getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(int concurrency) {
            this.concurrency = concurrency;
        }

        public @Nullable String getConsumer() {
            return consumer;
        }

        public void setConsumer(@Nullable String consumer) {
            this.consumer = consumer;
        }

        public @Nullable String getRoute() {
            return route;
        }

        public void setRoute(@Nullable String route) {
            this.route = route;
        }

        public int getWaitSeconds() {
            return waitSeconds;
        }

        public void setWaitSeconds(int waitSeconds) {
            this.waitSeconds = waitSeconds;
        }

        public int getLeaseSeconds() {
            return leaseSeconds;
        }

        public void setLeaseSeconds(int leaseSeconds) {
            this.leaseSeconds = leaseSeconds;
        }

        public boolean isAutoExtend() {
            return autoExtend;
        }

        public void setAutoExtend(boolean autoExtend) {
            this.autoExtend = autoExtend;
        }

        public @Nullable Duration getExtendEvery() {
            return extendEvery;
        }

        public void setExtendEvery(@Nullable Duration extendEvery) {
            this.extendEvery = extendEvery;
        }

        public Duration getErrorPause() {
            return errorPause;
        }

        public void setErrorPause(Duration errorPause) {
            this.errorPause = errorPause;
        }
    }
}
