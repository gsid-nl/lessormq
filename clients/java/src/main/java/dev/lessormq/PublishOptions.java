package dev.lessormq;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * The optional parts of a publish.
 *
 * <pre>{@code
 * PublishOptions.builder().dedupKey("invoice-1042").delaySeconds(600).header("type", "InvoiceDue").build()
 * }</pre>
 */
public final class PublishOptions {
    /** None of them. */
    public static final PublishOptions NONE = builder().build();

    private final Map<String, String> headers;
    private final @Nullable String dedupKey;
    private final int delaySeconds;
    private final @Nullable Instant availableAt;
    private final int maxAttempts;

    private PublishOptions(Builder builder) {
        this.headers = Map.copyOf(builder.headers);
        this.dedupKey = builder.dedupKey;
        this.delaySeconds = builder.delaySeconds;
        this.availableAt = builder.availableAt;
        this.maxAttempts = builder.maxAttempts;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, String> headers() {
        return headers;
    }

    public @Nullable String dedupKey() {
        return dedupKey;
    }

    public int delaySeconds() {
        return delaySeconds;
    }

    public @Nullable Instant availableAt() {
        return availableAt;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    /** Builds {@link PublishOptions}. */
    public static final class Builder {
        private final Map<String, String> headers = new LinkedHashMap<>();
        private @Nullable String dedupKey;
        private int delaySeconds;
        private @Nullable Instant availableAt;
        private int maxAttempts;

        private Builder() {
        }

        /** Free string metadata, passed to the worker untouched. */
        public Builder header(String name, String value) {
            headers.put(name, value);
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            this.headers.putAll(headers);
            return this;
        }

        /**
         * One open message per key: a publish with the key of an open message returns that
         * one ({@link Published#duplicate()}).
         */
        public Builder dedupKey(@Nullable String dedupKey) {
            this.dedupKey = dedupKey;
            return this;
        }

        /** Make the message available this many seconds from now. */
        public Builder delaySeconds(int delaySeconds) {
            this.delaySeconds = delaySeconds;
            return this;
        }

        /** Make the message available at this time (to the second). */
        public Builder availableAt(@Nullable Instant availableAt) {
            this.availableAt = availableAt;
            return this;
        }

        /** Attempts for this message, within the queue's max_attempts_limit; 0 is the queue's. */
        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public PublishOptions build() {
            return new PublishOptions(this);
        }
    }
}
