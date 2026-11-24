package dev.lessormq;

import org.jspecify.annotations.Nullable;

/** The optional parts of a failure report. */
public final class FailOptions {
    /** None of them: everything comes from the exception. */
    public static final FailOptions NONE = builder().build();

    private final @Nullable Boolean permanent;
    private final @Nullable String type;
    private final @Nullable Object details;

    private FailOptions(Builder builder) {
        this.permanent = builder.permanent;
        this.type = builder.type;
        this.details = builder.details;
    }

    public static Builder builder() {
        return new Builder();
    }

    public @Nullable Boolean permanent() {
        return permanent;
    }

    public @Nullable String type() {
        return type;
    }

    public @Nullable Object details() {
        return details;
    }

    /** Builds {@link FailOptions}. */
    public static final class Builder {
        private @Nullable Boolean permanent;
        private @Nullable String type;
        private @Nullable Object details;

        private Builder() {
        }

        /**
         * Overrides {@link PermanentFailure#isPermanent(Throwable)}: the input itself cannot
         * be processed.
         */
        public Builder permanent(@Nullable Boolean permanent) {
            this.permanent = permanent;
            return this;
        }

        /** The error type; by default the exception's class. */
        public Builder type(@Nullable String type) {
            this.type = type;
            return this;
        }

        /** Any value the JSON mapper takes, with more context. Stored, not interpreted. */
        public Builder details(@Nullable Object details) {
            this.details = details;
            return this;
        }

        public FailOptions build() {
            return new FailOptions(this);
        }
    }
}
