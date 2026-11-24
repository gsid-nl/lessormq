package dev.lessormq;

/** A queue's counts, for monitoring. Not part of the protocol. */
public record Stats(int ready, int delayed, int processing, int completed, int failed, int oldestReadySeconds,
        int oldestProcessingSeconds, int workers) {
}
