package dev.lessormq;

import java.time.Instant;

/**
 * The answer to a publish: the message, or the open one with the same dedup key
 * ({@code duplicate} true).
 */
public record Published(String id, String queue, String status, Instant createdAt, Instant availableAt,
        boolean duplicate) {
}
