<?php

declare(strict_types=1);

namespace App\Service;

use App\Api\Time;
use App\Entity\Enum\MessageStatus;
use App\Entity\Message;
use App\Model\QueueConfig;
use App\Time\Utc;

/**
 * A message as an administrator sees it (Message in docs/openapi.yaml).
 */
final class MessageView
{
    private function __construct()
    {
    }

    /**
     * @return array<string, mixed>
     */
    public static function render(Message $message, QueueConfig $config): array
    {
        $status = $message->getStatus();

        return [
            'id' => $message->getId()->toString(),
            'queue' => $message->getQueue(),
            'status' => $status->value,
            'body' => $message->getBody(),
            'headers' => (object) $message->getHeaders(),
            'dedup_key' => $message->getDedupKey(),
            'attempts' => $message->getAttempts(),
            'max_attempts' => min(
                $message->getMaxAttempts() ?? $config->int('max_attempts'),
                $config->int('max_attempts_limit'),
            ),
            'available_at' => Time::format($message->getProcessingAfter() ?? $message->getCreatedAt()),
            'lease_until' => $status === MessageStatus::PROCESSING ? Time::format($message->getLeaseUntil()) : null,
            'consumer' => $message->getConsumedBy(),
            'last_error' => self::lastError($message),
            'failed_routes' => (object) array_map(
                static fn(string $at) => Time::format(Utc::parse($at)),
                $message->getFailedRoutes(),
            ),
            'created_at' => Time::format($message->getCreatedAt()),
            'updated_at' => Time::format($message->getUpdatedAt()),
            'completed_at' => $status === MessageStatus::COMPLETED ? Time::format($message->getProcessedAt()) : null,
            'failed_at' => $status === MessageStatus::FAILED ? Time::format($message->getFailedAt()) : null,
        ];
    }

    /**
     * The last failure, whichever way it was recorded: a fail, or an expired lease.
     *
     * @return array<string, mixed>|null RecordedError
     */
    public static function lastError(Message $message): ?array
    {
        $details = $message->getErrorDetails() === null ? null : json_decode($message->getErrorDetails(), true);

        if (!is_array($details) && $message->getErrorMessage() === null) {
            return null;
        }

        $details = is_array($details) ? $details : [];

        // The failed attempt: the one before the current, unless it was the last one
        $attempt = $message->getRetryCount() + ($message->getStatus() === MessageStatus::FAILED ? 1 : 0);
        $at = $details['at'] ?? null;

        return [
            'type' => $details['type'] ?? null,
            'message' => (string) ($details['message'] ?? $message->getErrorMessage() ?? ''),
            'details' => $details['details'] ?? null,
            'at' => is_string($at) ? Time::format(Utc::parse($at)) : Time::format($message->getUpdatedAt()),
            'attempt' => (int) ($details['attempt'] ?? $attempt),
            'permanent' => (bool) ($details['permanent'] ?? false),
            'expired' => (bool) ($details['expired'] ?? false),
            'route' => $details['route'] ?? null,
            'consumer' => $details['consumer'] ?? null,
        ];
    }
}
