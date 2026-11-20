<?php

declare(strict_types=1);

namespace App\Model;

/**
 * What the server does after a failed attempt, from the queue's settings. One place for
 * a fail and (in SQL, without jitter) the lease sweep.
 */
final class FailurePolicy
{
    public const RETRYING = 'retrying';
    public const DEAD_LETTERED = 'dead_lettered';
    public const DISCARDED = 'discarded';

    private function __construct()
    {
    }

    /**
     * How many retries the message gets after its first attempt.
     *
     * @param int|null $messageMaxAttempts what the publisher asked for
     */
    public static function retryLimit(QueueConfig $config, ?int $messageMaxAttempts): int
    {
        return min($messageMaxAttempts ?? $config->int('max_attempts'), $config->int('max_attempts_limit')) - 1;
    }

    /**
     * @param int $retriesSoFar failed attempts before this one (the message's retry_count)
     * @return array{outcome: string, delay: int} delay in seconds, for a retry
     */
    public static function decide(QueueConfig $config, int $retriesSoFar, int $retryLimit, bool $permanent): array
    {
        $giveUp = ($permanent && $config->settings['on_permanent'] === 'dead_letter') || $retriesSoFar >= $retryLimit;

        if ($giveUp) {
            return [
                'outcome' => $config->settings['on_exhausted'] === 'discard' ? self::DISCARDED : self::DEAD_LETTERED,
                'delay' => 0,
            ];
        }

        return ['outcome' => self::RETRYING, 'delay' => self::delay($config->settings['backoff'], $retriesSoFar)];
    }

    /**
     * min(initial * multiplier^retries, max), randomised by +/- jitter so that a batch
     * that failed together does not come back together.
     *
     * @param array{initial_seconds: int, multiplier: int|float, max_seconds: int, jitter: int|float} $backoff
     */
    public static function delay(array $backoff, int $retriesSoFar): int
    {
        $delay = min($backoff['initial_seconds'] * $backoff['multiplier'] ** $retriesSoFar, $backoff['max_seconds']);

        if ($backoff['jitter'] > 0) {
            $delay += (random_int(0, 2_000_000) / 1_000_000 - 1) * $delay * $backoff['jitter'];
        }

        return max(0, (int) round($delay));
    }
}
