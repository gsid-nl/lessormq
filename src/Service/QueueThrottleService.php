<?php

declare(strict_types=1);

namespace App\Service;

use Predis\Client as RedisClient;
use Psr\Log\LoggerInterface;

/**
 * Cluster-wide "run this at most once per interval" gate, backed by SET NX EX.
 *
 * Used to keep housekeeping work (stale-message sweeps) off the per-request path:
 * the first caller in each window wins, everyone else skips the work entirely.
 */
class QueueThrottleService
{
    private const REDIS_KEY_PREFIX = 'queue_throttle:';

    public function __construct(
        private readonly RedisClient $redis,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * @return bool true when the caller may run the work, false when another
     *              caller already claimed the current window (or Redis is down)
     */
    public function attempt(string $key, int $intervalSeconds): bool
    {
        try {
            return $this->redis->set(self::REDIS_KEY_PREFIX . $key, '1', 'EX', $intervalSeconds, 'NX') !== null;
        } catch (\Exception $e) {
            $this->logger->debug('Throttle gate unavailable', [
                'key' => $key,
                'error' => $e->getMessage(),
            ]);

            return false;
        }
    }
}
