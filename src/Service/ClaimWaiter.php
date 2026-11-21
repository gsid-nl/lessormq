<?php

declare(strict_types=1);

namespace App\Service;

use Predis\Client as RedisClient;
use Psr\Log\LoggerInterface;

/**
 * Wakes the claims that wait for work (wait_seconds) when a queue may have some: a
 * publish, a retry, a release, a replay, a resume. And keeps count of them: a waiting
 * claim holds a PHP worker, so at most $maxWaitingClaims wait at once on a node.
 *
 * Per queue a short list in Redis holds one token per such event; a waiting claim blocks
 * on it (BLPOP) and claims as soon as it gets one, instead of asking the database every
 * second. A token stays until taken, so an event between a claim finding nothing and
 * starting to wait still wakes it. One event wakes one claim. Tokens nobody takes run
 * out after a minute, and a list holds a few dozen at most; a claim woken for nothing
 * finds nothing and waits again.
 *
 * Without Redis a waiting claim asks the database once a second, as before.
 */
class ClaimWaiter
{
    private const PREFIX = 'claim_wake:';
    private const WAITING_PREFIX = 'claims_waiting:';
    private const TOKENS_KEPT = 50;
    private const TOKEN_TTL = 60;
    /**
     * The longest single BLPOP: below the Redis client's read timeout (2 s), which a
     * longer block would trip. A longer wait is several of them.
     */
    private const BLOCK_SECONDS = 1.5;

    /**
     * @param int $maxWaitingClaims claims that may wait at once on this node (0: no limit)
     */
    public function __construct(
        private readonly RedisClient $redis,
        private readonly LoggerInterface $logger,
        private readonly int $maxWaitingClaims = 30,
    ) {
    }

    /**
     * Before a claim starts waiting: false when this node already has its maximum of
     * waiting claims, and this one should wait no more than a moment.
     *
     * The waiting claims of a node are a sorted set in Redis, each with the time its wait
     * ends; a claim whose process died before leave() drops out once that time is past.
     */
    public function enter(string $id, float $until): bool
    {
        if ($this->maxWaitingClaims <= 0) {
            return true;
        }

        $key = $this->waitingKey();
        $now = microtime(true);

        try {
            $this->redis->zremrangebyscore($key, '-inf', (string) $now);

            if ($this->redis->zcard($key) >= $this->maxWaitingClaims) {
                return false;
            }

            $this->redis->zadd($key, [$id => $until]);
            $this->redis->expire($key, 60);
        } catch (\Throwable $e) {
            $this->logger->warning('Could not count waiting claims', ['error' => $e->getMessage()]);
        }

        return true;
    }

    public function leave(string $id): void
    {
        if ($this->maxWaitingClaims <= 0) {
            return;
        }

        try {
            $this->redis->zrem($this->waitingKey(), $id);
        } catch (\Throwable $e) {
            $this->logger->warning('Could not count waiting claims', ['error' => $e->getMessage()]);
        }
    }

    public function notify(string $tenant, string $queue): void
    {
        $key = $this->key($tenant, $queue);

        try {
            $this->redis->pipeline(static function ($pipe) use ($key): void {
                $pipe->rpush($key, ['1']);
                $pipe->ltrim($key, -self::TOKENS_KEPT, -1);
                $pipe->expire($key, self::TOKEN_TTL);
            });
        } catch (\Throwable $e) {
            $this->logger->warning('Could not wake waiting claims', ['error' => $e->getMessage()]);
        }
    }

    /**
     * Wait at most $seconds for a token of this queue.
     *
     * @return bool true when woken (or when Redis failed and a second has passed instead),
     *              false when the time ran out
     */
    public function wait(string $tenant, string $queue, float $seconds): bool
    {
        $key = $this->key($tenant, $queue);
        $deadline = microtime(true) + $seconds;

        while (($left = $deadline - microtime(true)) > 0.01) {
            try {
                // Redis 6 takes a fractional timeout; 0 would block for ever.
                if ($this->redis->blpop([$key], max(0.01, min($left, self::BLOCK_SECONDS))) !== null) {
                    return true;
                }
            } catch (\Throwable $e) {
                $this->logger->warning('Waiting for work without Redis', ['error' => $e->getMessage()]);
                usleep((int) (min(1.0, $left) * 1_000_000));

                return true;
            }
        }

        return false;
    }

    private function waitingKey(): string
    {
        return self::WAITING_PREFIX . gethostname();
    }

    private function key(string $tenant, string $queue): string
    {
        return self::PREFIX . $tenant . ':' . $queue;
    }
}
