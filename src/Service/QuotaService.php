<?php

declare(strict_types=1);

namespace App\Service;

use App\Api\ApiProblem;
use App\Model\QueueConfig;
use App\Repository\MessageRepository;
use App\Repository\QueueRepository;
use Predis\Client as RedisClient;
use Psr\Log\LoggerInterface;

/**
 * The tenant limits that publishes and claims run into: the rates, and the number of open
 * messages. A tenant over one gets 429 with Retry-After; one without quotas costs nothing.
 *
 * Rates are token buckets in Redis, shared by every application node: a tenant may do
 * its limit per second, in bursts of up to a second's worth. The open messages are
 * counted in the database at most every few seconds and counted on in Redis between
 * those counts. Without Redis no limit is applied: a quota protects the installation,
 * it is not worth refusing work over.
 */
class QuotaService
{
    private const BUCKET_PREFIX = 'quota_rate:';
    private const OPEN_PREFIX = 'quota_open:';
    /** How long a count of open messages is used before it is counted again. */
    private const OPEN_TTL = 5;
    /** What a publish over max_open_messages is told to wait. */
    private const OPEN_RETRY_AFTER = 10;

    /**
     * Takes a token from the bucket in KEYS[1]: ARGV[1] tokens a second, as many at most,
     * ARGV[2] the time now. Returns {1, 0} when there was one, {0, seconds to wait} when
     * not; the wait as a string, as Redis truncates a number from Lua.
     */
    private const TAKE_TOKEN = <<<'LUA'
        local rate = tonumber(ARGV[1])
        local now = tonumber(ARGV[2])
        local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'at')
        local tokens = tonumber(bucket[1]) or rate
        local at = tonumber(bucket[2]) or now
        tokens = math.min(rate, tokens + math.max(0, now - at) * rate)
        local taken = 0
        local wait = 0
        if tokens >= 1 then
            tokens = tokens - 1
            taken = 1
        else
            wait = (1 - tokens) / rate
        end
        redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'at', tostring(now))
        redis.call('EXPIRE', KEYS[1], 60)
        return {taken, tostring(wait)}
        LUA;

    public function __construct(
        private readonly RedisClient $redis,
        private readonly MessageRepository $messages,
        private readonly QueueRepository $queues,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * Before a publish: the publish rate, then room for one more open message.
     */
    public function beforePublish(string $tenant, QueueConfig $config): void
    {
        $this->takeToken($tenant, 'publish', $config->quota('publish_per_second'));

        $max = $config->quota('max_open_messages');

        if ($max !== null && $this->openMessages($tenant) >= $max) {
            throw ApiProblem::rateLimited(
                self::OPEN_RETRY_AFTER,
                sprintf('This tenant has its maximum of %d open messages; publish again once some are done.', $max),
            );
        }
    }

    /**
     * After a publish that created a message: count it on, so publishes between two
     * counts in the database cannot run far past the limit.
     */
    public function afterPublish(string $tenant, QueueConfig $config): void
    {
        if ($config->quota('max_open_messages') === null) {
            return;
        }

        try {
            // Only while a count is held: INCR on a missing key would start one at 1.
            if ($this->redis->exists(self::OPEN_PREFIX . $tenant)) {
                $this->redis->incr(self::OPEN_PREFIX . $tenant);
            }
        } catch (\Throwable $e) {
            $this->logger->warning('Could not count an open message', ['error' => $e->getMessage()]);
        }
    }

    public function beforeClaim(string $tenant, QueueConfig $config): void
    {
        $this->takeToken($tenant, 'claim', $config->quota('claim_per_second'));
    }

    private function takeToken(string $tenant, string $action, ?int $perSecond): void
    {
        if ($perSecond === null) {
            return;
        }

        try {
            [$taken, $wait] = $this->redis->eval(
                self::TAKE_TOKEN,
                1,
                self::BUCKET_PREFIX . $action . ':' . $tenant,
                (string) $perSecond,
                sprintf('%.6F', microtime(true)),
            );
        } catch (\Throwable $e) {
            $this->logger->warning('Rate limit unavailable', ['error' => $e->getMessage()]);

            return;
        }

        if ((int) $taken !== 1) {
            throw ApiProblem::rateLimited(
                (int) ceil((float) $wait),
                sprintf('This tenant may %s %d times a second.', $action, $perSecond),
            );
        }
    }

    private function openMessages(string $tenant): int
    {
        $key = self::OPEN_PREFIX . $tenant;

        try {
            $cached = $this->redis->get($key);

            if (is_numeric($cached)) {
                return (int) $cached;
            }
        } catch (\Throwable $e) {
            $this->logger->warning('Open message count unavailable', ['error' => $e->getMessage()]);

            return 0;
        }

        $open = $this->messages->tenantUsage($tenant, $this->queues->names($tenant))['open_messages'];

        try {
            $this->redis->set($key, (string) $open, 'EX', self::OPEN_TTL);
        } catch (\Throwable $e) {
            $this->logger->warning('Could not cache the open message count', ['error' => $e->getMessage()]);
        }

        return $open;
    }
}
