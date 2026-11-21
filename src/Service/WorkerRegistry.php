<?php

declare(strict_types=1);

namespace App\Service;

use App\Api\Time;
use Predis\Client as RedisClient;
use Psr\Log\LoggerInterface;
use Symfony\Component\HttpFoundation\RequestStack;

/**
 * Who works on a queue: the workers that claim from it, by the consumer name they claim
 * with (the client libraries send host:pid by default), with the client they run (its
 * User-Agent), their address, when they last claimed and whether they wait now.
 *
 * Kept in Redis per queue, written once per claim request: a hash of the workers with
 * their last claim, and a sorted set of the ones waiting now with the end of their wait.
 * Which messages a worker holds comes from the database (consumed_by). A worker counts as
 * at work while it claimed within the last minute or holds a message, and is listed for
 * ten minutes after its last claim. Without Redis nothing is recorded and the lists are
 * empty; claims are not affected.
 */
class WorkerRegistry
{
    /** A worker that claimed this recently counts as at work. */
    public const RECENT_SECONDS = 60;
    /** How long after its last claim a worker is still listed. */
    private const LISTED_SECONDS = 600;
    private const SEEN_PREFIX = 'workers:';
    private const WAITING_PREFIX = 'workers_waiting:';
    private const TTL = 3600;

    public function __construct(
        private readonly RedisClient $redis,
        private readonly RequestStack $requests,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * A claim on this queue: the worker is at work, with the client and address of the
     * request it claims in.
     */
    public function claimed(string $tenant, string $queue, string $consumer): void
    {
        $request = $this->requests->getCurrentRequest();
        $entry = json_encode([
            'seen' => microtime(true),
            'client' => $request === null ? null : mb_substr((string) $request->headers->get('User-Agent'), 0, 200),
            'address' => $request?->getClientIp(),
        ]);
        $key = $this->seenKey($tenant, $queue);
        $name = self::name($consumer);

        try {
            $this->redis->pipeline(static function ($pipe) use ($key, $name, $entry): void {
                $pipe->hset($key, $name, $entry);
                $pipe->expire($key, self::TTL);
            });
        } catch (\Throwable $e) {
            $this->logger->warning('Could not record a worker', ['error' => $e->getMessage()]);
        }
    }

    /** The worker waits for work on this queue until $until. */
    public function waiting(string $tenant, string $queue, string $consumer, float $until): void
    {
        $key = $this->waitingKey($tenant, $queue);
        $name = self::name($consumer);

        try {
            $this->redis->pipeline(static function ($pipe) use ($key, $name, $until): void {
                $pipe->zadd($key, [$name => $until]);
                $pipe->expire($key, self::TTL);
            });
        } catch (\Throwable $e) {
            $this->logger->warning('Could not record a waiting worker', ['error' => $e->getMessage()]);
        }
    }

    public function doneWaiting(string $tenant, string $queue, string $consumer): void
    {
        try {
            $this->redis->zrem($this->waitingKey($tenant, $queue), self::name($consumer));
        } catch (\Throwable $e) {
            $this->logger->warning('Could not record a waiting worker', ['error' => $e->getMessage()]);
        }
    }

    /**
     * The workers of a queue: those that claimed in the last ten minutes, and those that
     * hold a message, newest claim first.
     *
     * @param array<string, int> $holding consumer => messages it holds now
     * @return list<array{
     *     consumer: string, last_claim_at: ?string, waiting: bool, processing: int,
     *     client: ?string, address: ?string, active: bool
     * }>
     */
    public function list(string $tenant, string $queue, array $holding): array
    {
        [$seen, $waiting] = $this->read($tenant, $queue);
        $now = microtime(true);
        $workers = [];

        foreach ($seen as $name => $entry) {
            if ($entry['seen'] < $now - self::LISTED_SECONDS && !isset($holding[$name])) {
                continue;
            }

            $workers[$name] = $this->view($name, $entry, $waiting, $holding, $now);
        }

        foreach ($holding as $name => $n) {
            $workers[$name] ??= $this->view((string) $name, null, $waiting, $holding, $now);
        }

        usort($workers, static fn(array $a, array $b) => [$b['last_claim_at'], $a['consumer']]
            <=> [$a['last_claim_at'], $b['consumer']]);

        return array_values($workers);
    }

    /**
     * How many workers are at work on each queue: claimed within the last minute, or
     * hold a message.
     *
     * @param list<string>                      $queues
     * @param array<string, array<string, int>> $holding queue => consumer => messages
     * @return array<string, int>
     */
    public function counts(string $tenant, array $queues, array $holding): array
    {
        $counts = array_fill_keys($queues, 0);

        if ($queues === []) {
            return $counts;
        }

        $keys = array_map(fn(string $queue) => $this->seenKey($tenant, $queue), $queues);

        try {
            $hashes = $this->redis->pipeline(static function ($pipe) use ($keys): void {
                foreach ($keys as $key) {
                    $pipe->hgetall($key);
                }
            });
        } catch (\Throwable $e) {
            $this->logger->warning('Could not read the workers', ['error' => $e->getMessage()]);
            $hashes = [];
        }

        $recent = microtime(true) - self::RECENT_SECONDS;

        foreach ($queues as $i => $queue) {
            $names = array_keys($holding[$queue] ?? []);

            foreach (is_array($hashes[$i] ?? null) ? $hashes[$i] : [] as $name => $json) {
                $entry = json_decode((string) $json, true);

                if (is_array($entry) && ($entry['seen'] ?? 0) >= $recent) {
                    $names[] = (string) $name;
                }
            }

            $counts[$queue] = count(array_unique($names));
        }

        return $counts;
    }

    /**
     * @return array{array<string, array{seen: float, client: ?string, address: ?string}>, array<string, float>}
     */
    private function read(string $tenant, string $queue): array
    {
        try {
            $hash = $this->redis->hgetall($this->seenKey($tenant, $queue));
            $this->redis->zremrangebyscore($this->waitingKey($tenant, $queue), '-inf', (string) microtime(true));
            $waiting = $this->redis->zrange($this->waitingKey($tenant, $queue), 0, -1, ['withscores' => true]);
        } catch (\Throwable $e) {
            $this->logger->warning('Could not read the workers', ['error' => $e->getMessage()]);

            return [[], []];
        }

        $seen = [];

        foreach ($hash as $name => $json) {
            $entry = json_decode((string) $json, true);

            if (is_array($entry) && isset($entry['seen'])) {
                $seen[(string) $name] = $entry;
            }
        }

        return [$seen, array_map('floatval', $waiting)];
    }

    /**
     * @param array{seen: float, client: ?string, address: ?string}|null $entry
     * @param array<string, float> $waiting
     * @param array<string, int>   $holding
     * @return array{
     *     consumer: string, last_claim_at: ?string, waiting: bool, processing: int,
     *     client: ?string, address: ?string, active: bool
     * }
     */
    private function view(string $name, ?array $entry, array $waiting, array $holding, float $now): array
    {
        $seen = $entry['seen'] ?? null;
        $at = $seen === null ? null : \DateTimeImmutable::createFromFormat('U.u', sprintf('%.6F', $seen));

        return [
            'consumer' => $name,
            'last_claim_at' => $at === false || $at === null ? null : Time::format($at),
            'waiting' => isset($waiting[$name]),
            'processing' => $holding[$name] ?? 0,
            'client' => ($entry['client'] ?? '') === '' ? null : $entry['client'],
            'address' => $entry['address'] ?? null,
            'active' => isset($holding[$name]) || ($seen !== null && $seen >= $now - self::RECENT_SECONDS),
        ];
    }

    /** A claim without a consumer name is the worker without a name. */
    public static function name(string $consumer): string
    {
        return $consumer === '' ? '(no name)' : $consumer;
    }

    private function seenKey(string $tenant, string $queue): string
    {
        return self::SEEN_PREFIX . $tenant . ':' . $queue;
    }

    private function waitingKey(string $tenant, string $queue): string
    {
        return self::WAITING_PREFIX . $tenant . ':' . $queue;
    }
}
