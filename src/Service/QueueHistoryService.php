<?php

declare(strict_types=1);

namespace App\Service;

use App\Api\Time;
use App\Model\Durations;
use App\Repository\HistoryRepository;
use Predis\Client as RedisClient;
use Psr\Log\LoggerInterface;

/**
 * How a tenant's queues did over a recent period, bucket by bucket: how many messages were
 * published, completed and dead-lettered, how long the completed ones waited for a worker
 * and how long they ran. For charts in an admin interface; the metrics are the figures of
 * this moment, this is what led up to them.
 *
 * It is read back from the messages that are still kept, so it reaches as far back as the
 * queue's retention (completed messages a day, unless set otherwise) and leaves out what
 * was deleted or discarded. The buckets are whole multiples of their size, so a chart
 * keeps its bars in place from one refresh to the next; the last one is the current
 * bucket, so far.
 *
 * A result is kept for a tenth of its bucket (10 seconds for an hour, 5 minutes for a
 * week): a day of a busy queue takes a few hundred milliseconds to read, a week about a
 * second (300,000 messages on MariaDB).
 */
class QueueHistoryService
{
    /** @var array<string, array{0: int, 1: int}> period => [seconds, seconds per bucket] */
    public const PERIODS = [
        '1h' => [3600, 60],
        '6h' => [21600, 300],
        '24h' => [86400, 900],
        '7d' => [604800, 7200],
    ];

    public const DEFAULT_PERIOD = '24h';

    private const CACHE_PREFIX = 'queue_history:';
    private const CACHE_MIN = 10;
    private const CACHE_MAX = 300;

    public function __construct(
        private readonly HistoryRepository $history,
        private readonly RedisClient $redis,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * @param list<string> $queues the queues to add up
     * @return array{period: string, bucket_seconds: int, from: string, to: string,
     *     totals: array<string, mixed>, buckets: list<array<string, mixed>>}
     */
    public function history(string $tenant, array $queues, string $period, \DateTimeInterface $now): array
    {
        if (!isset(self::PERIODS[$period])) {
            throw new \InvalidArgumentException(sprintf('No period "%s".', $period));
        }

        [$length, $size] = self::PERIODS[$period];
        $buckets = intdiv($length, $size);
        $from = (intdiv($now->getTimestamp(), $size) + 1 - $buckets) * $size;

        sort($queues);
        $key = self::CACHE_PREFIX . $tenant . ':' . $period . ':' . $from . ':' . sha1(implode("\n", $queues));
        $cached = $this->cached($key);

        if ($cached !== null) {
            return $cached;
        }

        $result = $this->compute($tenant, $queues, $period, $from, $size, $buckets, $now);

        try {
            $ttl = max(self::CACHE_MIN, min(self::CACHE_MAX, intdiv($size, 10)));
            $this->redis->set($key, json_encode($result, JSON_THROW_ON_ERROR), 'EX', $ttl);
        } catch (\Throwable $e) {
            $this->logger->warning('Could not cache a queue history', ['error' => $e->getMessage()]);
        }

        return $result;
    }

    /**
     * @param list<string> $queues
     * @return array{period: string, bucket_seconds: int, from: string, to: string,
     *     totals: array<string, mixed>, buckets: list<array<string, mixed>>}
     */
    private function compute(
        string $tenant,
        array $queues,
        string $period,
        int $from,
        int $size,
        int $buckets,
        \DateTimeInterface $now,
    ): array {
        $published = $completed = $deadLettered = array_fill(0, $buckets, 0);
        $waits = $runs = [];

        for ($i = 0; $i < $buckets; $i++) {
            $waits[$i] = new Durations();
            $runs[$i] = new Durations();
        }

        if ($queues !== []) {
            foreach ($this->history->published($tenant, $queues, $from, $size) as $bucket => $count) {
                if (isset($published[$bucket])) {
                    $published[$bucket] += $count;
                }
            }

            foreach ($this->history->deadLettered($tenant, $queues, $from, $size) as $bucket => $count) {
                if (isset($deadLettered[$bucket])) {
                    $deadLettered[$bucket] += $count;
                }
            }

            foreach ($this->history->completed($tenant, $queues, $from, $size) as $row) {
                $bucket = $row['bucket'];

                if (!isset($completed[$bucket])) {
                    continue;
                }

                $completed[$bucket] += $row['count'];
                $waits[$bucket]->add($row['wait_class'], $row['count'], $row['wait_sum'], $row['wait_max']);
                $runs[$bucket]->add($row['run_class'], $row['count'], $row['run_sum'], $row['run_max']);
            }
        }

        $allWaits = new Durations();
        $allRuns = new Durations();
        $series = [];

        for ($i = 0; $i < $buckets; $i++) {
            $allWaits->merge($waits[$i]);
            $allRuns->merge($runs[$i]);
            $series[] = [
                'at' => (string) Time::format(new \DateTimeImmutable('@' . ($from + $i * $size))),
                'published' => $published[$i],
                'completed' => $completed[$i],
                'dead_lettered' => $deadLettered[$i],
                'wait_seconds' => $waits[$i]->view(),
                'run_seconds' => $runs[$i]->view(),
            ];
        }

        return [
            'period' => $period,
            'bucket_seconds' => $size,
            'from' => (string) Time::format(new \DateTimeImmutable('@' . $from)),
            'to' => (string) Time::format($now),
            'totals' => [
                'published' => array_sum($published),
                'completed' => array_sum($completed),
                'dead_lettered' => array_sum($deadLettered),
                'wait_seconds' => $allWaits->view(),
                'run_seconds' => $allRuns->view(),
            ],
            'buckets' => $series,
        ];
    }

    /**
     * @return array{period: string, bucket_seconds: int, from: string, to: string,
     *     totals: array<string, mixed>, buckets: list<array<string, mixed>>}|null
     */
    private function cached(string $key): ?array
    {
        try {
            $value = $this->redis->get($key);
        } catch (\Throwable $e) {
            $this->logger->warning('Queue history cache unavailable', ['error' => $e->getMessage()]);

            return null;
        }

        if (!is_string($value)) {
            return null;
        }

        $decoded = json_decode($value, true);

        return is_array($decoded) ? $decoded : null;
    }
}
