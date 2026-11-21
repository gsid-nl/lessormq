<?php

declare(strict_types=1);

namespace App\Service;

use App\Repository\MessageRepository;
use Predis\Client as RedisClient;
use Psr\Log\LoggerInterface;

/**
 * The figures of a tenant's queues, for the API, the admin interfaces and the metrics.
 *
 * Open and dead-letter messages are counted live: there are few of them, and they are
 * what an alert or a person acts on. Completed messages are most of the table (kept for
 * the queue's retention), and counting them takes time in proportion to their number, so
 * that count is cached for half a minute per queue and dropped when an admin action
 * changes it. A page that refreshes every few seconds then reads them once in a while.
 */
class QueueStatsService
{
    private const CACHE_PREFIX = 'queue_completed:';
    private const CACHE_TTL = 30;

    public function __construct(
        private readonly MessageRepository $messages,
        private readonly RedisClient $redis,
        private readonly WorkerRegistry $workers,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * @param list<string> $queues
     * @return array<string, array{
     *     ready: int, delayed: int, processing: int, completed: int, failed: int,
     *     oldest_ready_seconds: int, oldest_processing_seconds: int, max_retry: int, workers: int
     * }>
     */
    public function forQueues(string $tenant, array $queues, \DateTimeInterface $now): array
    {
        $queues = array_values($queues);
        $stats = $this->messages->queueStats($tenant, $queues, $now);

        if ($stats === []) {
            return [];
        }

        $keys = array_map(fn(string $queue) => $this->key($tenant, $queue), $queues);

        try {
            $cached = $this->redis->mget($keys);
        } catch (\Throwable $e) {
            $this->logger->warning('Completed counts cache unavailable', ['error' => $e->getMessage()]);
            $cached = [];
        }

        foreach ($queues as $i => $queue) {
            if (isset($cached[$i]) && is_numeric($cached[$i])) {
                $stats[$queue]['completed'] = (int) $cached[$i];
                continue;
            }

            $stats[$queue]['completed'] = $this->messages->completedCount($tenant, $queue);

            try {
                $this->redis->set($keys[$i], (string) $stats[$queue]['completed'], 'EX', self::CACHE_TTL);
            } catch (\Throwable $e) {
                $this->logger->warning('Could not cache a completed count', ['error' => $e->getMessage()]);
            }
        }

        $holding = $this->messages->processingByConsumer($tenant, $queues);

        foreach ($this->workers->counts($tenant, $queues, $holding) as $queue => $n) {
            $stats[$queue]['workers'] = $n;
        }

        return $stats;
    }

    /**
     * Drop a queue's cached completed count, after something removed or replayed some of
     * its completed messages.
     */
    public function forget(string $tenant, string $queue): void
    {
        try {
            $this->redis->del([$this->key($tenant, $queue)]);
        } catch (\Throwable $e) {
            $this->logger->warning('Could not drop a cached completed count', ['error' => $e->getMessage()]);
        }
    }

    private function key(string $tenant, string $queue): string
    {
        return self::CACHE_PREFIX . $tenant . ':' . $queue;
    }
}
