<?php

declare(strict_types=1);

namespace App\Repository;

use App\Database\Dialect;
use App\Model\Durations;
use App\Time\Utc;
use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Ramsey\Uuid\Type\Hexadecimal;
use Ramsey\Uuid\Uuid;

/**
 * What happened in a tenant's queues over a period, read back from the messages that are
 * still kept: when they were published, completed or dead-lettered, and how long they
 * waited and ran. Every figure is grouped per bucket of $size seconds from $from, in the
 * database, so a busy period comes back as a few rows per bucket.
 *
 * Each query reads a range of an index rather than the queue's whole history: completed
 * messages by (tenant_id, queue, status, processing_started_at), published ones by the
 * message id, which is ordered by the time it was made, dead-lettered ones by the
 * dead-letter set, which is small.
 */
class HistoryRepository
{
    /**
     * How long before the period a completed message may have started and still count:
     * the index is read from there, so a run longer than this that ends in the period is
     * left out.
     */
    public const LONGEST_RUN = 3600;

    private const STATUSES = ['pending', 'processing', 'completed', 'failed'];

    public function __construct(private readonly Connection $connection, private readonly Dialect $dialect)
    {
    }

    /**
     * Completed messages per bucket of their completion, per class of their wait (from
     * available to claimed, for the attempt that succeeded) and of their run (from claimed
     * to completed); Durations::BOUNDS gives the classes.
     *
     * @param list<string> $queues
     * @return list<array{bucket: int, wait_class: int, run_class: int, count: int,
     *     wait_sum: float, wait_max: int, run_sum: float, run_max: int}>
     */
    public function completed(string $tenant, array $queues, int $from, int $size): array
    {
        $epoch = $this->dialect->epochSeconds(...);
        $sql = <<<SQL
            SELECT bucket, wait_class, run_class, COUNT(*) AS n,
                   SUM(wait) AS wait_sum, MAX(wait) AS wait_max, SUM(run) AS run_sum, MAX(run) AS run_max
            FROM (
                SELECT bucket, wait, run,
                       {$this->classOf('wait')} AS wait_class,
                       {$this->classOf('run')} AS run_class
                FROM (
                    SELECT FLOOR(({$epoch('processed_at')} - :from) / :size) AS bucket,
                           {$epoch('processing_started_at')}
                               - {$epoch('COALESCE(processing_after, created_at)')} AS wait,
                           {$epoch('processed_at')} - {$epoch('processing_started_at')} AS run
                    FROM messages
                    WHERE tenant_id = :tenant AND queue IN (:queues) AND status = 'completed'
                      AND processing_started_at >= :scan_from AND processed_at >= :from_time
                ) finished
            ) classed
            GROUP BY bucket, wait_class, run_class
            SQL;

        $rows = $this->connection->fetchAllAssociative(
            $sql,
            $this->params($tenant, $queues, $from, $size) + [
                'scan_from' => Utc::sql(self::at($from - self::LONGEST_RUN)),
            ],
            $this->types(),
        );

        return array_map(static fn(array $row): array => [
            'bucket' => (int) $row['bucket'],
            'wait_class' => (int) $row['wait_class'],
            'run_class' => (int) $row['run_class'],
            'count' => (int) $row['n'],
            'wait_sum' => (float) $row['wait_sum'],
            'wait_max' => (int) $row['wait_max'],
            'run_sum' => (float) $row['run_sum'],
            'run_max' => (int) $row['run_max'],
        ], $rows);
    }

    /**
     * Messages published per bucket, as far as they are still kept: open, completed
     * (until the purge) or dead-lettered.
     *
     * @param list<string> $queues
     * @return array<int, int> bucket => count
     */
    public function published(string $tenant, array $queues, int $from, int $size): array
    {
        $epoch = $this->dialect->epochSeconds(...);

        // The id range keeps the read to the period: ids are ordered by the time they were
        // made, and idx_tenant_claim holds them per (tenant_id, queue, status).
        return $this->perBucket(
            <<<SQL
                SELECT FLOOR(({$epoch('created_at')} - :from) / :size) AS bucket
                FROM messages
                WHERE tenant_id = :tenant AND queue IN (:queues) AND status IN (:statuses)
                  AND id >= :first_id AND created_at >= :from_time
                SQL,
            $this->params($tenant, $queues, $from, $size) + [
                'statuses' => self::STATUSES,
                'first_id' => Uuid::fromDateTime(self::at($from), new Hexadecimal('000000000000'), 0),
            ],
            $this->types() + ['statuses' => ArrayParameterType::STRING, 'first_id' => 'uuid_binary_ordered_time'],
        );
    }

    /**
     * Messages dead-lettered per bucket that are still in the dead-letter set (a replay
     * takes a message out of it).
     *
     * @param list<string> $queues
     * @return array<int, int> bucket => count
     */
    public function deadLettered(string $tenant, array $queues, int $from, int $size): array
    {
        $epoch = $this->dialect->epochSeconds(...);

        return $this->perBucket(
            <<<SQL
                SELECT FLOOR(({$epoch('failed_at')} - :from) / :size) AS bucket
                FROM messages
                WHERE tenant_id = :tenant AND queue IN (:queues) AND status = 'failed'
                  AND failed_at >= :from_time
                SQL,
            $this->params($tenant, $queues, $from, $size),
            $this->types(),
        );
    }

    /**
     * @param array<string, mixed> $params
     * @param array<string, mixed> $types
     * @return array<int, int>
     */
    private function perBucket(string $buckets, array $params, array $types): array
    {
        $rows = $this->connection->fetchAllAssociative(
            "SELECT bucket, COUNT(*) AS n FROM ({$buckets}) timed GROUP BY bucket",
            $params,
            $types,
        );
        $counts = [];

        foreach ($rows as $row) {
            $counts[(int) $row['bucket']] = (int) $row['n'];
        }

        return $counts;
    }

    private static function at(int $timestamp): \DateTimeImmutable
    {
        return new \DateTimeImmutable('@' . $timestamp);
    }

    private function classOf(string $seconds): string
    {
        return Durations::classSql($seconds);
    }

    /**
     * @param list<string> $queues
     * @return array<string, mixed>
     */
    private function params(string $tenant, array $queues, int $from, int $size): array
    {
        return [
            'tenant' => $tenant,
            'queues' => $queues,
            'from' => $from,
            'size' => $size,
            'from_time' => Utc::sql(self::at($from)),
        ];
    }

    /**
     * Numbers are bound as numbers: SQLite compares a number bound as text as text.
     *
     * @return array<string, mixed>
     */
    private function types(): array
    {
        return [
            'queues' => ArrayParameterType::STRING,
            'from' => ParameterType::INTEGER,
            'size' => ParameterType::INTEGER,
        ];
    }
}
