<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use DateTimeImmutable;
use DateTimeZone;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;
use Predis\Client as RedisClient;

/**
 * Message timestamps were stored as Europe/Amsterdam wall-clock time; from this release
 * on they are UTC. This converts the rows that exist.
 *
 * Safe to run more than once and to resume: a flag in lessormq_flags records the last
 * converted id, and 'done' at the end. Rows are converted in primary key batches, each
 * in its own short transaction with the flag, so the table is never locked as a whole
 * and an interrupted run continues where it stopped instead of shifting a batch twice.
 *
 * Every column is rewritten from its original value in one statement per batch (a CASE
 * per daylight saving period), so a value shifted out of one period is never shifted
 * again by the next. A time in the repeated October hour is read as its first
 * occurrence; there is no way to tell them apart.
 *
 * Rollout: pods of the previous release keep running while this migrates, and they read
 * every time as local. Their stale sweep would then take every lease started under UTC
 * as an hour old and hand the message to a second worker. So before converting, this
 * claims the previous release's sweep throttle keys in Redis for 15 minutes, which stops
 * those sweeps until its pods are gone. The new release uses tenant-scoped keys and is
 * not affected. Rows the old pods write during the overlap stay local time; that only
 * delays their retry or recovery by an hour.
 */
final class Version20261119150000 extends AbstractMigration
{
    private const SOURCE_ZONE = 'Europe/Amsterdam';
    private const FLAG = 'messages_timestamps_utc';
    private const BATCH_SIZE = 5000;
    private const SWEEP_BLOCK_SECONDS = 900;
    private const COLUMNS = [
        'processing_started_at',
        'processing_after',
        'processed_at',
        'failed_at',
        'created_at',
        'updated_at',
    ];

    public function getDescription(): string
    {
        return 'Convert message timestamps from Europe/Amsterdam wall-clock time to UTC';
    }

    public function isTransactional(): bool
    {
        return false;
    }

    public function up(Schema $schema): void
    {
        $this->connection->executeStatement(<<<'SQL'
            CREATE TABLE IF NOT EXISTS lessormq_flags (
                name VARCHAR(64) NOT NULL,
                value VARCHAR(255) NOT NULL,
                PRIMARY KEY (name)
            ) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE = InnoDB
            SQL);
        $this->connection->executeStatement(
            "INSERT IGNORE INTO lessormq_flags (name, value) VALUES (?, '')",
            [self::FLAG],
        );

        $progress = (string) $this->connection->fetchOne('SELECT value FROM lessormq_flags WHERE name = ?', [self::FLAG]);

        if ($progress === 'done') {
            $this->write('Message timestamps are already UTC.');

            return;
        }

        $this->blockPreviousReleaseSweeps();

        $assignments = $this->assignments();
        $lastId = $progress === '' ? null : hex2bin($progress);
        $converted = 0;

        while (true) {
            $ids = $lastId === null
                ? $this->connection->fetchFirstColumn('SELECT id FROM messages ORDER BY id LIMIT ' . self::BATCH_SIZE)
                : $this->connection->fetchFirstColumn(
                    'SELECT id FROM messages WHERE id > ? ORDER BY id LIMIT ' . self::BATCH_SIZE,
                    [$lastId],
                );

            if ($ids === []) {
                break;
            }

            $firstId = $ids[0];
            $batchLast = $ids[count($ids) - 1];

            $this->connection->beginTransaction();

            try {
                $converted += $this->connection->executeStatement(
                    'UPDATE messages SET ' . $assignments . ' WHERE id >= ? AND id <= ?',
                    [$firstId, $batchLast],
                );
                $this->connection->executeStatement(
                    'UPDATE lessormq_flags SET value = ? WHERE name = ?',
                    [bin2hex($batchLast), self::FLAG],
                );
                $this->connection->commit();
            } catch (\Throwable $e) {
                $this->connection->rollBack();

                throw $e;
            }

            $lastId = $batchLast;
        }

        $this->connection->executeStatement("UPDATE lessormq_flags SET value = 'done' WHERE name = ?", [self::FLAG]);
        $this->write(sprintf('Converted the timestamps of %d messages to UTC.', $converted));
    }

    public function down(Schema $schema): void
    {
        $this->throwIrreversibleMigrationException(
            'Timestamps are not converted back; write a forward migration if that is ever needed.',
        );
    }

    /**
     * `col = CASE WHEN col in period 1 THEN col - offset 1 ... ELSE col END` for every column.
     */
    private function assignments(): string
    {
        $cases = [];

        foreach ($this->periods() as [$from, $until, $offset]) {
            $cases[] = sprintf(
                "WHEN %%1\$s >= '%s' AND %%1\$s < '%s' THEN %%1\$s - INTERVAL %d SECOND",
                $from,
                $until,
                $offset,
            );
        }

        $template = 'CASE ' . implode(' ', $cases) . ' ELSE %1$s END';
        $assignments = [];

        foreach (self::COLUMNS as $column) {
            $assignments[] = $column . ' = ' . sprintf($template, $column);
        }

        return implode(', ', $assignments);
    }

    /**
     * Local wall-clock ranges with their UTC offset, from before the oldest message to a
     * year ahead (processing_after can lie in the future).
     *
     * @return list<array{0: string, 1: string, 2: int}>
     */
    private function periods(): array
    {
        $zone = new DateTimeZone(self::SOURCE_ZONE);
        $oldest = $this->connection->fetchOne('SELECT MIN(created_at) FROM messages');
        $start = ($oldest ? new DateTimeImmutable($oldest, $zone) : new DateTimeImmutable('now', $zone))
            ->modify('-1 day')
            ->getTimestamp();
        $end = (new DateTimeImmutable('+1 year'))->getTimestamp();

        $transitions = $zone->getTransitions($start, $end);
        $periods = [];

        foreach ($transitions as $index => $transition) {
            $offset = $transition['offset'];
            $from = $index === 0 ? '1000-01-01 00:00:00' : gmdate('Y-m-d H:i:s', $transition['ts'] + $offset);
            $next = $transitions[$index + 1] ?? null;
            $until = $next === null ? '9999-12-31 23:59:59' : gmdate('Y-m-d H:i:s', $next['ts'] + $offset);

            $periods[] = [$from, $until, $offset];
        }

        return $periods;
    }

    /**
     * Claim the previous release's per-queue sweep throttle (SET NX EX 60 on
     * queue_throttle:recovery:<queue>) so its pods skip the stale sweep for the rest of
     * the rollout. Best effort: without Redis the migration still runs.
     */
    private function blockPreviousReleaseSweeps(): void
    {
        $url = $_SERVER['REDIS_URL'] ?? $_ENV['REDIS_URL'] ?? getenv('REDIS_URL');

        if (!is_string($url) || $url === '') {
            $this->write('REDIS_URL is not set; the previous release\'s stale sweeps are not paused.');

            return;
        }

        try {
            $redis = new RedisClient($url . (str_contains($url, '?') ? '&' : '?') . 'timeout=2&read_write_timeout=2');
            $queues = $this->connection->fetchFirstColumn('SELECT DISTINCT queue FROM messages');

            foreach ($queues as $queue) {
                $redis->set('queue_throttle:recovery:' . $queue, '1', 'EX', self::SWEEP_BLOCK_SECONDS);
            }

            $this->write(sprintf('Paused the previous release\'s stale sweeps on %d queues.', count($queues)));
        } catch (\Throwable $e) {
            $this->write('Could not pause the previous release\'s stale sweeps: ' . $e->getMessage());
        }
    }
}
