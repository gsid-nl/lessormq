<?php

declare(strict_types=1);

namespace App\Repository;

use App\Time\Utc;
use Doctrine\DBAL\Connection;

/**
 * The queues table: per queue, the settings it overrides and whether it is paused.
 *
 * Plain DBAL rather than an entity: it is read on the claim path (through
 * QueueSettingsService's cache) and written by a handful of admin calls.
 */
class QueueRepository
{
    public function __construct(private readonly Connection $connection)
    {
    }

    /**
     * Everything needed to resolve one queue's settings, in one query.
     *
     * @return array{defaults: array<string, mixed>, settings: array<string, mixed>, paused_at: ?string, exists: bool}
     */
    public function load(string $tenant, string $queue): array
    {
        $row = $this->connection->fetchAssociative(
            <<<'SQL'
                SELECT t.queue_defaults, q.settings, q.paused_at, q.name
                FROM tenants t
                LEFT JOIN queues q ON q.tenant_id = t.id AND q.name = :queue
                WHERE t.id = :tenant
                SQL,
            ['tenant' => $tenant, 'queue' => $queue],
        );

        if ($row === false) {
            return ['defaults' => [], 'settings' => [], 'paused_at' => null, 'exists' => false];
        }

        return [
            'defaults' => json_decode((string) $row['queue_defaults'], true) ?: [],
            'settings' => $row['settings'] === null ? [] : (json_decode((string) $row['settings'], true) ?: []),
            'paused_at' => $row['paused_at'],
            'exists' => $row['name'] !== null,
        ];
    }

    /**
     * @return list<string> the tenant's queue names
     */
    public function names(string $tenant): array
    {
        return $this->connection->fetchFirstColumn(
            'SELECT name FROM queues WHERE tenant_id = ? ORDER BY name',
            [$tenant],
        );
    }

    public function createIfMissing(string $tenant, string $queue, ?string $pausedAt): void
    {
        $now = Utc::sql(Utc::now());

        $this->connection->executeStatement(
            <<<'SQL'
                INSERT IGNORE INTO queues (tenant_id, name, settings, paused_at, created_at, updated_at)
                VALUES (?, ?, '{}', ?, ?, ?)
                SQL,
            [$tenant, $queue, $pausedAt, $now, $now],
        );
    }

    /**
     * Create or replace a queue's own settings and pause state.
     *
     * @param array<string, mixed> $settings the queue's overrides, not the resolved settings
     */
    public function save(string $tenant, string $queue, array $settings, ?string $pausedAt): void
    {
        $now = Utc::sql(Utc::now());

        $this->connection->executeStatement(
            <<<'SQL'
                INSERT INTO queues (tenant_id, name, settings, paused_at, created_at, updated_at)
                VALUES (:tenant, :queue, :settings, :paused_at, :now, :now)
                ON DUPLICATE KEY UPDATE settings = VALUES(settings),
                                        paused_at = VALUES(paused_at),
                                        updated_at = VALUES(updated_at)
                SQL,
            [
                'tenant' => $tenant,
                'queue' => $queue,
                'settings' => json_encode((object) $settings),
                'paused_at' => $pausedAt,
                'now' => $now,
            ],
        );
    }

    public function deleteForTenant(string $tenant): int
    {
        return (int) $this->connection->executeStatement('DELETE FROM queues WHERE tenant_id = ?', [$tenant]);
    }
}
