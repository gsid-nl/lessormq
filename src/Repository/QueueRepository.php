<?php

declare(strict_types=1);

namespace App\Repository;

use App\Database\Dialect;
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
    private const COLUMNS = ['tenant_id', 'name', 'settings', 'paused_at', 'created_at', 'updated_at'];
    private const KEY = ['tenant_id', 'name'];

    public function __construct(private readonly Connection $connection, private readonly Dialect $dialect)
    {
    }

    /**
     * Everything needed to resolve one queue's settings, in one query.
     *
     * @return array{
     *     defaults: array<string, mixed>, settings: array<string, mixed>, quotas: array<string, int|null>,
     *     paused_at: ?string, exists: bool
     * }
     */
    public function load(string $tenant, string $queue): array
    {
        $row = $this->connection->fetchAssociative(
            <<<'SQL'
                SELECT t.queue_defaults, t.quotas, q.settings, q.paused_at, q.name
                FROM tenants t
                LEFT JOIN queues q ON q.tenant_id = t.id AND q.name = :queue
                WHERE t.id = :tenant
                SQL,
            ['tenant' => $tenant, 'queue' => $queue],
        );

        if ($row === false) {
            return ['defaults' => [], 'settings' => [], 'quotas' => [], 'paused_at' => null, 'exists' => false];
        }

        return [
            'defaults' => json_decode((string) $row['queue_defaults'], true) ?: [],
            'quotas' => json_decode((string) $row['quotas'], true) ?: [],
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
            $this->dialect->insertOrIgnore('queues', self::COLUMNS, self::KEY),
            [
                'tenant_id' => $tenant,
                'name' => $queue,
                'settings' => '{}',
                'paused_at' => $pausedAt,
                'created_at' => $now,
                'updated_at' => $now,
            ],
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
            $this->dialect->upsert('queues', self::COLUMNS, self::KEY, ['settings', 'paused_at', 'updated_at']),
            [
                'tenant_id' => $tenant,
                'name' => $queue,
                'settings' => json_encode((object) $settings),
                'paused_at' => $pausedAt,
                'created_at' => $now,
                'updated_at' => $now,
            ],
        );
    }

    public function count(string $tenant): int
    {
        return (int) $this->connection->fetchOne('SELECT COUNT(*) FROM queues WHERE tenant_id = ?', [$tenant]);
    }

    public function deleteForTenant(string $tenant): int
    {
        return (int) $this->connection->executeStatement('DELETE FROM queues WHERE tenant_id = ?', [$tenant]);
    }
}
