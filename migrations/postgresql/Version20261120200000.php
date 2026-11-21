<?php

declare(strict_types=1);

namespace LessorMQ\Migrations\PostgreSQL;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * PostgreSQL's first migration: the schema as the MariaDB series had made it by
 * 2026-11-20 (Version20261120180000), so a PostgreSQL installation starts where MariaDB is.
 *
 * JSON columns are json rather than jsonb, which keeps a body exactly as it was published.
 * NULLs are distinct in a unique index, so uniq_open_dedup holds only open messages, as on
 * MariaDB: dedup_open is NULL once a message is finished.
 */
final class Version20261120200000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'The schema on PostgreSQL: tenants, api_keys, queues, messages';
    }

    public function up(Schema $schema): void
    {
        $time = 'TIMESTAMP(0) WITHOUT TIME ZONE';

        $this->addSql(<<<SQL
            CREATE TABLE IF NOT EXISTS tenants (
                id VARCHAR(63) NOT NULL,
                name VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL,
                quotas JSON NOT NULL,
                queue_defaults JSON NOT NULL,
                created_at {$time} NOT NULL,
                updated_at {$time} NOT NULL,
                suspended_at {$time} DEFAULT NULL,
                PRIMARY KEY (id)
            )
            SQL);

        $this->addSql(<<<SQL
            CREATE TABLE IF NOT EXISTS api_keys (
                id VARCHAR(32) NOT NULL,
                tenant_id VARCHAR(63) DEFAULT NULL,
                name VARCHAR(255) NOT NULL,
                prefix VARCHAR(16) NOT NULL,
                secret_hash CHAR(64) NOT NULL,
                scopes JSON NOT NULL,
                queues JSON NOT NULL,
                created_at {$time} NOT NULL,
                last_used_at {$time} DEFAULT NULL,
                revoked_at {$time} DEFAULT NULL,
                PRIMARY KEY (id)
            )
            SQL);
        $this->addSql('CREATE UNIQUE INDEX IF NOT EXISTS uniq_api_keys_secret_hash ON api_keys (secret_hash)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys (tenant_id)');

        $this->addSql(<<<SQL
            CREATE TABLE IF NOT EXISTS queues (
                tenant_id VARCHAR(63) NOT NULL,
                name VARCHAR(255) NOT NULL,
                settings JSON NOT NULL,
                paused_at {$time} DEFAULT NULL,
                created_at {$time} NOT NULL,
                updated_at {$time} NOT NULL,
                PRIMARY KEY (tenant_id, name)
            )
            SQL);

        $this->addSql(<<<SQL
            CREATE TABLE IF NOT EXISTS messages (
                id BYTEA NOT NULL,
                tenant_id VARCHAR(63) NOT NULL,
                queue VARCHAR(255) NOT NULL,
                body JSON NOT NULL,
                headers JSON DEFAULT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                processing_started_at {$time} DEFAULT NULL,
                processing_after {$time} DEFAULT NULL,
                processed_at {$time} DEFAULT NULL,
                failed_at {$time} DEFAULT NULL,
                retry_count INT NOT NULL,
                max_attempts INT DEFAULT NULL,
                consumed_by VARCHAR(255) DEFAULT NULL,
                receipt VARCHAR(64) DEFAULT NULL,
                lease_until {$time} DEFAULT NULL,
                route VARCHAR(255) DEFAULT NULL,
                failed_routes JSON DEFAULT NULL,
                dedup_key VARCHAR(255) DEFAULT NULL,
                dedup_open VARCHAR(255) DEFAULT NULL,
                error_message TEXT DEFAULT NULL,
                error_details TEXT DEFAULT NULL,
                created_at {$time} NOT NULL,
                updated_at {$time} NOT NULL,
                PRIMARY KEY (id)
            )
            SQL);
        $indexes = [
            'UNIQUE INDEX IF NOT EXISTS uniq_open_dedup' => 'tenant_id, queue, dedup_open',
            'INDEX IF NOT EXISTS idx_tenant_claim' => 'tenant_id, queue, status, id, processing_after',
            'INDEX IF NOT EXISTS idx_tenant_stale' => 'tenant_id, queue, status, processing_started_at',
            'INDEX IF NOT EXISTS idx_tenant_purge' => 'tenant_id, queue, status, updated_at',
        ];

        foreach ($indexes as $index => $columns) {
            $this->addSql("CREATE {$index} ON messages ({$columns})");
        }
    }

    public function down(Schema $schema): void
    {
        $this->throwIrreversibleMigrationException('The first migration is not undone: drop the database instead.');
    }
}
