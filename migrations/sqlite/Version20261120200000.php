<?php

declare(strict_types=1);

namespace LessorMQ\Migrations\SQLite;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * SQLite's first migration: the schema as the MariaDB series had made it by 2026-11-20
 * (Version20261120180000), so a SQLite installation starts where MariaDB is.
 *
 * Also switches the database file to write-ahead logging, so readers do not wait for the
 * writer; that lasts for the file. It cannot be done inside a transaction, so this
 * migration runs without one.
 */
final class Version20261120200000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'The schema on SQLite: tenants, api_keys, queues, messages; write-ahead logging';
    }

    public function isTransactional(): bool
    {
        return false;
    }

    public function up(Schema $schema): void
    {
        $this->addSql('PRAGMA journal_mode = WAL');

        $this->addSql(<<<'SQL'
            CREATE TABLE IF NOT EXISTS tenants (
                id VARCHAR(63) NOT NULL,
                name VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL,
                quotas CLOB NOT NULL,
                queue_defaults CLOB NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                suspended_at DATETIME DEFAULT NULL,
                PRIMARY KEY (id)
            )
            SQL);

        $this->addSql(<<<'SQL'
            CREATE TABLE IF NOT EXISTS api_keys (
                id VARCHAR(32) NOT NULL,
                tenant_id VARCHAR(63) DEFAULT NULL,
                name VARCHAR(255) NOT NULL,
                prefix VARCHAR(16) NOT NULL,
                secret_hash CHAR(64) NOT NULL,
                scopes CLOB NOT NULL,
                queues CLOB NOT NULL,
                created_at DATETIME NOT NULL,
                last_used_at DATETIME DEFAULT NULL,
                revoked_at DATETIME DEFAULT NULL,
                PRIMARY KEY (id)
            )
            SQL);
        $this->addSql('CREATE UNIQUE INDEX IF NOT EXISTS uniq_api_keys_secret_hash ON api_keys (secret_hash)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys (tenant_id)');

        $this->addSql(<<<'SQL'
            CREATE TABLE IF NOT EXISTS queues (
                tenant_id VARCHAR(63) NOT NULL,
                name VARCHAR(255) NOT NULL,
                settings CLOB NOT NULL,
                paused_at DATETIME DEFAULT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                PRIMARY KEY (tenant_id, name)
            )
            SQL);

        $this->addSql(<<<'SQL'
            CREATE TABLE IF NOT EXISTS messages (
                id BLOB NOT NULL,
                tenant_id VARCHAR(63) NOT NULL,
                queue VARCHAR(255) NOT NULL,
                body CLOB NOT NULL,
                headers CLOB DEFAULT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                processing_started_at DATETIME DEFAULT NULL,
                processing_after DATETIME DEFAULT NULL,
                processed_at DATETIME DEFAULT NULL,
                failed_at DATETIME DEFAULT NULL,
                retry_count INTEGER NOT NULL,
                max_attempts INTEGER DEFAULT NULL,
                consumed_by VARCHAR(255) DEFAULT NULL,
                receipt VARCHAR(64) DEFAULT NULL,
                lease_until DATETIME DEFAULT NULL,
                route VARCHAR(255) DEFAULT NULL,
                failed_routes CLOB DEFAULT NULL,
                dedup_key VARCHAR(255) DEFAULT NULL,
                dedup_open VARCHAR(255) DEFAULT NULL,
                error_message CLOB DEFAULT NULL,
                error_details CLOB DEFAULT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
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
        $this->throwIrreversibleMigrationException(
            'The first migration is not undone: delete the database file instead.',
        );
    }
}
