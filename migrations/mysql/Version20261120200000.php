<?php

declare(strict_types=1);

namespace LessorMQ\Migrations\MySQL;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * MySQL's first migration: the schema as the MariaDB series had made it by 2026-11-20
 * (Version20261120180000), so a MySQL installation starts where MariaDB is.
 *
 * JSON is kept as text, as on MariaDB, so a body comes back exactly as it was published
 * (MySQL's JSON type rewrites it). Dedup keys compare byte for byte.
 */
final class Version20261120200000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'The schema on MySQL: tenants, api_keys, queues, messages';
    }

    public function up(Schema $schema): void
    {
        $table = 'DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE = InnoDB';
        $json = 'LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin';

        $this->addSql(<<<SQL
            CREATE TABLE IF NOT EXISTS tenants (
                id VARCHAR(63) NOT NULL,
                name VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL,
                quotas {$json} NOT NULL CHECK (JSON_VALID(quotas)),
                queue_defaults {$json} NOT NULL CHECK (JSON_VALID(queue_defaults)),
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                suspended_at DATETIME DEFAULT NULL,
                PRIMARY KEY (id)
            ) {$table}
            SQL);

        $this->addSql(<<<SQL
            CREATE TABLE IF NOT EXISTS api_keys (
                id VARCHAR(32) NOT NULL,
                tenant_id VARCHAR(63) DEFAULT NULL,
                name VARCHAR(255) NOT NULL,
                prefix VARCHAR(16) NOT NULL,
                secret_hash CHAR(64) NOT NULL,
                scopes {$json} NOT NULL CHECK (JSON_VALID(scopes)),
                queues {$json} NOT NULL CHECK (JSON_VALID(queues)),
                created_at DATETIME NOT NULL,
                last_used_at DATETIME DEFAULT NULL,
                revoked_at DATETIME DEFAULT NULL,
                PRIMARY KEY (id),
                UNIQUE INDEX uniq_api_keys_secret_hash (secret_hash),
                INDEX idx_api_keys_tenant (tenant_id)
            ) {$table}
            SQL);

        $this->addSql(<<<SQL
            CREATE TABLE IF NOT EXISTS queues (
                tenant_id VARCHAR(63) NOT NULL,
                name VARCHAR(255) NOT NULL,
                settings {$json} NOT NULL CHECK (JSON_VALID(settings)),
                paused_at DATETIME DEFAULT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                PRIMARY KEY (tenant_id, name)
            ) {$table}
            SQL);

        $this->addSql(<<<SQL
            CREATE TABLE IF NOT EXISTS messages (
                id BINARY(16) NOT NULL,
                tenant_id VARCHAR(63) NOT NULL,
                queue VARCHAR(255) NOT NULL,
                body {$json} NOT NULL CHECK (JSON_VALID(body)),
                headers {$json} DEFAULT NULL CHECK (JSON_VALID(headers)),
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                processing_started_at DATETIME DEFAULT NULL,
                processing_after DATETIME DEFAULT NULL,
                processed_at DATETIME DEFAULT NULL,
                failed_at DATETIME DEFAULT NULL,
                retry_count INT NOT NULL,
                max_attempts INT DEFAULT NULL,
                consumed_by VARCHAR(255) DEFAULT NULL,
                receipt VARCHAR(64) DEFAULT NULL,
                lease_until DATETIME DEFAULT NULL,
                route VARCHAR(255) DEFAULT NULL,
                failed_routes {$json} DEFAULT NULL CHECK (JSON_VALID(failed_routes)),
                dedup_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
                dedup_open VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
                error_message LONGTEXT DEFAULT NULL,
                error_details LONGTEXT DEFAULT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                PRIMARY KEY (id),
                UNIQUE INDEX uniq_open_dedup (tenant_id, queue, dedup_open),
                INDEX idx_tenant_claim (tenant_id, queue, status, id, processing_after),
                INDEX idx_tenant_stale (tenant_id, queue, status, processing_started_at),
                INDEX idx_tenant_purge (tenant_id, queue, status, updated_at)
            ) {$table}
            SQL);
    }

    public function down(Schema $schema): void
    {
        $this->throwIrreversibleMigrationException('The first migration is not undone: drop the database instead.');
    }
}
