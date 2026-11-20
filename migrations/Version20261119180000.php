<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * Queue settings on the server: a queues table with each queue's own overrides and its
 * pause state, a row for every queue that has messages, and a purge index per queue.
 *
 * Retention becomes a queue setting with a 7-day default for failed messages. FOS has
 * always purged failed messages after 24 hours like completed ones, and keeps that until
 * someone decides otherwise (PATCH /v1/tenants/fos).
 *
 * Pause state still in Redis (queue_paused:<queue>) is honoured at runtime and moves
 * into this table the first time the queue is paused, resumed or changed.
 */
final class Version20261119180000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add queues (settings and pause state per queue) and idx_tenant_purge; FOS keeps 24 h retention';
    }

    public function up(Schema $schema): void
    {
        $this->addSql(<<<'SQL'
            CREATE TABLE IF NOT EXISTS queues (
                tenant_id VARCHAR(63) NOT NULL,
                name VARCHAR(255) NOT NULL,
                settings JSON NOT NULL,
                paused_at DATETIME DEFAULT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                PRIMARY KEY (tenant_id, name)
            ) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE = InnoDB
            SQL);

        $this->addSql(<<<'SQL'
            INSERT IGNORE INTO queues (tenant_id, name, settings, paused_at, created_at, updated_at)
            SELECT tenant_id, queue, '{}', NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()
            FROM messages
            GROUP BY tenant_id, queue
            SQL);

        $this->addSql(<<<'SQL'
            UPDATE tenants
            SET queue_defaults = JSON_SET(
                queue_defaults,
                '$.retention',
                JSON_OBJECT('completed_seconds', 86400, 'failed_seconds', 86400)
            )
            WHERE id = 'fos'
              AND JSON_EXTRACT(queue_defaults, '$.retention') IS NULL
            SQL);

        $this->addSql('CREATE INDEX IF NOT EXISTS idx_tenant_purge ON messages (tenant_id, queue, status, updated_at)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP INDEX IF EXISTS idx_tenant_purge ON messages');
        $this->addSql('DROP TABLE IF EXISTS queues');
    }
}
