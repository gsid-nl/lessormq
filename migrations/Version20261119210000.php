<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * Leases: every claim gets a receipt and a lease_until, and only the holder of the
 * current receipt may extend, ack, fail or release the message (v1).
 *
 * Also the two v1 publish fields a message keeps: its headers, and the max_attempts the
 * publisher asked for (null means the queue's setting).
 *
 * Messages processing when this runs were claimed by the previous release, which gave
 * every claim 900 seconds; their lease_until says so. Claims made by its pods during the
 * rollout have no lease_until and are expired by processing_started_at instead, until
 * the post-rollout cleanup removes that fallback.
 */
final class Version20261119210000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add messages.receipt, lease_until, headers and max_attempts';
    }

    public function up(Schema $schema): void
    {
        $this->addSql(<<<'SQL'
            ALTER TABLE messages
                ADD COLUMN IF NOT EXISTS receipt VARCHAR(64) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS lease_until DATETIME DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS headers JSON DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS max_attempts INT DEFAULT NULL
            SQL);

        $this->addSql(<<<'SQL'
            UPDATE messages
            SET lease_until = processing_started_at + INTERVAL 900 SECOND
            WHERE status = 'processing'
              AND lease_until IS NULL
              AND processing_started_at IS NOT NULL
            SQL);
    }

    public function down(Schema $schema): void
    {
        $this->addSql(<<<'SQL'
            ALTER TABLE messages
                DROP COLUMN IF EXISTS max_attempts,
                DROP COLUMN IF EXISTS headers,
                DROP COLUMN IF EXISTS lease_until,
                DROP COLUMN IF EXISTS receipt
            SQL);
    }
}
