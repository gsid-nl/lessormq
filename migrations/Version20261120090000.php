<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * Deduplication by a key the publisher chooses, enforced by the database: at most one
 * open (pending or processing) message per tenant, queue and dedup_key.
 *
 * dedup_open holds the key while the message is open and is cleared when it completes
 * or is dead-lettered (and set again on replay), so a unique index on it lets a
 * finished message's key be used again. Keys compare byte for byte (utf8mb4_bin):
 * "Order-1" and "order-1" are different keys.
 *
 * dedup_open is a plain column kept by the application, not a generated one: an index on
 * a virtual column stops MariaDB from adding any later column to messages online, and
 * this table is too busy for a locking rebuild. (Changed before this ever ran outside a
 * test container, with Guido's consent, 2026-11-20.)
 *
 * Each step names its algorithm so that a server that cannot do it online fails the
 * migration (and the rollout stops) instead of locking the table: both columns are
 * instant, and the index builds in place while publishes and claims go on.
 */
final class Version20261120090000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add messages.dedup_key and dedup_open with a unique index over open messages';
    }

    public function up(Schema $schema): void
    {
        $this->addSql(<<<'SQL'
            ALTER TABLE messages
                ADD COLUMN IF NOT EXISTS dedup_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS dedup_open VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
                ALGORITHM=INSTANT
            SQL);
        $this->addSql(
            'CREATE UNIQUE INDEX IF NOT EXISTS uniq_open_dedup ON messages (tenant_id, queue, dedup_open) '
            . 'ALGORITHM=INPLACE LOCK=NONE'
        );
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP INDEX IF EXISTS uniq_open_dedup ON messages');
        $this->addSql('ALTER TABLE messages DROP COLUMN IF EXISTS dedup_open');
        $this->addSql('ALTER TABLE messages DROP COLUMN IF EXISTS dedup_key');
    }
}
