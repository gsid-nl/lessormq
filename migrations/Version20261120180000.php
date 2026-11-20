<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * The end of the pre-v1 API, which only kept FOS's workers running during an upgrade in
 * place. FOS moved to a new installation on v1 instead, so its compatibility goes:
 *
 * - consumed_by_vpn, the pre-v1 VPN history (routes replaced it; no longer written);
 * - the default 'fos' on messages.tenant_id, which let pre-v1 inserts skip the tenant;
 * - the indexes from before tenants, which no query uses since every query names one;
 * - tenant 'fos', which the earlier migrations create on every installation, when it is
 *   still empty: no keys, no queues, no messages. Where FOS does use it, it stays.
 *
 * All online: dropping a column or a default is instant, dropping an index is metadata.
 */
final class Version20261120180000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Drop the pre-v1 compatibility: consumed_by_vpn, the fos default, old indexes, an empty fos tenant';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('ALTER TABLE messages DROP COLUMN IF EXISTS consumed_by_vpn, ALGORITHM=INSTANT');
        $this->addSql('ALTER TABLE messages ALTER COLUMN tenant_id DROP DEFAULT');
        $this->addSql('DROP INDEX IF EXISTS idx_claim_ordered ON messages');
        $this->addSql('DROP INDEX IF EXISTS idx_stale_recovery ON messages');
        $this->addSql('DROP INDEX IF EXISTS idx_purge ON messages');
        $this->addSql(<<<'SQL'
            DELETE FROM tenants
            WHERE id = 'fos'
              AND NOT EXISTS (SELECT 1 FROM api_keys WHERE tenant_id = 'fos')
              AND NOT EXISTS (SELECT 1 FROM queues WHERE tenant_id = 'fos')
              AND NOT EXISTS (SELECT 1 FROM messages WHERE tenant_id = 'fos')
            SQL);
    }

    /**
     * The schema half only: a deleted tenant is not brought back.
     */
    public function down(Schema $schema): void
    {
        $this->addSql('ALTER TABLE messages ADD COLUMN IF NOT EXISTS consumed_by_vpn JSON DEFAULT NULL, ALGORITHM=INSTANT');
        $this->addSql("ALTER TABLE messages ALTER COLUMN tenant_id SET DEFAULT 'fos'");
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_claim_ordered ON messages (queue, status, id, processing_after)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_stale_recovery ON messages (queue, status, processing_started_at)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_purge ON messages (status, updated_at)');
    }

    public function isTransactional(): bool
    {
        return false;
    }
}
