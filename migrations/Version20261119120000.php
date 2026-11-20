<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * Tenants and API keys, and a tenant on every message.
 *
 * Every message that exists now belongs to FOS, the only user so far, so the column
 * defaults to 'fos'. The default also keeps pods of the previous release working during
 * the rollout: their inserts do not name a tenant. The new code always writes it.
 *
 * The new indexes lead with tenant_id. The old idx_claim_ordered and idx_stale_recovery
 * stay until a later migration, because the previous release's claim and stale queries
 * use them until its last pod is gone.
 *
 * Timestamps in the new tables are UTC (UtcDateTimeImmutableType).
 */
final class Version20261119120000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add tenants, api_keys and messages.tenant_id; existing messages belong to tenant fos';
    }

    public function up(Schema $schema): void
    {
        $this->addSql(<<<'SQL'
            CREATE TABLE IF NOT EXISTS tenants (
                id VARCHAR(63) NOT NULL,
                name VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL,
                quotas JSON NOT NULL,
                queue_defaults JSON NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                suspended_at DATETIME DEFAULT NULL,
                PRIMARY KEY (id)
            ) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE = InnoDB
            SQL);

        $this->addSql(<<<'SQL'
            CREATE TABLE IF NOT EXISTS api_keys (
                id VARCHAR(32) NOT NULL,
                tenant_id VARCHAR(63) DEFAULT NULL,
                name VARCHAR(255) NOT NULL,
                prefix VARCHAR(16) NOT NULL,
                secret_hash CHAR(64) NOT NULL,
                scopes JSON NOT NULL,
                queues JSON NOT NULL,
                created_at DATETIME NOT NULL,
                last_used_at DATETIME DEFAULT NULL,
                revoked_at DATETIME DEFAULT NULL,
                PRIMARY KEY (id),
                UNIQUE INDEX uniq_api_keys_secret_hash (secret_hash),
                INDEX idx_api_keys_tenant (tenant_id)
            ) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE = InnoDB
            SQL);

        // FOS has run with a fixed 15-minute claim window; keep that as its lease default.
        $this->addSql(<<<'SQL'
            INSERT IGNORE INTO tenants (id, name, status, quotas, queue_defaults, created_at, updated_at)
            VALUES ('fos', 'FOS', 'active', '{}', '{"lease_seconds": 900}', UTC_TIMESTAMP(), UTC_TIMESTAMP())
            SQL);

        $this->addSql("ALTER TABLE messages ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(63) NOT NULL DEFAULT 'fos' AFTER id");
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_tenant_claim ON messages (tenant_id, queue, status, id, processing_after)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_tenant_stale ON messages (tenant_id, queue, status, processing_started_at)');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('DROP INDEX IF EXISTS idx_tenant_stale ON messages');
        $this->addSql('DROP INDEX IF EXISTS idx_tenant_claim ON messages');
        $this->addSql('ALTER TABLE messages DROP COLUMN IF EXISTS tenant_id');
        $this->addSql('DROP TABLE IF EXISTS api_keys');
        $this->addSql('DROP TABLE IF EXISTS tenants');
    }
}
