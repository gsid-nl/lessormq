<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * Index tuning for the claim and purge hot paths.
 *
 * The claim query filters on (queue, status) and orders by id, with only a residual
 * predicate on processing_after. idx_claim put processing_after ahead of id, so the
 * ORDER BY could not be served by the index and MariaDB sorted the entire pending
 * backlog to return one row. idx_claim_ordered leads with (queue, status, id) so the
 * scan stops at the first matching row, and carries processing_after so the residual
 * predicate needs no row lookup.
 */
final class Version20260723090000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add covering claim index and purge index; drop superseded idx_claim';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_claim_ordered ON messages (queue, status, id, processing_after)');
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_purge ON messages (status, updated_at)');
        $this->addSql('DROP INDEX IF EXISTS idx_claim ON messages');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_claim ON messages (queue, status, processing_after, id)');
        $this->addSql('DROP INDEX IF EXISTS idx_purge ON messages');
        $this->addSql('DROP INDEX IF EXISTS idx_claim_ordered ON messages');
    }
}
