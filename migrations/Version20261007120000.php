<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * idx_consumed_by only served the post-claim fetch, which now loads the claimed row by
 * primary key. Every claim and every retry rewrote its entry for no reader.
 */
final class Version20261007120000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Drop idx_consumed_by: no query reads it any more';
    }

    public function up(Schema $schema): void
    {
        $this->addSql('DROP INDEX IF EXISTS idx_consumed_by ON messages');
    }

    public function down(Schema $schema): void
    {
        $this->addSql('CREATE INDEX IF NOT EXISTS idx_consumed_by ON messages (consumed_by)');
    }
}
