<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * Auto-generated Migration: Please modify to your needs!
 */
final class Version20260319151724 extends AbstractMigration
{
    public function getDescription(): string
    {
        return '';
    }

    public function up(Schema $schema): void
    {
        // this up() migration is auto-generated, please modify it to your needs
        $this->addSql('CREATE TABLE messages (id BINARY(16) NOT NULL COMMENT \'(DC2Type:uuid_binary_ordered_time)\', queue VARCHAR(255) NOT NULL, body JSON NOT NULL COMMENT \'(DC2Type:json)\', status VARCHAR(20) DEFAULT \'pending\' NOT NULL, processing_started_at DATETIME DEFAULT NULL, processing_after DATETIME DEFAULT NULL, processed_at DATETIME DEFAULT NULL, failed_at DATETIME DEFAULT NULL, retry_count INT NOT NULL, consumed_by VARCHAR(255) DEFAULT NULL, error_message LONGTEXT DEFAULT NULL, error_details LONGTEXT DEFAULT NULL, consumed_by_vpn JSON DEFAULT NULL COMMENT \'(DC2Type:json)\', created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL, INDEX idx_claim (queue, status, processing_after, id), INDEX idx_consumed_by (consumed_by), INDEX idx_stale_recovery (queue, status, processing_started_at), PRIMARY KEY(id)) DEFAULT CHARACTER SET utf8mb4 COLLATE `utf8mb4_unicode_ci` ENGINE = InnoDB');
    }

    public function down(Schema $schema): void
    {
        // this down() migration is auto-generated, please modify it to your needs
        $this->addSql('DROP TABLE messages');
    }
}
