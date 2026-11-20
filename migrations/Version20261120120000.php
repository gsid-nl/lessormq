<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

/**
 * Routes: the path an attempt ran through (a VPN exit, a proxy, an API account) and,
 * per message, when an attempt last failed on each route. With route_policy avoid_failed
 * a claim skips a message that failed on the claimer's route within the cooldown.
 *
 * This is the general form of FOS's VPN restriction (restrictByVpn + vpn_id, recorded in
 * consumed_by_vpn). FOS gets avoid_failed as its default, and the VPNs its open messages
 * were already tried on carry over as failed routes, so no message goes back through a
 * VPN it was refused on because of the upgrade. consumed_by_vpn is no longer written.
 */
final class Version20261120120000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Add messages.route and failed_routes; FOS avoids failed routes; carry over consumed_by_vpn';
    }

    public function up(Schema $schema): void
    {
        $this->addSql(<<<'SQL'
            ALTER TABLE messages
                ADD COLUMN IF NOT EXISTS route VARCHAR(255) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS failed_routes JSON DEFAULT NULL,
                ALGORITHM=INSTANT
            SQL);

        $this->addSql(<<<'SQL'
            UPDATE tenants
            SET queue_defaults = JSON_SET(queue_defaults, '$.route_policy', JSON_OBJECT('mode', 'avoid_failed'))
            WHERE id = 'fos'
              AND JSON_EXTRACT(queue_defaults, '$.route_policy') IS NULL
            SQL);
    }

    /**
     * The VPN history of open messages becomes failed routes, timed now so the cooldown
     * starts at the upgrade. Few rows: only pending and processing messages matter.
     */
    public function postUp(Schema $schema): void
    {
        $rows = $this->connection->fetchAllAssociative(<<<'SQL'
            SELECT id, consumed_by_vpn
            FROM messages
            WHERE status IN ('pending', 'processing')
              AND failed_routes IS NULL
              AND consumed_by_vpn IS NOT NULL
              AND consumed_by_vpn NOT IN ('[]', 'null')
            SQL);
        $now = gmdate('Y-m-d H:i:s');
        $carried = 0;

        foreach ($rows as $row) {
            $vpns = json_decode((string) $row['consumed_by_vpn'], true);

            if (!is_array($vpns) || $vpns === []) {
                continue;
            }

            $routes = [];

            foreach ($vpns as $vpn) {
                if (is_string($vpn) && $vpn !== '') {
                    $routes[$vpn] = $now;
                }
            }

            $this->connection->executeStatement(
                'UPDATE messages SET failed_routes = ? WHERE id = ? AND failed_routes IS NULL',
                [json_encode($routes), $row['id']],
            );
            $carried++;
        }

        $this->write(sprintf('Carried the VPN history of %d open messages over as failed routes.', $carried));
    }

    public function down(Schema $schema): void
    {
        $this->addSql(<<<'SQL'
            ALTER TABLE messages
                DROP COLUMN IF EXISTS failed_routes,
                DROP COLUMN IF EXISTS route
            SQL);
    }
}
