<?php

declare(strict_types=1);

namespace App\Tests\Model;

use App\Model\QueueSettings;
use App\Model\Quotas;
use PHPUnit\Framework\TestCase;

final class QuotasTest extends TestCase
{
    public function testCapsTheMessageSizeAndRetentionOfAQueue(): void
    {
        $quotas = Quotas::resolve(['max_message_bytes' => 1000, 'max_retention_seconds' => 3600]);
        $settings = Quotas::cap(QueueSettings::DEFAULTS, $quotas);

        self::assertSame(1000, $settings['max_message_bytes']);
        self::assertSame(3600, $settings['retention']['completed_seconds']);
        self::assertSame(3600, $settings['retention']['failed_seconds']);
        // Below the cap stays as it is
        $small = Quotas::cap(['max_message_bytes' => 10] + QueueSettings::DEFAULTS, $quotas);
        self::assertSame(10, $small['max_message_bytes']);
    }

    public function testWithoutQuotasNothingChanges(): void
    {
        self::assertSame(QueueSettings::DEFAULTS, Quotas::cap(QueueSettings::DEFAULTS, Quotas::resolve([])));
    }

    public function testRefusesSettingsAboveTheQuotas(): void
    {
        $quotas = Quotas::resolve(['max_message_bytes' => 1000, 'max_retention_seconds' => 3600]);

        self::assertSame([], Quotas::settingErrors(['max_message_bytes' => 1000], $quotas));
        self::assertSame(
            ['queue_defaults.max_message_bytes', 'queue_defaults.retention.failed_seconds'],
            array_column(Quotas::settingErrors(
                ['max_message_bytes' => 1001, 'retention' => ['completed_seconds' => 60, 'failed_seconds' => 7200]],
                $quotas,
                'queue_defaults.',
            ), 'field'),
        );
    }
}
