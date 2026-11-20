<?php

declare(strict_types=1);

namespace App\Tests\Model;

use App\Model\QueueSettings;
use App\Model\Quotas;
use PHPUnit\Framework\TestCase;

class QueueSettingsTest extends TestCase
{
    public function testResolveMergesNestedPatchesOverTheDefaults(): void
    {
        $settings = QueueSettings::resolve(
            ['lease_seconds' => 900, 'backoff' => ['initial_seconds' => 5]],
            ['backoff' => ['max_seconds' => 60]],
        );

        $this->assertSame(900, $settings['lease_seconds']);
        $this->assertSame(5, $settings['backoff']['initial_seconds']);
        $this->assertSame(60, $settings['backoff']['max_seconds']);
        // Untouched siblings keep their defaults
        $this->assertSame(2, $settings['backoff']['multiplier']);
        $this->assertSame('none', $settings['route_policy']['mode']);
    }

    public function testValidPatchHasNoErrors(): void
    {
        $this->assertSame([], QueueSettings::validatePatch([
            'lease_seconds' => 60,
            'backoff' => ['jitter' => 0, 'multiplier' => 1.5],
            'route_policy' => ['mode' => 'avoid_failed', 'cooldown_seconds' => 600],
            'on_exhausted' => 'discard',
            'paused' => true,
        ]));
        $this->assertSame([], QueueSettings::validatePatch([]));
    }

    public function testReportsEveryInvalidFieldWithItsPath(): void
    {
        $errors = QueueSettings::validatePatch([
            'lease_seconds' => 0,
            'backoff' => ['jitter' => 2, 'colour' => 'red'],
            'on_permanent' => 'ignore',
            'route_policy' => 'avoid_failed',
            'paused' => 'yes',
            'speed' => 11,
        ], prefix: 'queue_defaults.');

        $fields = array_column($errors, 'field');
        sort($fields);

        $this->assertSame([
            'queue_defaults.backoff.colour',
            'queue_defaults.backoff.jitter',
            'queue_defaults.lease_seconds',
            'queue_defaults.on_permanent',
            'queue_defaults.paused',
            'queue_defaults.route_policy',
            'queue_defaults.speed',
        ], $fields);
    }

    public function testChecksRelationsAgainstTheSettingsThePatchLandsOn(): void
    {
        $errors = QueueSettings::validatePatch(['lease_seconds' => 4000]);
        $this->assertSame([['field' => 'lease_seconds', 'message' => 'must not exceed max_lease_seconds']], $errors);

        $this->assertSame([], QueueSettings::validatePatch(['lease_seconds' => 4000, 'max_lease_seconds' => 7200]));

        $base = QueueSettings::resolve(['max_attempts_limit' => 10]);
        $errors = QueueSettings::validatePatch(['max_attempts' => 12], $base);
        $this->assertSame('max_attempts', $errors[0]['field']);
    }

    public function testRejectsAListWhereAnObjectBelongs(): void
    {
        $this->assertSame('(body)', QueueSettings::validatePatch([1, 2])[0]['field']);
    }

    public function testQuotasAcceptNullAsUnlimited(): void
    {
        $this->assertSame([], Quotas::validate(['max_queues' => null, 'max_open_messages' => 1000]));

        $errors = Quotas::validate(['max_queues' => 0, 'parking_spaces' => 3]);
        $this->assertSame(['quotas.max_queues', 'quotas.parking_spaces'], array_column($errors, 'field'));

        $resolved = Quotas::resolve(['max_queues' => 5]);
        $this->assertSame(5, $resolved['max_queues']);
        $this->assertNull($resolved['claim_per_second']);
        $this->assertSame(array_keys(Quotas::FIELDS), array_keys($resolved));
    }
}
