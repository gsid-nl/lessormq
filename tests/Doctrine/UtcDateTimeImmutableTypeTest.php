<?php

declare(strict_types=1);

namespace App\Tests\Doctrine;

use App\Doctrine\UtcDateTimeImmutableType;
use Doctrine\DBAL\Platforms\MariaDBPlatform;
use PHPUnit\Framework\TestCase;

class UtcDateTimeImmutableTypeTest extends TestCase
{
    public function testStoresUtcWhateverTheZoneOfTheValue(): void
    {
        $type = new UtcDateTimeImmutableType();
        $platform = new MariaDBPlatform();

        // The second 02:30 of the October change in Amsterdam is 01:30 UTC
        $local = new \DateTimeImmutable('2026-10-25 02:30:00 CET');

        $this->assertSame('2026-10-25 01:30:00', $type->convertToDatabaseValue($local, $platform));
        $this->assertNull($type->convertToDatabaseValue(null, $platform));
    }

    public function testReadsAsUtc(): void
    {
        $type = new UtcDateTimeImmutableType();
        $read = $type->convertToPHPValue('2026-10-25 01:30:00', new MariaDBPlatform());

        $this->assertSame('UTC', $read->getTimezone()->getName());
        $this->assertSame(strtotime('2026-10-25 01:30:00 UTC'), $read->getTimestamp());
    }
}
