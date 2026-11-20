<?php

declare(strict_types=1);

namespace App\Tests\Time;

use App\Time\Utc;
use PHPUnit\Framework\TestCase;

class UtcTest extends TestCase
{
    public function testSqlLiteralIsUtcWhateverTheZoneOfTheValue(): void
    {
        $summer = new \DateTimeImmutable('2026-07-01 12:00:00', new \DateTimeZone('Europe/Amsterdam'));
        $winter = new \DateTime('2026-11-19 12:00:00', new \DateTimeZone('Europe/Amsterdam'));

        $this->assertSame('2026-07-01 10:00:00', Utc::sql($summer));
        $this->assertSame('2026-11-19 11:00:00', Utc::sql($winter));
    }

    public function testNowAndParseAreUtc(): void
    {
        $this->assertSame('UTC', Utc::now()->getTimezone()->getName());
        $this->assertSame(strtotime('2026-10-25 01:30:00 UTC'), Utc::parse('2026-10-25 01:30:00')->getTimestamp());
    }

    public function testKernelMakesUtcTheDefaultZone(): void
    {
        date_default_timezone_set('Europe/Amsterdam');

        try {
            $kernel = new \App\Kernel('test', false);
            $kernel->boot();
            $this->assertSame('UTC', date_default_timezone_get());
            $kernel->shutdown();
        } finally {
            date_default_timezone_set('UTC');
        }
    }
}
