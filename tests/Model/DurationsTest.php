<?php

declare(strict_types=1);

namespace App\Tests\Model;

use App\Model\Durations;
use PHPUnit\Framework\TestCase;

final class DurationsTest extends TestCase
{
    public function testNothingToSayWithoutDurations(): void
    {
        self::assertNull((new Durations())->view());
    }

    public function testAverageAndMaximumAreExact(): void
    {
        $durations = new Durations();
        // 10 runs of 0 s and 10 of 1 s (classes 0 and 1), one of 40 s (class 5).
        $durations->add(0, 10, 0, 0);
        $durations->add(1, 10, 10, 1);
        $durations->add(5, 1, 40, 40);

        $view = $durations->view();

        self::assertSame(round(50 / 21, 2), $view['avg']);
        self::assertSame(40, $view['max']);
    }

    public function testPercentilesAreInterpolatedWithinTheirClass(): void
    {
        $durations = new Durations();
        // 100 waits between 10 and 30 s (class 4) with a longest of 29 s.
        $durations->add(4, 100, 2000, 29);

        self::assertSame(20.0, $durations->percentile(0.5));
        // 10 + 20 * 0.95 = 29, the longest.
        self::assertSame(29.0, $durations->percentile(0.95));
    }

    public function testAPercentileIsNeverAboveTheLongest(): void
    {
        $durations = new Durations();
        $durations->add(0, 50, 0, 0);

        self::assertSame(['avg' => 0.0, 'p50' => 0.0, 'p95' => 0.0, 'max' => 0], $durations->view());
    }

    public function testTheLastClassReachesToTheLongest(): void
    {
        $durations = new Durations();
        $last = count(Durations::BOUNDS);
        $durations->add($last, 2, 200000, 100000 + 86400);

        self::assertSame(86400.0 + 100000 / 2, $durations->percentile(0.5));
    }

    public function testMergedDurationsCountAsOne(): void
    {
        $a = new Durations();
        $a->add(0, 90, 0, 0);
        $b = new Durations();
        $b->add(6, 10, 900, 100);
        $a->merge($b);

        self::assertSame(100, $a->count());
        self::assertSame(9.0, $a->view()['avg']);
        // The 95th of 100 is the 5th of the 10 in class 6 (60 to 120 s).
        self::assertSame(90.0, $a->percentile(0.95));
        self::assertSame(100, $a->view()['max']);
    }

    public function testTheClassesInSql(): void
    {
        $sql = Durations::classSql('run');

        self::assertStringStartsWith('CASE WHEN run < 1 THEN 0 WHEN run < 2 THEN 1', $sql);
        self::assertStringEndsWith('WHEN run < 86400 THEN 13 ELSE 14 END', $sql);
    }
}
