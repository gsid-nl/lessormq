<?php

declare(strict_types=1);

namespace App\Time;

use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;

/**
 * Every stored and compared time is UTC. Kernel::boot() also makes UTC the default
 * zone, but the write paths name it explicitly so a stray default can never shift a
 * lease or a retry by an hour.
 */
final class Utc
{
    public const SQL_FORMAT = 'Y-m-d H:i:s';

    private static ?DateTimeZone $zone = null;

    private function __construct()
    {
    }

    public static function zone(): DateTimeZone
    {
        return self::$zone ??= new DateTimeZone('UTC');
    }

    public static function now(string $relative = 'now'): DateTime
    {
        return new DateTime($relative, self::zone());
    }

    /**
     * A time as a DATETIME literal in UTC.
     */
    public static function sql(DateTimeInterface $time): string
    {
        return DateTimeImmutable::createFromInterface($time)->setTimezone(self::zone())->format(self::SQL_FORMAT);
    }

    /**
     * A stored DATETIME value, read as the UTC time it is.
     */
    public static function parse(string $stored): DateTimeImmutable
    {
        return new DateTimeImmutable($stored, self::zone());
    }
}
