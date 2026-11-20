<?php

declare(strict_types=1);

namespace App\Api;

use App\Doctrine\UtcDateTimeImmutableType;
use DateTimeInterface;

/**
 * Every timestamp in the API is RFC 3339 in UTC with a Z suffix.
 */
final class Time
{
    private function __construct()
    {
    }

    public static function format(?DateTimeInterface $time): ?string
    {
        if ($time === null) {
            return null;
        }

        return \DateTimeImmutable::createFromInterface($time)
            ->setTimezone(UtcDateTimeImmutableType::utc())
            ->format('Y-m-d\TH:i:s\Z');
    }
}
