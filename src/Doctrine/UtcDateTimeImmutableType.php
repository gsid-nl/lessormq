<?php

declare(strict_types=1);

namespace App\Doctrine;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Types\ConversionException;
use Doctrine\DBAL\Types\DateTimeImmutableType;

/**
 * A DATETIME column that always holds UTC, whatever the PHP default timezone is.
 *
 * The stock datetime types write the wall-clock time of the object as given and read it
 * back in the default timezone, so a value written from Europe/Amsterdam is ambiguous for
 * an hour every October. This type converts to UTC on the way in and reads as UTC.
 */
final class UtcDateTimeImmutableType extends DateTimeImmutableType
{
    public const NAME = 'utc_datetime_immutable';

    private static ?DateTimeZone $utc = null;

    public function getName(): string
    {
        return self::NAME;
    }

    public function convertToDatabaseValue($value, AbstractPlatform $platform): ?string
    {
        if ($value instanceof DateTimeInterface) {
            $value = DateTimeImmutable::createFromInterface($value)->setTimezone(self::utc());
        }

        return parent::convertToDatabaseValue($value, $platform);
    }

    public function convertToPHPValue($value, AbstractPlatform $platform): ?DateTimeImmutable
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof DateTimeInterface) {
            return DateTimeImmutable::createFromInterface($value)->setTimezone(self::utc());
        }

        $format = $platform->getDateTimeFormatString();
        $converted = DateTimeImmutable::createFromFormat('!' . $format, (string) $value, self::utc());

        if ($converted === false) {
            throw ConversionException::conversionFailedFormat($value, $this->getName(), $format);
        }

        return $converted;
    }

    public static function utc(): DateTimeZone
    {
        return self::$utc ??= new DateTimeZone('UTC');
    }
}
