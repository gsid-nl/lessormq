<?php

declare(strict_types=1);

namespace App\Doctrine;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Ramsey\Uuid\Doctrine\UuidBinaryOrderedTimeType;
use Ramsey\Uuid\UuidInterface;

/**
 * The message id column: ramsey's ordered-time UUID, also read from a stream.
 *
 * PostgreSQL hands bytea back as a stream, which ramsey's type reads as null; MariaDB,
 * MySQL and SQLite hand back a string.
 */
final class OrderedTimeUuidType extends UuidBinaryOrderedTimeType
{
    public function convertToPHPValue($value, AbstractPlatform $platform): ?UuidInterface
    {
        return parent::convertToPHPValue(self::bytes($value), $platform);
    }

    /**
     * The raw bytes of a value read from the column: as they come, or read from the
     * stream PostgreSQL returns.
     */
    public static function bytes(mixed $value): mixed
    {
        return is_resource($value) ? stream_get_contents($value) : $value;
    }
}
