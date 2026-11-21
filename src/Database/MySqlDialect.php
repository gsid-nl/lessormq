<?php

declare(strict_types=1);

namespace App\Database;

/**
 * MySQL 8.0.21 and newer (JSON_VALUE). MariaDB's SQL, but for the upsert: MySQL
 * deprecates VALUES() there and names the new row instead.
 */
final class MySqlDialect extends MariaDbDialect
{
    public function name(): string
    {
        return 'mysql';
    }

    public function upsert(string $table, array $columns, array $key, array $update): string
    {
        $set = array_map(static fn(string $column) => "{$column} = new.{$column}", $update);

        return "INSERT INTO {$table} " . self::values($columns)
            . ' AS new ON DUPLICATE KEY UPDATE ' . implode(', ', $set);
    }
}
