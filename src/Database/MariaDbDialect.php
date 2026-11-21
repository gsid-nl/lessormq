<?php

declare(strict_types=1);

namespace App\Database;

use Doctrine\DBAL\Connection;

/**
 * MariaDB 10.6 and newer: the SQL LessorMQ was written in.
 */
class MariaDbDialect extends Dialect
{
    public function name(): string
    {
        return 'mariadb';
    }

    /**
     * InnoDB defaults to REPEATABLE READ, whose gap locks made claims and the range
     * writers deadlock. Without SESSION this holds for the next transaction only, so the
     * connection keeps its own level afterwards (php-fpm keeps it across requests).
     */
    public function readCommitted(Connection $connection): void
    {
        $connection->executeStatement('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');
    }

    public function replaysInOneStatement(): bool
    {
        return true;
    }

    public function jsonValue(string $column, string $key): string
    {
        return "JSON_VALUE({$column}, '$.{$key}')";
    }

    public function jsonMember(string $column, string $param): string
    {
        return "JSON_VALUE({$column}, {$param})";
    }

    public function jsonMemberArgument(string $name): string
    {
        return '$."' . $name . '"';
    }

    public function jsonObject(array $members): string
    {
        $parts = [];

        foreach ($members as $name => $value) {
            $parts[] = "'{$name}', {$value}";
        }

        return 'JSON_OBJECT(' . implode(', ', $parts) . ')';
    }

    public function jsonBool(bool $value): string
    {
        return $value ? "JSON_EXTRACT('true', '$')" : "JSON_EXTRACT('false', '$')";
    }

    public function jsonWith(string $column, string $name, string $value): string
    {
        return "JSON_SET(COALESCE({$column}, JSON_OBJECT()), CONCAT('$.\"', {$name}, '\"'), {$value})";
    }

    public function plusSeconds(string $time, string $seconds): string
    {
        return "CAST({$time} AS DATETIME) + INTERVAL {$seconds} SECOND";
    }

    public function epochSeconds(string $time): string
    {
        // Not UNIX_TIMESTAMP(), which reads the time in the session's time zone.
        return "TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', {$time})";
    }

    public function deleteMessages(string $where, ?string $orderBy = null): string
    {
        $order = $orderBy === null ? '' : " ORDER BY {$orderBy}";

        return "DELETE FROM messages WHERE {$where}{$order} LIMIT :limit";
    }

    public function insertOrIgnore(string $table, array $columns, array $key): string
    {
        return "INSERT IGNORE INTO {$table} " . self::values($columns);
    }

    public function upsert(string $table, array $columns, array $key, array $update): string
    {
        $set = array_map(static fn(string $column) => "{$column} = VALUES({$column})", $update);

        return "INSERT INTO {$table} " . self::values($columns) . ' ON DUPLICATE KEY UPDATE ' . implode(', ', $set);
    }
}
