<?php

declare(strict_types=1);

namespace App\Database;

/**
 * SQLite 3.35 and newer, built with its math functions (as the usual builds are): for
 * one machine, locally and in tests. It has no row locks, only one writer at a time, so
 * a claim is one UPDATE ... RETURNING; DELETE takes no LIMIT in the usual builds.
 */
final class SqliteDialect extends Dialect
{
    public function name(): string
    {
        return 'sqlite';
    }

    public function skipLocked(): string
    {
        return '';
    }

    public function claimsInOneStatement(): bool
    {
        return true;
    }

    public function jsonValue(string $column, string $key): string
    {
        return "json_extract({$column}, '$.{$key}')";
    }

    public function jsonMember(string $column, string $param): string
    {
        return "json_extract({$column}, {$param})";
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

        return 'json_object(' . implode(', ', $parts) . ')';
    }

    public function jsonBool(bool $value): string
    {
        return $value ? "json('true')" : "json('false')";
    }

    public function jsonWith(string $column, string $name, string $value): string
    {
        return "json_set(COALESCE({$column}, '{}'), '$.\"' || {$name} || '\"', {$value})";
    }

    public function least(string $a, string $b): string
    {
        return "MIN({$a}, {$b})";
    }

    public function power(string $base, string $exponent): string
    {
        return "pow({$base}, {$exponent})";
    }

    public function plusSeconds(string $time, string $seconds): string
    {
        return "datetime({$time}, '+' || CAST({$seconds} AS INTEGER) || ' seconds')";
    }

    public function epochSeconds(string $time): string
    {
        return "CAST(strftime('%s', {$time}) AS INTEGER)";
    }

    public function deleteMessages(string $where, ?string $orderBy = null): string
    {
        $order = $orderBy === null ? '' : " ORDER BY {$orderBy}";

        return "DELETE FROM messages WHERE id IN (SELECT id FROM messages WHERE {$where}{$order} LIMIT :limit)";
    }

    public function insertOrIgnore(string $table, array $columns, array $key): string
    {
        return "INSERT INTO {$table} " . self::values($columns)
            . ' ON CONFLICT (' . implode(', ', $key) . ') DO NOTHING';
    }

    public function upsert(string $table, array $columns, array $key, array $update): string
    {
        $set = array_map(static fn(string $column) => "{$column} = excluded.{$column}", $update);

        return "INSERT INTO {$table} " . self::values($columns)
            . ' ON CONFLICT (' . implode(', ', $key) . ') DO UPDATE SET ' . implode(', ', $set);
    }
}
