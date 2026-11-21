<?php

declare(strict_types=1);

namespace App\Database;

/**
 * PostgreSQL 13 and newer. Its default isolation is already READ COMMITTED, DELETE takes
 * no LIMIT, and JSON is built with json functions on jsonb. A parameter inside a json
 * builder has no type PostgreSQL can infer, so it is cast to text.
 */
final class PostgreSqlDialect extends Dialect
{
    public function name(): string
    {
        return 'postgresql';
    }

    public function jsonValue(string $column, string $key): string
    {
        return "(CAST({$column} AS jsonb) ->> '{$key}')";
    }

    public function jsonMember(string $column, string $param): string
    {
        return "(CAST({$column} AS jsonb) ->> CAST({$param} AS TEXT))";
    }

    public function jsonMemberArgument(string $name): string
    {
        return $name;
    }

    public function jsonText(string $column): string
    {
        return "CAST({$column} AS TEXT)";
    }

    public function jsonObject(array $members): string
    {
        $parts = [];

        foreach ($members as $name => $value) {
            $parts[] = "'{$name}', {$value}";
        }

        return 'CAST(json_build_object(' . implode(', ', $parts) . ') AS TEXT)';
    }

    public function jsonBool(bool $value): string
    {
        return $value ? 'TRUE' : 'FALSE';
    }

    public function jsonString(string $param): string
    {
        return "CAST({$param} AS TEXT)";
    }

    public function jsonWith(string $column, string $name, string $value): string
    {
        return "CAST(COALESCE(CAST({$column} AS jsonb), '{}'::jsonb) || jsonb_build_object({$name}, {$value}) AS json)";
    }

    public function power(string $base, string $exponent): string
    {
        return "POWER(CAST({$base} AS DOUBLE PRECISION), {$exponent})";
    }

    public function plusSeconds(string $time, string $seconds): string
    {
        return "CAST({$time} AS TIMESTAMP(0)) + ({$seconds}) * INTERVAL '1 second'";
    }

    public function epochSeconds(string $time): string
    {
        // Of a timestamp without time zone, the nominal seconds, whatever the session's zone.
        return "CAST(EXTRACT(EPOCH FROM {$time}) AS BIGINT)";
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
        $set = array_map(static fn(string $column) => "{$column} = EXCLUDED.{$column}", $update);

        return "INSERT INTO {$table} " . self::values($columns)
            . ' ON CONFLICT (' . implode(', ', $key) . ') DO UPDATE SET ' . implode(', ', $set);
    }
}
