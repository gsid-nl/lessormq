<?php

declare(strict_types=1);

namespace App\Database;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Platforms\MariaDBPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;

/**
 * The SQL that differs between the databases LessorMQ runs on: MariaDB (where it began),
 * MySQL 8, PostgreSQL and SQLite. Everything else is written once, in SQL all four read
 * alike.
 *
 * Fragments take column names and SQL expressions (with :named parameters) as they come;
 * none of them takes a value from a request.
 */
abstract class Dialect
{
    public static function for(Connection $connection): self
    {
        $platform = $connection->getDatabasePlatform();

        return match (true) {
            $platform instanceof MariaDBPlatform => new MariaDbDialect(),
            $platform instanceof AbstractMySQLPlatform => new MySqlDialect(),
            $platform instanceof PostgreSQLPlatform => new PostgreSqlDialect(),
            $platform instanceof SqlitePlatform => new SqliteDialect(),
            default => throw new \LogicException(sprintf(
                'LessorMQ runs on MariaDB, MySQL, PostgreSQL or SQLite, not on %s.',
                $platform::class,
            )),
        };
    }

    /** mariadb, mysql, postgresql or sqlite. */
    abstract public function name(): string;

    /**
     * Make the next transaction, or the next statement in autocommit, READ COMMITTED,
     * where that is not already what it gets.
     */
    public function readCommitted(Connection $connection): void
    {
    }

    /** What a claim's candidate SELECT ends with, to walk past rows other claims hold. */
    public function skipLocked(): string
    {
        return ' FOR UPDATE SKIP LOCKED';
    }

    /**
     * Whether a claim is a single UPDATE ... RETURNING rather than a locking SELECT and an
     * UPDATE: for a database without row locks, where one statement is what is atomic.
     */
    public function claimsInOneStatement(): bool
    {
        return false;
    }

    /**
     * Whether a bulk replay is one UPDATE that skips the rows whose dedup_key an open
     * message holds (UPDATE IGNORE), rather than one replay per message.
     */
    public function replaysInOneStatement(): bool
    {
        return false;
    }

    /** A top-level scalar of a JSON column as text; $key is a plain name such as type. */
    abstract public function jsonValue(string $column, string $key): string;

    /** The member of a JSON object column named by the parameter, as text. */
    abstract public function jsonMember(string $column, string $param): string;

    /** What the parameter of jsonMember() is bound to, for the member $name. */
    abstract public function jsonMemberArgument(string $name): string;

    /** A JSON column as text, for LIKE. */
    public function jsonText(string $column): string
    {
        return $column;
    }

    /** A JSON object with these members (name => SQL expression), as text. */
    abstract public function jsonObject(array $members): string;

    /** The JSON true or false, as a value of jsonObject(). */
    abstract public function jsonBool(bool $value): string;

    /** A string parameter, as a value of jsonObject() or jsonWith(). */
    public function jsonString(string $param): string
    {
        return $param;
    }

    /**
     * The JSON object in $column (an empty one when it is NULL) with the member named by
     * the expression $name set to $value.
     */
    abstract public function jsonWith(string $column, string $name, string $value): string;

    /** The smaller of two numbers. */
    public function least(string $a, string $b): string
    {
        return "LEAST({$a}, {$b})";
    }

    /** $base to the power $exponent. */
    public function power(string $base, string $exponent): string
    {
        return "POW({$base}, {$exponent})";
    }

    /** The time $time (a UTC datetime parameter) plus $seconds, a whole number. */
    abstract public function plusSeconds(string $time, string $seconds): string;

    /**
     * The datetime column or expression $time as whole seconds since 1970-01-01 00:00 UTC,
     * whatever time zone the connection is in (times are stored in UTC without a zone).
     */
    abstract public function epochSeconds(string $time): string;

    /**
     * DELETE the rows of messages matching $where, at most :limit of them, in $orderBy
     * order when one is given.
     */
    abstract public function deleteMessages(string $where, ?string $orderBy = null): string;

    /**
     * INSERT a row with these columns (bound as :column) unless one with the same $key
     * exists.
     *
     * @param list<string> $columns
     * @param list<string> $key
     */
    abstract public function insertOrIgnore(string $table, array $columns, array $key): string;

    /**
     * INSERT a row with these columns (bound as :column), or, when one with the same $key
     * exists, set its $update columns to the new values.
     *
     * @param list<string> $columns
     * @param list<string> $key
     * @param list<string> $update
     */
    abstract public function upsert(string $table, array $columns, array $key, array $update): string;

    /**
     * @param list<string> $columns
     */
    protected static function values(array $columns): string
    {
        return '(' . implode(', ', $columns) . ') VALUES (:' . implode(', :', $columns) . ')';
    }
}
