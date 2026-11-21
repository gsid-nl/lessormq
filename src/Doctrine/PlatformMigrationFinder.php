<?php

declare(strict_types=1);

namespace App\Doctrine;

use App\Database\Dialect;
use Doctrine\Migrations\Finder\MigrationFinder;
use Doctrine\Migrations\Finder\RecursiveRegexFinder;

/**
 * Finds only the migrations of the database LessorMQ runs on: each database has its own
 * series, in its own namespace (config/packages/doctrine_migrations.yaml).
 *
 * The MariaDB series is the one LessorMQ grew up with, from before the others existed;
 * it stays as it is. MySQL, PostgreSQL and SQLite each start with a baseline of the
 * schema as it was when they came, and change from there.
 */
final class PlatformMigrationFinder implements MigrationFinder
{
    /** Migration namespace => the database its series is for (Dialect::name()). */
    public const SERIES = [
        'DoctrineMigrations' => 'mariadb',
        'LessorMQ\Migrations\MySQL' => 'mysql',
        'LessorMQ\Migrations\PostgreSQL' => 'postgresql',
        'LessorMQ\Migrations\SQLite' => 'sqlite',
    ];

    private readonly RecursiveRegexFinder $finder;

    public function __construct(private readonly Dialect $dialect)
    {
        $this->finder = new RecursiveRegexFinder();
    }

    public function findMigrations(string $directory, ?string $namespace = null): array
    {
        if ($namespace === null || (self::SERIES[$namespace] ?? null) !== $this->dialect->name()) {
            return [];
        }

        return $this->finder->findMigrations($directory, $namespace);
    }
}
