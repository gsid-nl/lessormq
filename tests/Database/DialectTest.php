<?php

declare(strict_types=1);

namespace App\Tests\Database;

use App\Database\Dialect;
use App\Doctrine\PlatformMigrationFinder;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\MariaDBPlatform;
use Doctrine\DBAL\Platforms\MySQL80Platform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Doctrine\DBAL\Platforms\SQLServerPlatform;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class DialectTest extends TestCase
{
    /**
     * @return iterable<string, array{AbstractPlatform, string}>
     */
    public static function platforms(): iterable
    {
        yield 'MariaDB' => [new MariaDBPlatform(), 'mariadb'];
        yield 'MySQL' => [new MySQL80Platform(), 'mysql'];
        yield 'PostgreSQL' => [new PostgreSQLPlatform(), 'postgresql'];
        yield 'SQLite' => [new SqlitePlatform(), 'sqlite'];
    }

    #[DataProvider('platforms')]
    public function testPicksTheDialectOfTheConnection(AbstractPlatform $platform, string $name): void
    {
        self::assertSame($name, Dialect::for($this->connection($platform))->name());
    }

    public function testRefusesAnotherDatabase(): void
    {
        $this->expectException(\LogicException::class);

        Dialect::for($this->connection(new SQLServerPlatform()));
    }

    #[DataProvider('platforms')]
    public function testOnlyTheDatabasesOwnMigrationsAreFound(AbstractPlatform $platform, string $name): void
    {
        $finder = new PlatformMigrationFinder(Dialect::for($this->connection($platform)));
        $root = dirname(__DIR__, 2) . '/migrations';

        foreach (PlatformMigrationFinder::SERIES as $namespace => $database) {
            $directory = $database === 'mariadb' ? $root : $root . '/' . $database;
            $found = $finder->findMigrations($directory, $namespace);

            if ($database === $name) {
                self::assertNotEmpty($found, "{$database} has migrations");

                foreach ($found as $class) {
                    self::assertStringStartsWith($namespace . '\\', $class);
                }
            } else {
                self::assertSame([], $found, "{$database}'s series is not run on {$name}");
            }
        }
    }

    private function connection(AbstractPlatform $platform): Connection
    {
        $connection = $this->createStub(Connection::class);
        $connection->method('getDatabasePlatform')->willReturn($platform);

        return $connection;
    }
}
