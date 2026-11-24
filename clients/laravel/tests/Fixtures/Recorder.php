<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests\Fixtures;

/**
 * What the fixture jobs did, for the tests to look at.
 */
final class Recorder
{
    /** @var list<array{string, mixed}> */
    public static array $events = [];

    public static function record(string $what, mixed $detail = null): void
    {
        self::$events[] = [$what, $detail];
    }

    /**
     * @return list<mixed> the details recorded under $what
     */
    public static function of(string $what): array
    {
        return array_values(array_map(
            static fn(array $event) => $event[1],
            array_filter(self::$events, static fn(array $event) => $event[0] === $what),
        ));
    }

    public static function reset(): void
    {
        self::$events = [];
    }
}
