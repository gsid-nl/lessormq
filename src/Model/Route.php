<?php

declare(strict_types=1);

namespace App\Model;

/**
 * A route names the path a worker runs its jobs through: a VPN exit, a proxy, an API
 * account, a region. Routes are keys in a message's failed_routes JSON object, so the
 * characters are limited to those that need no escaping in a JSON path.
 */
final class Route
{
    public const PATTERN = '[A-Za-z0-9][A-Za-z0-9._:@+\/-]{0,254}';

    private function __construct()
    {
    }

    public static function isValid(?string $route): bool
    {
        return $route !== null && preg_match('/^' . self::PATTERN . '$/D', $route) === 1;
    }

    /**
     * The JSON path of this route's entry in failed_routes.
     */
    public static function path(string $route): string
    {
        return '$."' . $route . '"';
    }

    /**
     * Cooldown in seconds for claims on $route, or null when nothing is to be avoided:
     * no route, or a queue that does not avoid failed routes.
     */
    public static function avoidance(?string $route, QueueConfig $config): ?int
    {
        if ($route === null || $config->settings['route_policy']['mode'] !== 'avoid_failed') {
            return null;
        }

        return (int) $config->settings['route_policy']['cooldown_seconds'];
    }
}
