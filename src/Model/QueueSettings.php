<?php

declare(strict_types=1);

namespace App\Model;

/**
 * The settings that decide what happens to a queue's messages, with the defaults of the
 * spec (docs/openapi.yaml, QueueSettings).
 *
 * Settings are layered: product defaults, then the tenant's queue_defaults, then the
 * queue's own settings. Every layer above the defaults is stored as a patch, so a tenant
 * that never overrode a value follows the product default when it changes.
 */
final class QueueSettings
{
    public const DEFAULTS = [
        'lease_seconds' => 300,
        'max_lease_seconds' => 3600,
        'max_attempts' => 7,
        'max_attempts_limit' => 25,
        'backoff' => [
            'initial_seconds' => 10,
            'multiplier' => 2,
            'max_seconds' => 3600,
            'jitter' => 0.1,
        ],
        'on_exhausted' => 'dead_letter',
        'on_permanent' => 'dead_letter',
        'route_policy' => [
            'mode' => 'none',
            'cooldown_seconds' => 3600,
        ],
        'retention' => [
            'completed_seconds' => 86400,
            'failed_seconds' => 604800,
        ],
        'max_message_bytes' => 1048576,
        'paused' => false,
    ];

    private const ENUMS = [
        'on_exhausted' => ['dead_letter', 'discard'],
        'on_permanent' => ['dead_letter', 'retry'],
        'route_policy.mode' => ['none', 'avoid_failed'],
    ];

    /** field => minimum, for integer fields */
    private const INTEGERS = [
        'lease_seconds' => 1,
        'max_lease_seconds' => 1,
        'max_attempts' => 1,
        'max_attempts_limit' => 1,
        'backoff.initial_seconds' => 0,
        'backoff.max_seconds' => 0,
        'route_policy.cooldown_seconds' => 0,
        'retention.completed_seconds' => 0,
        'retention.failed_seconds' => 0,
        'max_message_bytes' => 1,
    ];

    private function __construct()
    {
    }

    /**
     * Apply patches in order over the defaults. Nested objects are merged, not replaced.
     *
     * @param array<string, mixed> ...$patches
     * @return array<string, mixed>
     */
    public static function resolve(array ...$patches): array
    {
        $settings = self::DEFAULTS;

        foreach ($patches as $patch) {
            $settings = self::merge($settings, $patch);
        }

        return $settings;
    }

    /**
     * @param array<string, mixed> $base
     * @param array<string, mixed> $patch
     * @return array<string, mixed>
     */
    public static function merge(array $base, array $patch): array
    {
        foreach ($patch as $key => $value) {
            if (is_array($value) && isset($base[$key]) && is_array($base[$key])) {
                $base[$key] = self::merge($base[$key], $value);
            } else {
                $base[$key] = $value;
            }
        }

        return $base;
    }

    /**
     * Check a patch on its own (shape, types, ranges), then the settings it produces
     * over $base (rules between fields).
     *
     * @param array<string, mixed> $base the resolved settings the patch applies to
     * @return list<array{field: string, message: string}>
     */
    public static function validatePatch(mixed $patch, array $base = self::DEFAULTS, string $prefix = ''): array
    {
        if (!is_array($patch) || ($patch !== [] && array_is_list($patch))) {
            return [['field' => rtrim($prefix, '.') ?: '(body)', 'message' => 'must be an object']];
        }

        $errors = self::validateShape($patch, self::DEFAULTS, $prefix);

        if ($errors !== []) {
            return $errors;
        }

        return self::validateRelations(self::merge($base, $patch), $prefix);
    }

    /**
     * @param array<string, mixed> $patch
     * @param array<string, mixed> $template
     * @return list<array{field: string, message: string}>
     */
    private static function validateShape(array $patch, array $template, string $prefix, string $path = ''): array
    {
        $errors = [];

        foreach ($patch as $key => $value) {
            $name = $path === '' ? (string) $key : $path . '.' . $key;
            $field = $prefix . $name;

            if (!array_key_exists($key, $template)) {
                $errors[] = ['field' => $field, 'message' => 'unknown setting'];
                continue;
            }

            $expected = $template[$key];

            if (is_array($expected)) {
                if (!is_array($value) || ($value !== [] && array_is_list($value))) {
                    $errors[] = ['field' => $field, 'message' => 'must be an object'];
                    continue;
                }

                array_push($errors, ...self::validateShape($value, $expected, $prefix, $name));
                continue;
            }

            $error = self::checkScalar($name, $value);

            if ($error !== null) {
                $errors[] = ['field' => $field, 'message' => $error];
            }
        }

        return $errors;
    }

    private static function checkScalar(string $name, mixed $value): ?string
    {
        if (isset(self::INTEGERS[$name])) {
            if (!is_int($value)) {
                return 'must be an integer';
            }

            return $value < self::INTEGERS[$name] ? 'must be at least ' . self::INTEGERS[$name] : null;
        }

        if (isset(self::ENUMS[$name])) {
            return in_array($value, self::ENUMS[$name], true)
                ? null
                : 'must be one of: ' . implode(', ', self::ENUMS[$name]);
        }

        return match ($name) {
            'paused' => is_bool($value) ? null : 'must be a boolean',
            'backoff.multiplier' => (is_int($value) || is_float($value)) && $value >= 1
                ? null
                : 'must be a number of at least 1',
            'backoff.jitter' => (is_int($value) || is_float($value)) && $value >= 0 && $value <= 1
                ? null
                : 'must be a number from 0 to 1',
            default => 'unknown setting',
        };
    }

    /**
     * @param array<string, mixed> $settings
     * @return list<array{field: string, message: string}>
     */
    private static function validateRelations(array $settings, string $prefix): array
    {
        $errors = [];

        if ($settings['lease_seconds'] > $settings['max_lease_seconds']) {
            $errors[] = ['field' => $prefix . 'lease_seconds', 'message' => 'must not exceed max_lease_seconds'];
        }

        if ($settings['max_attempts'] > $settings['max_attempts_limit']) {
            $errors[] = ['field' => $prefix . 'max_attempts', 'message' => 'must not exceed max_attempts_limit'];
        }

        return $errors;
    }
}
