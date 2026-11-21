<?php

declare(strict_types=1);

namespace App\Model;

/**
 * Limits per tenant, set by the operator. A missing or null value means unlimited.
 *
 * - max_queues: a new queue beyond it is refused (403 quota_exceeded).
 * - max_open_messages: pending plus processing; a publish beyond it waits (429).
 * - publish_per_second, claim_per_second: rates (429 with Retry-After).
 * - max_message_bytes, max_retention_seconds: caps on every queue's own setting; a queue
 *   setting above them is refused, and one set before the cap came is held to it.
 */
final class Quotas
{
    /** field => minimum */
    public const FIELDS = [
        'max_queues' => 1,
        'max_open_messages' => 1,
        'publish_per_second' => 1,
        'claim_per_second' => 1,
        'max_message_bytes' => 1,
        'max_retention_seconds' => 0,
    ];

    private function __construct()
    {
    }

    /**
     * @param array<string, int|null> $stored
     * @return array<string, int|null> every field, null where unlimited
     */
    public static function resolve(array $stored): array
    {
        $quotas = [];

        foreach (array_keys(self::FIELDS) as $field) {
            $quotas[$field] = $stored[$field] ?? null;
        }

        return $quotas;
    }

    /**
     * Queue settings as the tenant's quotas allow them: max_message_bytes and the
     * retention held to their caps.
     *
     * @param array<string, mixed>    $settings resolved queue settings
     * @param array<string, int|null> $quotas   resolved quotas
     * @return array<string, mixed>
     */
    public static function cap(array $settings, array $quotas): array
    {
        if (($quotas['max_message_bytes'] ?? null) !== null) {
            $settings['max_message_bytes'] = min((int) $settings['max_message_bytes'], $quotas['max_message_bytes']);
        }

        if (($quotas['max_retention_seconds'] ?? null) !== null) {
            foreach (['completed_seconds', 'failed_seconds'] as $field) {
                $settings['retention'][$field] = min(
                    (int) $settings['retention'][$field],
                    $quotas['max_retention_seconds'],
                );
            }
        }

        return $settings;
    }

    /**
     * The fields of a settings patch (a queue's, or a tenant's queue defaults) that go
     * beyond the tenant's quotas.
     *
     * @param array<string, mixed>    $patch
     * @param array<string, int|null> $quotas resolved quotas
     * @return list<array{field: string, message: string}>
     */
    public static function settingErrors(array $patch, array $quotas, string $prefix = ''): array
    {
        $errors = [];
        $bytes = $quotas['max_message_bytes'] ?? null;
        $retention = $quotas['max_retention_seconds'] ?? null;

        if ($bytes !== null && is_int($patch['max_message_bytes'] ?? null) && $patch['max_message_bytes'] > $bytes) {
            $errors[] = [
                'field' => $prefix . 'max_message_bytes',
                'message' => 'must be at most the tenant\'s quota of ' . $bytes,
            ];
        }

        foreach (['completed_seconds', 'failed_seconds'] as $field) {
            $value = is_array($patch['retention'] ?? null) ? ($patch['retention'][$field] ?? null) : null;

            if ($retention !== null && is_int($value) && $value > $retention) {
                $errors[] = [
                    'field' => $prefix . 'retention.' . $field,
                    'message' => 'must be at most the tenant\'s quota of ' . $retention,
                ];
            }
        }

        return $errors;
    }

    /**
     * @return list<array{field: string, message: string}>
     */
    public static function validate(mixed $quotas, string $prefix = 'quotas.'): array
    {
        if (!is_array($quotas) || ($quotas !== [] && array_is_list($quotas))) {
            return [['field' => rtrim($prefix, '.'), 'message' => 'must be an object']];
        }

        $errors = [];

        foreach ($quotas as $field => $value) {
            if (!array_key_exists($field, self::FIELDS)) {
                $errors[] = ['field' => $prefix . $field, 'message' => 'unknown quota'];
            } elseif ($value !== null && (!is_int($value) || $value < self::FIELDS[$field])) {
                $errors[] = [
                    'field' => $prefix . $field,
                    'message' => 'must be null or an integer of at least ' . self::FIELDS[$field],
                ];
            }
        }

        return $errors;
    }
}
