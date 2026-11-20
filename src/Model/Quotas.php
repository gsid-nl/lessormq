<?php

declare(strict_types=1);

namespace App\Model;

/**
 * Limits per tenant, set by the operator. A missing or null value means unlimited.
 * Stored and returned here; enforcement is a separate piece of work.
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
