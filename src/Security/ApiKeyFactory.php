<?php

declare(strict_types=1);

namespace App\Security;

use App\Entity\ApiKey;
use App\Entity\Enum\Scope;

/**
 * Creates keys. A secret is 192 random bits, so a plain SHA-256 is enough to store it:
 * there is nothing to brute-force that a slow hash would protect.
 */
final class ApiKeyFactory
{
    public const SECRET_PREFIX = 'lmq_';
    private const PREFIX_LENGTH = 12;

    private function __construct()
    {
    }

    /**
     * @param list<Scope>  $scopes
     * @param list<string> $queues
     * @return array{0: ApiKey, 1: string} the key and its secret, which is not stored
     */
    public static function create(?string $tenantId, string $name, array $scopes, array $queues = []): array
    {
        $secret = self::SECRET_PREFIX . bin2hex(random_bytes(24));

        $key = new ApiKey(
            bin2hex(random_bytes(8)),
            $tenantId,
            $name,
            substr($secret, 0, self::PREFIX_LENGTH),
            self::hash($secret),
            array_map(static fn(Scope $scope) => $scope->value, $scopes),
            $queues,
        );

        return [$key, $secret];
    }

    public static function hash(string $secret): string
    {
        return hash('sha256', $secret);
    }
}
