<?php

declare(strict_types=1);

namespace App\Api;

use Symfony\Component\HttpFoundation\Request;

/**
 * Cursor pagination: the cursor is the sort key of the last item, encoded so clients
 * treat it as opaque.
 */
final class Pagination
{
    private const DEFAULT_LIMIT = 20;
    private const MAX_LIMIT = 100;

    private function __construct()
    {
    }

    public static function limit(Request $request): int
    {
        $raw = $request->query->get('limit');

        if ($raw === null) {
            return self::DEFAULT_LIMIT;
        }

        $limit = filter_var($raw, FILTER_VALIDATE_INT);

        if ($limit === false || $limit < 1 || $limit > self::MAX_LIMIT) {
            throw ApiProblem::validation([
                ['field' => 'limit', 'message' => 'must be an integer from 1 to ' . self::MAX_LIMIT],
            ]);
        }

        return $limit;
    }

    /**
     * @param string $keyPattern what a decoded cursor must look like
     */
    public static function after(Request $request, string $keyPattern): ?string
    {
        $cursor = $request->query->get('cursor');

        if ($cursor === null || $cursor === '') {
            return null;
        }

        $key = base64_decode(strtr($cursor, '-_', '+/'), true);

        if ($key === false || preg_match('/^(?:' . $keyPattern . ')$/D', $key) !== 1) {
            throw ApiProblem::validation([['field' => 'cursor', 'message' => 'is not a cursor from this API']]);
        }

        return $key;
    }

    public static function cursor(string $key): string
    {
        return rtrim(strtr(base64_encode($key), '+/', '-_'), '=');
    }
}
