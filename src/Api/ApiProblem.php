<?php

declare(strict_types=1);

namespace App\Api;

use Symfony\Component\HttpFoundation\JsonResponse;

/**
 * An error for an API client, rendered as RFC 9457 problem details with a stable `code`
 * (docs/openapi.yaml, Problem). Throw it anywhere under /v1; ProblemListener renders it.
 */
final class ApiProblem extends \RuntimeException
{
    private const TITLES = [
        'validation_failed' => 'The request is not valid',
        'unauthorized' => 'Not authenticated',
        'forbidden' => 'Not allowed',
        'tenant_suspended' => 'Tenant suspended',
        'not_found' => 'Not found',
        'lease_lost' => 'Lease lost',
        'invalid_state' => 'Invalid state',
        'payload_too_large' => 'Payload too large',
        'rate_limited' => 'Rate limited',
        'quota_exceeded' => 'Quota exceeded',
        'unavailable' => 'Service unavailable',
        'internal' => 'Internal error',
    ];

    /**
     * @param list<array{field: string, message: string}> $errors
     * @param array<string, string> $headers
     */
    public function __construct(
        public readonly int $status,
        public readonly string $problemCode,
        string $detail,
        public readonly array $errors = [],
        public readonly array $headers = [],
    ) {
        parent::__construct($detail);
    }

    /**
     * @param list<array{field: string, message: string}> $errors
     */
    public static function validation(array $errors, string $detail = 'One or more fields are not valid.'): self
    {
        return new self(400, 'validation_failed', $detail, $errors);
    }

    public static function unauthorized(string $detail): self
    {
        return new self(401, 'unauthorized', $detail, [], ['WWW-Authenticate' => 'Bearer']);
    }

    public static function forbidden(string $detail): self
    {
        return new self(403, 'forbidden', $detail);
    }

    public static function tenantSuspended(): self
    {
        return new self(403, 'tenant_suspended', 'The tenant of this key is suspended.');
    }

    public static function notFound(string $detail): self
    {
        return new self(404, 'not_found', $detail);
    }

    public static function invalidState(string $detail): self
    {
        return new self(409, 'invalid_state', $detail);
    }

    public static function leaseLost(): self
    {
        return new self(
            409,
            'lease_lost',
            'This receipt is not the current lease of the message: it expired, was released, or the message was '
            . 'claimed again. Stop reporting on it.',
        );
    }

    public static function payloadTooLarge(int $limit): self
    {
        return new self(
            413,
            'payload_too_large',
            sprintf('Body and headers exceed the queue limit of %d bytes.', $limit),
        );
    }

    /**
     * A limit that lifts by itself: a rate, or open messages that will be worked off.
     */
    public static function rateLimited(int $retryAfter, string $detail): self
    {
        return new self(429, 'rate_limited', $detail, [], ['Retry-After' => (string) max(1, $retryAfter)]);
    }

    /**
     * A limit that waiting does not lift; it takes a change by the tenant or the operator.
     */
    public static function quotaExceeded(string $detail): self
    {
        return new self(403, 'quota_exceeded', $detail);
    }

    public static function internal(): self
    {
        return new self(500, 'internal', 'An unexpected error occurred.');
    }

    public function toResponse(): JsonResponse
    {
        $body = [
            'type' => 'urn:lessormq:problem:' . $this->problemCode,
            'title' => self::TITLES[$this->problemCode] ?? 'Error',
            'status' => $this->status,
            'detail' => $this->getMessage(),
            'code' => $this->problemCode,
        ];

        if ($this->errors !== []) {
            $body['errors'] = $this->errors;
        }

        return new JsonResponse(
            $body,
            $this->status,
            $this->headers + ['Content-Type' => 'application/problem+json'],
        );
    }
}
