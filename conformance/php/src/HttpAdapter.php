<?php

declare(strict_types=1);

namespace LessorMQ\Conformance;

/**
 * The reference adapter: every call straight onto the v1 HTTP API (docs/openapi.yaml).
 */
final class HttpAdapter implements Adapter
{
    public function __construct(private readonly Http $http)
    {
    }

    public function call(string $call, string $apiKey, array $args): array
    {
        $auth = ['Authorization' => 'Bearer ' . $apiKey];

        $response = match ($call) {
            'publish' => $this->http->request(
                'POST',
                '/v1/queues/' . rawurlencode($args['queue']) . '/messages',
                $this->only($args, ['body', 'headers', 'dedup_key', 'delay_seconds', 'available_at', 'max_attempts']),
                $auth,
            ),
            'claim' => $this->http->request(
                'POST',
                '/v1/queues/' . rawurlencode($args['queue']) . '/claims',
                (object) $this->only($args, ['consumer', 'route', 'wait_seconds', 'lease_seconds']),
                $auth,
            ),
            'extend', 'ack', 'fail', 'release' => $this->http->request(
                'POST',
                '/v1/messages/' . rawurlencode($args['message']) . '/' . $call,
                $this->only($args, ['receipt', 'lease_seconds', 'error', 'permanent', 'delay_seconds']),
                $auth,
            ),
            default => throw new \InvalidArgumentException('Unknown call ' . $call),
        };

        return ['status' => $response['status'], 'body' => $response['body']];
    }

    /**
     * @param array<string, mixed> $args
     * @param list<string>         $keys
     * @return array<string, mixed>
     */
    private function only(array $args, array $keys): array
    {
        return array_intersect_key($args, array_flip($keys));
    }
}
