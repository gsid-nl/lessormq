<?php

declare(strict_types=1);

namespace LessorMQ\Conformance;

use Symfony\Component\Yaml\Yaml;

/**
 * Runs the scenarios in conformance/scenarios against a server, through an Adapter.
 *
 * Every run makes two fresh tenants (a, the default, and b) with the operator key, and
 * gives every scenario its own queues, so runs never see each other's messages. Protocol
 * calls go through the adapter; setup and inspection (queue settings, reading a
 * message) go straight to the admin API, because a client library need not offer them.
 */
final class Runner
{
    private const PROTOCOL_CALLS = ['publish', 'claim', 'extend', 'ack', 'fail', 'release', 'claim_batch', 'ack_batch'];

    /** @var array<string, array{id: string, key: string}> */
    private array $tenants = [];

    private string $runId;

    public function __construct(
        private readonly Http $http,
        private readonly string $operatorKey,
        private readonly Adapter $adapter,
        private readonly string $baseUrl,
    ) {
        $this->runId = substr(bin2hex(random_bytes(4)), 0, 8);
    }

    /**
     * @param list<string> $files
     * @return bool whether every scenario passed
     */
    public function run(array $files, ?string $filter = null): bool
    {
        $files = array_values(array_filter(
            $files,
            static fn(string $file) => $filter === null || str_contains(basename($file), $filter),
        ));

        $this->createTenants();
        $failed = 0;

        printf("1..%d\n", count($files));

        try {
            foreach ($files as $number => $file) {
                $scenario = Yaml::parseFile($file);
                $failures = $this->scenario($scenario, $number);
                $label = sprintf('%s (%s)', $scenario['name'], basename($file, '.yaml'));

                if ($failures === []) {
                    printf("ok %d - %s\n", $number + 1, $label);
                } else {
                    $failed++;
                    printf("not ok %d - %s\n", $number + 1, $label);

                    foreach ($failures as $failure) {
                        printf("  # %s\n", $failure);
                    }
                }
            }
        } finally {
            $this->removeTenants();
        }

        printf("# %d of %d scenarios passed\n", count($files) - $failed, count($files));

        return $failed === 0;
    }

    /**
     * @param array<string, mixed> $scenario
     * @return list<string> failures, empty when the scenario passed
     */
    private function scenario(array $scenario, int $number): array
    {
        if (!isset($scenario['tenant_quotas']) && empty($scenario['own_tenant'])) {
            return $this->runScenario($scenario, $number);
        }

        // Quotas, and figures over all of a tenant's queues, hold for a whole tenant, so
        // this scenario gets one of its own as tenant a.
        $shared = $this->tenants['a'];
        $this->tenants['a'] = $this->createTenant('q' . $number, $scenario['tenant_quotas'] ?? []);

        try {
            return $this->runScenario($scenario, $number);
        } finally {
            $this->removeTenant($this->tenants['a']);
            $this->tenants['a'] = $shared;
        }
    }

    /**
     * @param array<string, mixed> $scenario
     * @return list<string> failures, empty when the scenario passed
     */
    private function runScenario(array $scenario, int $number): array
    {
        $prefix = sprintf('c%s-%d', $this->runId, $number);
        $saved = [];

        foreach ($scenario['queues'] ?? [] as $queue => $settings) {
            foreach (array_keys($this->tenants) as $tenant) {
                $response = $this->admin(
                    $this->tenants[$tenant]['key'],
                    'PATCH',
                    '/v1/queues/' . $prefix . '-' . $queue,
                    $settings,
                );

                if ($response['status'] !== 200) {
                    return [sprintf('setup of queue %s failed: %s', $queue, json_encode($response['body']))];
                }
            }
        }

        foreach ($scenario['steps'] as $index => $step) {
            try {
                $result = $this->step($step, $prefix, $saved);
            } catch (\Throwable $e) {
                return [sprintf('step %d (%s): %s', $index + 1, $this->stepName($step), $e->getMessage())];
            }

            if (isset($step['save'])) {
                $saved[$step['save']] = $result;
            }

            $failures = Expect::check($this->resolve($step['expect'] ?? [], $saved), $result);

            if ($failures !== []) {
                return array_map(
                    fn(string $failure) => sprintf('step %d (%s) %s', $index + 1, $this->stepName($step), $failure),
                    $failures,
                );
            }
        }

        return [];
    }

    /**
     * @param array<string, mixed> $step
     * @param array<string, mixed> $saved
     * @return array<string, mixed>
     */
    private function step(array $step, string $prefix, array $saved): array
    {
        $name = $this->stepName($step);

        if ($name === 'sleep') {
            usleep((int) ((float) $step['sleep'] * 1_000_000));

            return [];
        }

        $args = $this->resolve($step[$name] ?? [], $saved);
        $tenant = $args['tenant'] ?? 'a';
        unset($args['tenant']);

        $queueSteps = [
            'publish', 'claim', 'publish_http', 'claim_http', 'settings', 'queue', 'messages', 'replay_all', 'purge',
            'claim_batch',
            'workers',
            'history',
        ];

        if (isset($args['queue']) || in_array($name, $queueSteps, true)) {
            $args['queue'] = $prefix . '-' . ($args['queue'] ?? 'main');
        }

        // A claim's lease stands for its message id and receipt; an explicit receipt or
        // message overrides it (to send a stale receipt, say).
        if (isset($args['lease'])) {
            $lease = $args['lease'];
            $args['message'] ??= $lease['body']['message']['id'] ?? null;
            $args['receipt'] ??= $lease['body']['receipt'] ?? null;
            unset($args['lease']);
        }

        $publishAfter = $args['publish_after'] ?? null;
        unset($args['publish_after']);

        if ($publishAfter !== null) {
            $this->publishLater($tenant, $args['queue'], $publishAfter);
        }

        $started = microtime(true);
        // Another key than the tenant's admin key: one saved from an api_key step, or the
        // operator key.
        $key = match (true) {
            !empty($args['operator']) => $this->operatorKey,
            isset($args['key']) => (string) $args['key'],
            default => $this->tenants[$tenant]['key'],
        };
        unset($args['key'], $args['operator']);
        $id = rawurlencode((string) ($args['id'] ?? ''));

        $result = match (true) {
            in_array($name, self::PROTOCOL_CALLS, true) => $this->adapter->call($name, $key, $args),
            $name === 'settings' => $this->admin($key, 'PATCH', '/v1/queues/' . $args['queue'], $args['set']),
            $name === 'queue' => $this->admin($key, 'GET', '/v1/queues/' . $args['queue']),
            $name === 'messages' => $this->admin(
                $key,
                'GET',
                '/v1/queues/' . $args['queue'] . '/messages?'
                    . http_build_query(array_diff_key($args, ['queue' => true])),
            ),
            $name === 'replay_all', $name === 'purge' => $this->admin(
                $key,
                'POST',
                '/v1/queues/' . $args['queue'] . '/' . ($name === 'purge' ? 'purge' : 'replay'),
                array_diff_key($args, ['queue' => true]),
            ),
            // Plain HTTP also when a client library runs the suite: for answers a library
            // would hide, such as a 429 it waits out and retries.
            $name === 'publish_http' => $this->admin(
                $key,
                'POST',
                '/v1/queues/' . $args['queue'] . '/messages',
                array_diff_key($args, ['queue' => true]),
            ),
            $name === 'claim_http' => $this->admin(
                $key,
                'POST',
                '/v1/queues/' . $args['queue'] . '/claims',
                (object) array_diff_key($args, ['queue' => true]),
            ),
            $name === 'workers' => $this->admin($key, 'GET', '/v1/queues/' . $args['queue'] . '/workers'),
            $name === 'tenant' => $this->admin($key, 'GET', '/v1/tenant'),
            $name === 'metrics' => $this->http->request('GET', '/metrics'),
            $name === 'tenant_metrics' => $this->admin($key, 'GET', '/v1/metrics'),
            $name === 'history' => $this->admin(
                $key,
                'GET',
                '/v1/queues/' . $args['queue'] . '/history?'
                    . http_build_query(array_diff_key($args, ['queue' => true])),
            ),
            $name === 'tenant_history' => $this->admin($key, 'GET', '/v1/history?' . http_build_query(
                $this->tenantIds($args),
            )),
            $name === 'api_key' => $this->admin($key, 'POST', '/v1/api-keys', $args),
            // Plain HTTP, for answers a library may give before asking the server.
            $name === 'ack_batch_http' => $this->admin($key, 'POST', '/v1/acks', $args),
            $name === 'message' => $this->admin($key, 'GET', '/v1/messages/' . $id),
            $name === 'replay' => $this->admin($key, 'POST', '/v1/messages/' . $id . '/replay'),
            default => throw new \InvalidArgumentException('unknown step ' . $name),
        };

        return [
            'status' => $result['status'],
            'body' => $result['body'],
            'headers' => $result['headers'] ?? [],
            'elapsed' => microtime(true) - $started,
        ];
    }

    /**
     * @param array<string, mixed> $step
     */
    private function stepName(array $step): string
    {
        foreach (array_keys($step) as $key) {
            if (!in_array($key, ['save', 'expect'], true)) {
                return (string) $key;
            }
        }

        throw new \InvalidArgumentException('step without an action');
    }

    /**
     * Replaces "$name.path" strings with values from saved results, at any depth.
     *
     * @param array<string, mixed> $saved
     */
    private function resolve(mixed $value, array $saved): mixed
    {
        if (is_array($value)) {
            return array_map(fn(mixed $item) => $this->resolve($item, $saved), $value);
        }

        if (is_string($value) && str_starts_with($value, '$')) {
            $path = substr($value, 1);
            $name = explode('.', $path, 2)[0];

            if (!array_key_exists($name, $saved)) {
                throw new \InvalidArgumentException('nothing saved as ' . $name);
            }

            $rest = substr($path, strlen($name) + 1);

            return $rest === '' || $rest === false ? $saved[$name] : Expect::dig($saved[$name], $rest)[1];
        }

        return $value;
    }

    /**
     * Publish from a separate process after a delay, while this one waits in a claim.
     *
     * @param array{seconds: int|float, body?: mixed} $publish
     */
    private function publishLater(string $tenant, string $queue, array $publish): void
    {
        $script = sprintf(
            'usleep(%d); $c = curl_init(%s); curl_setopt_array($c, [CURLOPT_POST => true, '
            . 'CURLOPT_POSTFIELDS => %s, CURLOPT_RETURNTRANSFER => true, CURLOPT_HTTPHEADER => '
            . '["Content-Type: application/json", %s]]); curl_exec($c);',
            (int) ($publish['seconds'] * 1_000_000),
            var_export(rtrim($this->baseUrl, '/') . '/v1/queues/' . rawurlencode($queue) . '/messages', true),
            var_export(json_encode(['body' => $publish['body'] ?? new \stdClass()]), true),
            var_export('Authorization: Bearer ' . $this->tenants[$tenant]['key'], true),
        );

        $process = proc_open([PHP_BINARY, '-r', $script], [], $pipes);

        if (!is_resource($process)) {
            throw new \RuntimeException('could not start the delayed publish');
        }
    }

    /**
     * @return array{status: int, body: mixed}
     */
    /**
     * The scenario names tenants by their letter; LessorMQ by their id.
     *
     * @param array<string, mixed> $args
     * @return array<string, mixed>
     */
    private function tenantIds(array $args): array
    {
        if (isset($args['tenants']) && is_array($args['tenants'])) {
            $args['tenants'] = implode(',', array_map(
                fn (string $name) => $this->tenants[$name]['id'] ?? $name,
                $args['tenants'],
            ));
        }

        return $args;
    }

    private function admin(string $key, string $method, string $path, mixed $body = null): array
    {
        return $this->http->request($method, $path, $body, ['Authorization' => 'Bearer ' . $key]);
    }

    private function createTenants(): void
    {
        foreach (['a', 'b'] as $name) {
            $this->tenants[$name] = $this->createTenant($name);
        }
    }

    /**
     * @param array<string, int|null> $quotas
     * @return array{id: string, key: string}
     */
    private function createTenant(string $name, array $quotas = []): array
    {
        $id = sprintf('conformance-%s-%s', $this->runId, $name);
        $body = ['id' => $id, 'name' => 'Conformance ' . $name];

        if ($quotas !== []) {
            $body['quotas'] = $quotas;
        }

        $auth = ['Authorization' => 'Bearer ' . $this->operatorKey];
        $response = $this->http->request('POST', '/v1/tenants', $body, $auth);

        if ($response['status'] !== 201) {
            throw new \RuntimeException('could not create tenant ' . $id . ': ' . json_encode($response['body']));
        }

        return ['id' => $id, 'key' => $response['body']['admin_key']['secret']];
    }

    private function removeTenants(): void
    {
        foreach ($this->tenants as $tenant) {
            $this->removeTenant($tenant);
        }
    }

    /**
     * @param array{id: string, key: string} $tenant
     */
    private function removeTenant(array $tenant): void
    {
        $auth = ['Authorization' => 'Bearer ' . $this->operatorKey];
        $this->http->request('PATCH', '/v1/tenants/' . $tenant['id'], ['status' => 'suspended'], $auth);
        $this->http->request('DELETE', '/v1/tenants/' . $tenant['id'], null, $auth);
    }
}
