<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests;

use LessorMQ\Laravel\HttpClientFactory;
use LessorMQ\Laravel\LessorMQQueue;
use LessorMQ\Laravel\Tests\Fixtures\FakeHttpFactory;
use LessorMQ\Tests\Fake\FakeHttp;

/**
 * Tests against a scripted server: FakeHttp answers, and records what was sent.
 */
abstract class FakeServerTestCase extends TestCase
{
    protected FakeHttpFactory $http;

    protected function setUp(): void
    {
        parent::setUp();
        $this->http = new FakeHttpFactory();
        $this->app->instance(HttpClientFactory::class, $this->http);
    }

    protected function queue(): LessorMQQueue
    {
        $queue = $this->app['queue']->connection('lessormq');
        $this->assertInstanceOf(LessorMQQueue::class, $queue);

        return $queue;
    }

    protected function server(): FakeHttp
    {
        return $this->http->main;
    }

    /**
     * The payload Laravel publishes for a job, as the server would store it.
     *
     * @return array<string, mixed>
     */
    protected function payloadOf(object $job): array
    {
        $this->server()->reply(201, $this->published());
        $this->queue()->push($job);
        $payload = $this->server()->body(count($this->server()->sent) - 1)['body'];
        $this->server()->sent = [];

        return $payload;
    }

    /**
     * @return array<string, mixed>
     */
    protected function published(string $id = 'm-1'): array
    {
        return [
            'id' => $id,
            'queue' => 'default',
            'status' => 'pending',
            'created_at' => '2026-11-24T10:00:00Z',
            'available_at' => '2026-11-24T10:00:00Z',
            'duplicate' => false,
        ];
    }

    /**
     * @param array<string, mixed>|string $body
     * @return array<string, mixed>
     */
    protected function lease(array|string $body, int $attempt = 1, int $leaseSeconds = 300): array
    {
        return [
            'receipt' => 'r-1',
            'lease_until' => gmdate('Y-m-d\TH:i:s\Z', time() + $leaseSeconds),
            'message' => [
                'id' => 'm-1',
                'queue' => 'default',
                'body' => $body,
                'headers' => [],
                'attempt' => $attempt,
                'max_attempts' => 7,
                'created_at' => '2026-11-24T10:00:00Z',
            ],
        ];
    }

    /**
     * @return array<string, mixed>
     */
    protected function failed(string $outcome = 'retrying', int $attempt = 1): array
    {
        return [
            'outcome' => $outcome,
            'available_at' => $outcome === 'retrying' ? gmdate('Y-m-d\TH:i:s\Z', time() + 10) : null,
            'attempt' => $attempt,
            'max_attempts' => 7,
        ];
    }

    /**
     * @return list<string> "METHOD /path" of every request to the main client
     */
    protected function paths(?FakeHttp $http = null): array
    {
        $http ??= $this->server();

        return array_map(fn(int $i) => $http->path($i), array_keys($http->sent));
    }

    /**
     * Claim the job (as the scripted lease) and run it with queue:work --once.
     *
     * @param array<string, mixed> $options more queue:work options
     */
    protected function work(array $options = []): void
    {
        $this->artisan('queue:work', ['connection' => 'lessormq', '--once' => true, '--sleep' => 0] + $options)
            ->run();
    }
}
