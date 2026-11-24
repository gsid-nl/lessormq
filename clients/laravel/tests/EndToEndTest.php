<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests;

use Illuminate\Http\Client\PendingRequest;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Queue;
use LessorMQ\Laravel\Tests\Fixtures\FailingJob;
use LessorMQ\Laravel\Tests\Fixtures\PermanentJob;
use LessorMQ\Laravel\Tests\Fixtures\Recorder;
use LessorMQ\Laravel\Tests\Fixtures\SlowJob;
use LessorMQ\Laravel\Tests\Fixtures\SucceedingJob;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

/**
 * Laravel's worker against a real server, in a tenant of its own:
 *
 *   LESSORMQ_URL=http://127.0.0.1:8080 LESSORMQ_OPERATOR_KEY=lmq_... vendor/bin/phpunit --group e2e
 *
 * The server needs LEASE_SWEEP_SECONDS=1 (as for the conformance suite), or an expired
 * lease is noticed too late for the keepalive test to mean anything.
 */
#[Group('e2e')]
class EndToEndTest extends TestCase
{
    private string $url = '';
    private string $operatorKey = '';
    private string $tenant = '';
    private string $key = '';
    private string $queueName = '';

    protected function setUp(): void
    {
        $this->url = (string) getenv('LESSORMQ_URL');
        $this->operatorKey = (string) getenv('LESSORMQ_OPERATOR_KEY');

        if ($this->url === '' || $this->operatorKey === '') {
            $this->markTestSkipped('Set LESSORMQ_URL and LESSORMQ_OPERATOR_KEY to run against a server.');
        }

        parent::setUp();

        $this->tenant = 'laravel-e2e-' . bin2hex(random_bytes(4));
        $created = $this->operator()->post('/v1/tenants', ['id' => $this->tenant, 'name' => 'Laravel e2e']);
        $this->assertSame(201, $created->status(), $created->body());
        $this->key = $created->json('admin_key.secret');
        $this->queueName = 'jobs';

        $this->app['config']->set('queue.connections.lessormq', [
            'driver' => 'lessormq',
            'url' => $this->url,
            'key' => $this->key,
            'queue' => $this->queueName,
            'wait_seconds' => 1,
        ]);
    }

    protected function tearDown(): void
    {
        if ($this->tenant !== '') {
            $this->operator()->patch('/v1/tenants/' . $this->tenant, ['status' => 'suspended']);
            $this->operator()->delete('/v1/tenants/' . $this->tenant);
        }

        parent::tearDown();
    }

    public function testAJobIsRunByTheWorkerAndAcked(): void
    {
        $id = Queue::push(new SucceedingJob('Ada'));

        $this->work();

        $this->assertSame([['name' => 'Ada', 'attempt' => 1]], Recorder::of('handled'));
        $message = $this->message($id);
        $this->assertSame('completed', $message['status']);
        $this->assertSame(SucceedingJob::class, $message['body']['displayName'], 'stored as JSON');
    }

    public function testAFailingJobComesBackAfterTheQueuesBackoffWithTheServersAttemptCount(): void
    {
        $this->settings(['max_attempts' => 3, 'backoff' => ['initial_seconds' => 1, 'multiplier' => 2, 'jitter' => 0]]);
        // A claim waits up to 3 s: long enough for the backoff (1 s, then 2 s) to run out.
        $this->app['config']->set('queue.connections.lessormq.wait_seconds', 3);
        $id = Queue::push(new FailingJob());

        $this->work(['--tries' => 1]);

        $message = $this->message($id);
        $this->assertSame('pending', $message['status'], 'the queue retries, whatever --tries says');
        $this->assertSame(1, $message['attempts']);
        $this->assertSame('RuntimeException', $message['last_error']['type']);
        $this->assertSame('upstream 503', $message['last_error']['message']);

        $this->work(['--tries' => 1]);
        $this->assertSame('pending', $this->message($id)['status']);
        $this->assertSame([], Recorder::of('failed'));

        $this->work(['--tries' => 1]);

        $this->assertSame([1, 2, 3], Recorder::of('attempt'), "attempts() is the server's count");
        $message = $this->message($id);
        $this->assertSame('failed', $message['status'], 'max_attempts reached: the dead-letter set');
        $this->assertSame(3, $message['attempts']);
        $this->assertSame(['RuntimeException'], Recorder::of('failed'), "failed() once, when the server gave up");
    }

    public function testAPermanentFailureGoesToTheDeadLetterSet(): void
    {
        $id = Queue::push(new PermanentJob());

        $this->work();

        $message = $this->message($id);
        $this->assertSame('failed', $message['status']);
        $this->assertSame(1, $message['attempts']);
        $this->assertTrue($message['last_error']['permanent']);
        $this->assertSame(['LessorMQ\\PermanentFailureException'], Recorder::of('failed'));
    }

    #[RequiresPhpExtension('pcntl')]
    public function testAJobLongerThanItsLeaseKeepsItAndIsAcked(): void
    {
        // A 3 s lease (it ends on a whole second, so 2 to 3 s from the claim) and a 5 s
        // job: without the keepalive the server takes the message back, counts the
        // attempt, and the ack says lease_lost.
        $this->app['config']->set('queue.connections.lessormq.lease_seconds', 3);
        $id = Queue::push(new SlowJob(5));

        $this->work();

        $this->assertSame([1], Recorder::of('handled'));
        $message = $this->message($id);
        $this->assertSame('completed', $message['status']);
        $this->assertSame(1, $message['attempts']);
    }

    /**
     * @param array<string, mixed> $options
     */
    private function work(array $options = []): void
    {
        $this->artisan('queue:work', ['connection' => 'lessormq', '--once' => true, '--sleep' => 0] + $options)
            ->run();
    }

    /**
     * @return array<string, mixed>
     */
    private function message(string $id): array
    {
        $response = $this->admin()->get('/v1/messages/' . $id);
        $this->assertSame(200, $response->status(), $response->body());

        return $response->json();
    }

    /**
     * @param array<string, mixed> $settings
     */
    private function settings(array $settings): void
    {
        $response = $this->admin()->patch('/v1/queues/' . $this->queueName, $settings);
        $this->assertSame(200, $response->status(), $response->body());
    }

    private function admin(): PendingRequest
    {
        return Http::baseUrl($this->url)->withToken($this->key)->acceptJson();
    }

    private function operator(): PendingRequest
    {
        return Http::baseUrl($this->url)->withToken($this->operatorKey)->acceptJson();
    }
}
