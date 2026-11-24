<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests;

use Illuminate\Support\Facades\Queue;
use LessorMQ\Laravel\LessorMQQueue;
use LessorMQ\Laravel\Tests\Fixtures\OptionsJob;
use LessorMQ\Laravel\Tests\Fixtures\SucceedingJob;

class QueueTest extends FakeServerTestCase
{
    public function testTheDriverIsRegistered(): void
    {
        $this->assertInstanceOf(LessorMQQueue::class, Queue::connection('lessormq'));
    }

    public function testAnUnknownOptionIsRefused(): void
    {
        $this->app['config']->set('queue.connections.retrying', ['retry_after' => 90] + $this->connection());

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('retry_after');

        Queue::connection('retrying');
    }

    public function testDispatchPublishesLaravelsPayloadAsJson(): void
    {
        $this->server()->reply(201, $this->published('m-42'));

        $id = Queue::connection('lessormq')->push(new SucceedingJob('Ada'));

        $sent = $this->server()->body(0);
        $this->assertSame('m-42', $id);
        $this->assertSame('POST /v1/queues/default/messages', $this->server()->path(0));
        $this->assertSame('Illuminate\\Queue\\CallQueuedHandler@call', $sent['body']['job']);
        $this->assertSame(SucceedingJob::class, $sent['body']['displayName']);
        $this->assertStringStartsWith('lmq-auto-', $sent['dedup_key'], 'safe to retry');
        $this->assertArrayNotHasKey('max_attempts', $sent, 'the queue decides');
        $this->assertArrayNotHasKey('delay_seconds', $sent);
        $this->assertSame('lessormq-laravel/1', $this->server()->sent[0]->getHeaderLine('User-Agent'));
    }

    public function testDelayTriesAndDeduplicationIdBecomeTheMessagesOptions(): void
    {
        $this->server()->reply(201, $this->published());

        OptionsJob::dispatch(7)->onQueue('orders')->delay(90);

        $sent = $this->server()->body(0);
        $this->assertSame('POST /v1/queues/orders/messages', $this->server()->path(0));
        $this->assertSame(90, $sent['delay_seconds']);
        $this->assertSame(3, $sent['max_attempts'], '$tries asks for max_attempts');
        $this->assertSame('order-7@orders', $sent['dedup_key']);
    }

    public function testADeduplicatorBecomesTheDedupKey(): void
    {
        $job = new SucceedingJob('Ada');

        $this->server()->reply(201, $this->published());

        Queue::connection('lessormq')->push($job->withDeduplicator(fn(string $payload, string $queue) => 'greet-ada'));

        $this->assertSame('greet-ada', $this->server()->body(0)['dedup_key']);
    }

    public function testAnEmptyObjectInARawPayloadStaysAnObject(): void
    {
        $this->server()->reply(201, $this->published());

        Queue::connection('lessormq')->pushRaw('{"job":"X@handle","data":{},"list":[]}', 'raw', ['delay' => 5]);

        $this->assertStringContainsString('"data":{},"list":[]', $this->server()->raw(0));
        $this->assertSame(5, $this->server()->body(0)['delay_seconds']);
    }

    public function testPopClaimsWithTheConnectionsSettings(): void
    {
        $this->app['config']->set('queue.connections.lessormq.lease_seconds', 120);
        $this->app['config']->set('queue.connections.lessormq.route', 'vpn-nl-03');
        $payload = $this->payloadOf(new SucceedingJob());
        $this->server()->reply(200, $this->lease($payload, attempt: 3));

        $job = Queue::connection('lessormq')->pop('mail');

        $this->assertSame('POST /v1/queues/mail/claims', $this->server()->path(0));
        $this->assertSame(
            ['consumer' => 'host-1', 'route' => 'vpn-nl-03', 'wait_seconds' => 5, 'lease_seconds' => 120],
            $this->server()->body(0),
        );
        $this->assertSame('m-1', $job->getJobId());
        $this->assertSame(3, $job->attempts(), "the server's count");
        $this->assertSame(json_decode($job->getRawBody(), true), $payload);
        $this->assertSame(0, $job->maxTries(), 'no limit for the worker');
        $this->assertNull($job->retryUntil());
        $this->assertNull($job->maxExceptions());
    }

    public function testPopReturnsNullWhenNothingArrived(): void
    {
        $this->server()->reply(204);

        $this->assertNull(Queue::connection('lessormq')->pop());
    }

    public function testAMessageThatIsNoLaravelJobIsFailedAsPermanent(): void
    {
        $this->server()->reply(200, $this->lease(['to' => 'ada@example.com']))
            ->reply(200, $this->failed('dead_lettered'));

        $this->assertNull(Queue::connection('lessormq')->pop());
        $this->assertSame('POST /v1/messages/m-1/fail', $this->server()->path(1));
        $this->assertTrue($this->server()->body(1)['permanent']);
    }

    public function testSizesComeFromTheQueuesStats(): void
    {
        $stats = ['stats' => ['ready' => 4, 'delayed' => 2, 'processing' => 3, 'oldest_ready_seconds' => 60]];

        for ($i = 0; $i < 5; $i++) {
            $this->server()->reply(200, $stats);
        }

        $this->server()->reply(404, ['code' => 'not_found', 'title' => 'Not found']);

        $queue = Queue::connection('lessormq');
        $this->assertSame(9, $queue->size());
        $this->assertSame(4, $queue->pendingSize());
        $this->assertSame(2, $queue->delayedSize());
        $this->assertSame(3, $queue->reservedSize());
        $this->assertEqualsWithDelta(time() - 60, $queue->creationTimeOfOldestPendingJob(), 1);
        $this->assertSame(0, $queue->size('never-used'));
    }
}
