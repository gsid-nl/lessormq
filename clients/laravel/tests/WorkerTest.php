<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobReleasedAfterException;
use Illuminate\Support\Facades\Event;
use LessorMQ\Laravel\Tests\Fixtures\FailingJob;
use LessorMQ\Laravel\Tests\Fixtures\FailsItselfJob;
use LessorMQ\Laravel\Tests\Fixtures\PermanentJob;
use LessorMQ\Laravel\Tests\Fixtures\Recorder;
use LessorMQ\Laravel\Tests\Fixtures\ReleasingJob;
use LessorMQ\Laravel\Tests\Fixtures\SucceedingJob;
use LessorMQ\PermanentFailureException;

/**
 * queue:work against a scripted server: what the worker reports, and that Laravel's own
 * retry settings decide nothing.
 */
class WorkerTest extends FakeServerTestCase
{
    /** @var list<object> */
    private array $events = [];

    protected function setUp(): void
    {
        parent::setUp();

        foreach ([JobFailed::class, JobReleasedAfterException::class] as $event) {
            Event::listen($event, fn(object $e) => $this->events[] = $e);
        }
    }

    public function testAJobThatSucceedsIsAcked(): void
    {
        $this->server()->reply(200, $this->lease($this->payloadOf(new SucceedingJob('Ada'))))->reply(204);

        $this->work();

        $this->assertSame([['name' => 'Ada', 'attempt' => 1]], Recorder::of('handled'));
        $this->assertSame(['POST /v1/queues/default/claims', 'POST /v1/messages/m-1/ack'], $this->paths());
        $this->assertSame(['receipt' => 'r-1'], $this->server()->body(1));
    }

    public function testAnExceptionIsReportedAndLaravelsRetrySettingsDecideNothing(): void
    {
        // FailingJob has $tries = 1, $maxExceptions = 1, a past retryUntil, a backoff and
        // failOnTimeout, and the worker runs with --tries=1: Laravel would fail it for good.
        $this->server()->reply(200, $this->lease($this->payloadOf(new FailingJob()), attempt: 3))
            ->reply(200, $this->failed('retrying', 3));

        $this->work(['--tries' => 1]);

        $this->assertSame([3], Recorder::of('attempt'), "attempts() is the server's");
        $this->assertSame(['POST /v1/queues/default/claims', 'POST /v1/messages/m-1/fail'], $this->paths());
        $fail = $this->server()->body(1);
        $this->assertFalse($fail['permanent']);
        $this->assertSame('RuntimeException', $fail['error']['type']);
        $this->assertSame('upstream 503', $fail['error']['message']);
        $this->assertSame([], Recorder::of('failed'), 'the queue tries again: no failed()');
        $this->assertSame([JobReleasedAfterException::class], array_map('get_class', $this->events));
    }

    public function testAnExceptionTheServerGivesUpOnRunsLaravelsFailureHandlingOnce(): void
    {
        $this->server()->reply(200, $this->lease($this->payloadOf(new FailingJob()), attempt: 7))
            ->reply(200, $this->failed('dead_lettered', 7));

        $this->work();

        $this->assertCount(2, $this->server()->sent, 'no ack, no release');
        $this->assertSame(['RuntimeException'], Recorder::of('failed'));
        $this->assertSame([JobFailed::class], array_map('get_class', $this->events));
    }

    public function testAPermanentFailureExceptionIsReportedAsPermanent(): void
    {
        $this->server()->reply(200, $this->lease($this->payloadOf(new PermanentJob())))
            ->reply(200, $this->failed('dead_lettered'));

        $this->work();

        $fail = $this->server()->body(1);
        $this->assertTrue($fail['permanent']);
        $this->assertSame(PermanentFailureException::class, $fail['error']['type']);
        $this->assertSame([PermanentFailureException::class], Recorder::of('failed'));
    }

    public function testFailIsPermanentAndRunsFailureHandlingWhenTheServerGaveUp(): void
    {
        $this->server()->reply(200, $this->lease($this->payloadOf(new FailsItselfJob())))
            ->reply(200, $this->failed('dead_lettered'));

        $this->work();

        $this->assertSame(['POST /v1/queues/default/claims', 'POST /v1/messages/m-1/fail'], $this->paths());
        $this->assertTrue($this->server()->body(1)['permanent']);
        $this->assertSame('order 7 has no lines', $this->server()->body(1)['error']['message']);
        $this->assertSame([\InvalidArgumentException::class], Recorder::of('failed'));
        $this->assertSame([JobFailed::class], array_map('get_class', $this->events));
    }

    public function testFailOnAQueueThatRetriesPermanentFailuresRunsNoFailureHandling(): void
    {
        // on_permanent: retry. The message comes back; Laravel's failure handling waits.
        $this->server()->reply(200, $this->lease($this->payloadOf(new FailsItselfJob())))
            ->reply(200, $this->failed('retrying'));

        $this->work();

        $this->assertCount(2, $this->server()->sent, 'not acked');
        $this->assertSame([], Recorder::of('failed'));
        $this->assertSame([], $this->events);
    }

    public function testAnExceptionLaravelIsToldNotToRetryIsPermanent(): void
    {
        $handler = $this->app->make(ExceptionHandler::class);

        if (!method_exists($handler, 'dontRetry')) {
            $this->markTestSkipped('dontRetry() is Laravel 13 and up.');
        }

        $handler->dontRetry(\RuntimeException::class);
        $this->server()->reply(200, $this->lease($this->payloadOf(new FailingJob())))
            ->reply(200, $this->failed('dead_lettered'));

        $this->work();

        $this->assertCount(2, $this->server()->sent);
        $this->assertTrue($this->server()->body(1)['permanent']);
        $this->assertSame(['RuntimeException'], Recorder::of('failed'));
    }

    public function testReleaseFromTheJobGivesTheMessageBackWithItsDelay(): void
    {
        $this->server()->reply(200, $this->lease($this->payloadOf(new ReleasingJob())))->reply(204);

        $this->work();

        $this->assertSame(['POST /v1/queues/default/claims', 'POST /v1/messages/m-1/release'], $this->paths());
        $this->assertSame(['receipt' => 'r-1', 'delay_seconds' => 30], $this->server()->body(1));
    }

    public function testALostLeaseOnAckIsLoggedNotThrown(): void
    {
        $this->server()->reply(200, $this->lease($this->payloadOf(new SucceedingJob())))
            ->reply(409, ['code' => 'lease_lost', 'title' => 'Lease lost']);

        $this->work();

        $this->assertCount(1, Recorder::of('handled'));
        $this->assertCount(2, $this->server()->sent, 'no fail after a lost lease');
        $this->assertSame([], $this->events);
    }

    public function testAServerThatCannotBeReachedDoesNotStopTheWorker(): void
    {
        $this->app['config']->set('queue.connections.lessormq.http_retries', 0);
        $this->server()->failNetwork();

        $this->work();

        $this->assertSame(['POST /v1/queues/default/claims'], $this->paths());
    }
}
