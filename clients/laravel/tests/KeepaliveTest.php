<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests;

use Illuminate\Queue\TimeoutExceededException;
use Illuminate\Queue\Worker;
use LessorMQ\Client;
use LessorMQ\Laravel\LeaseKeeper;
use LessorMQ\Laravel\Tests\Fixtures\Recorder;
use LessorMQ\Laravel\Tests\Fixtures\SlowJob;
use LessorMQ\Lease;
use LessorMQ\Tests\Fake\FakeHttp;
use Nyholm\Psr7\Response;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use Psr\Http\Message\RequestInterface;
use Psr\Log\NullLogger;

/**
 * The lease is extended while a job runs, and queue:work's timeout still works. These
 * tests take real seconds: pcntl_alarm counts in whole ones.
 */
#[RequiresPhpExtension('pcntl')]
class KeepaliveTest extends FakeServerTestCase
{
    protected function tearDown(): void
    {
        Worker::killUsing(null);
        pcntl_alarm(0);
        pcntl_signal(SIGALRM, SIG_DFL);
        parent::tearDown();
    }

    public function testALongJobKeepsItsLease(): void
    {
        $this->app['config']->set('queue.connections.lessormq.lease_seconds', 3);
        $this->extendsAnswered($this->http->keepalive, 10);
        // A 3 s lease is extended every second; the job takes 2.5 s.
        $this->server()->reply(200, $this->lease($this->payloadOf(new SlowJob(2.5)), leaseSeconds: 3))->reply(204);

        $this->work();

        $this->assertSame([1], Recorder::of('handled'));
        $this->assertSame(['POST /v1/queues/default/claims', 'POST /v1/messages/m-1/ack'], $this->paths());
        $extends = $this->paths($this->http->keepalive);
        $this->assertGreaterThanOrEqual(2, count($extends));
        $this->assertSame('POST /v1/messages/m-1/extend', $extends[0]);
        $this->assertSame(['receipt' => 'r-1', 'lease_seconds' => 3], $this->http->keepalive->body(0));
    }

    public function testAFailedExtendDoesNotFailTheJob(): void
    {
        $this->app['config']->set('queue.connections.lessormq.http_retries', 0);

        for ($i = 0; $i < 10; $i++) {
            $this->http->keepalive->failNetwork();
        }

        $this->server()->reply(200, $this->lease($this->payloadOf(new SlowJob(2.2)), leaseSeconds: 3))->reply(204);

        $this->work();

        $this->assertSame([1], Recorder::of('handled'));
        $this->assertSame('POST /v1/messages/m-1/ack', $this->paths()[1]);
        $this->assertNotEmpty($this->http->keepalive->sent);
    }

    public function testTheWorkersTimeoutStillEndsTheJobAndCountsAsAFailedAttempt(): void
    {
        // The worker ends the process on a timeout; here it throws instead, which ends the job.
        Worker::killUsing(static function (): void {
            throw new \RuntimeException('killed');
        });
        $this->extendsAnswered($this->http->keepalive, 10);
        $this->http->keepalive->reply(200, $this->failed('retrying', 1));
        $this->server()->reply(200, $this->lease($this->payloadOf(new SlowJob(6)), leaseSeconds: 3));

        $started = microtime(true);
        // Not --once: only the daemon arms the timeout. --max-jobs stops it after this job.
        $this->artisan('queue:work', [
            'connection' => 'lessormq',
            '--timeout' => 3,
            '--max-jobs' => 1,
            '--sleep' => 0,
        ])->run();

        $this->assertLessThan(5, microtime(true) - $started, 'ended at the timeout');
        $this->assertSame([], Recorder::of('handled'));
        $this->assertSame(['POST /v1/queues/default/claims'], $this->paths(), 'nothing more on the main client');
        $calls = $this->paths($this->http->keepalive);
        $this->assertSame('POST /v1/messages/m-1/extend', $calls[0], 'extended before the timeout');
        $this->assertSame('POST /v1/messages/m-1/fail', end($calls));
        $fail = $this->http->keepalive->body(count($calls) - 1);
        $this->assertFalse($fail['permanent'], 'failOnTimeout decides nothing');
        $this->assertSame(TimeoutExceededException::class, $fail['error']['type']);
    }

    public function testTheKeeperHandsTheAlarmBackAndCallsTheOwnHandlerAtItsDeadline(): void
    {
        $fired = 0;
        pcntl_signal(SIGALRM, function () use (&$fired): void {
            $fired++;
        });
        pcntl_alarm(2);
        $http = new FakeHttp();
        $this->extendsAnswered($http, 10);
        $timedOut = 0;
        $keeper = new LeaseKeeper(new Client('https://mq.example.com', 'k', null, $http), null, new NullLogger());
        $lease = Lease::fromArray($this->lease(['job' => 'X'], leaseSeconds: 3));

        $stop = $keeper->keep($lease, function () use (&$timedOut): void {
            $timedOut++;
        });
        $this->waitFor(2.5);
        $stop();

        $this->assertSame(1, $timedOut);
        $this->assertSame(1, $fired, "the worker's own handler ran at its deadline");
        $this->assertNotEmpty($http->sent, 'extended before');
        $this->assertSame(0, pcntl_alarm(0), 'no alarm left after the deadline');
    }

    public function testStoppingBeforeTheDeadlineRestoresTheWorkersAlarm(): void
    {
        $handler = static function (): void {
        };
        pcntl_signal(SIGALRM, $handler);
        pcntl_alarm(30);
        $client = new Client('https://mq.example.com', 'k', null, new FakeHttp());
        $keeper = new LeaseKeeper($client, null, new NullLogger());

        $stop = $keeper->keep(Lease::fromArray($this->lease(['job' => 'X'])), static function (): void {
        });
        $stop();

        $this->assertSame($handler, pcntl_signal_get_handler(SIGALRM));
        $this->assertEqualsWithDelta(30, pcntl_alarm(0), 1);
    }

    private function extendsAnswered(FakeHttp $http, int $times): void
    {
        for ($i = 0; $i < $times; $i++) {
            $http->answer(static fn(RequestInterface $request) => new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) json_encode(['lease_until' => gmdate('Y-m-d\TH:i:s\Z', time() + 3)]),
            ));
        }
    }

    private function waitFor(float $seconds): void
    {
        $until = microtime(true) + $seconds;

        while (microtime(true) < $until) {
            usleep(50_000);
        }
    }
}
