<?php

declare(strict_types=1);

namespace LessorMQ\Laravel;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Queue\ManuallyFailedException;
use Illuminate\Queue\TimeoutExceededException;
use LessorMQ\Client;
use LessorMQ\Exception\LeaseLostException;
use LessorMQ\Exception\LessorMQException;
use LessorMQ\FailResult;
use LessorMQ\Lease;
use LessorMQ\PermanentFailure;
use Psr\Log\LoggerInterface;
use Throwable;

/**
 * A job claimed from LessorMQ. It reports, the queue decides:
 *
 * - delete() (the job succeeded) acks the message;
 * - an exception fails it as retryable, with the exception's class and message, or as
 *   permanent when the exception implements LessorMQ\PermanentFailure;
 * - fail() fails it as permanent;
 * - release($delay) from inside the job gives the message back for $delay seconds,
 *   without counting the attempt.
 *
 * Whether and when the message comes back is the queue's decision, so the worker's
 * tries, backoff, maxExceptions, retryUntil and failOnTimeout have no say here: the
 * methods it asks return "no limit". Laravel's failure handling (the job's failed()
 * method, batches and chains, JobFailed and the failed_jobs table) runs once the server
 * says it gave up on the message, not before.
 */
class LessorMQJob extends Job implements JobContract
{
    /** The server has the outcome of this attempt (or the lease is gone): report nothing more. */
    private bool $reported = false;

    /** Inside fire(): a release() now comes from the job, not from the worker. */
    private bool $firing = false;

    private ?Throwable $exception = null;

    public function __construct(
        Container $container,
        private readonly Client $client,
        private readonly ?LeaseKeeper $keeper,
        private readonly LoggerInterface $logger,
        private readonly Lease $lease,
        private readonly string $rawBody,
        ?string $connectionName,
        string $queue,
    ) {
        $this->container = $container;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
    }

    public function getJobId()
    {
        return $this->lease->message->id;
    }

    public function getRawBody()
    {
        return $this->rawBody;
    }

    /**
     * The server's count: this claim's attempt, an expired lease included, a release not.
     */
    public function attempts()
    {
        return $this->lease->message->attempt;
    }

    /**
     * Zero is "no limit" to the worker, so neither the job's $tries nor queue:work --tries
     * fails the job; the queue's max_attempts does (the job's $tries became it at dispatch).
     */
    public function maxTries()
    {
        return 0;
    }

    public function maxExceptions()
    {
        return null;
    }

    public function retryUntil()
    {
        return null;
    }

    /**
     * A timeout is reported as a failed attempt, like any other; the queue decides.
     */
    public function shouldFailOnTimeout()
    {
        return false;
    }

    public function getLease(): Lease
    {
        return $this->lease;
    }

    /**
     * Keep the lease for another lease_seconds (or the queue's lease). Only for jobs that
     * run longer than the lease where the worker cannot do it on its own (no ext-pcntl).
     *
     * @throws LeaseLostException when the lease already ended
     */
    public function extend(?int $leaseSeconds = null): void
    {
        $this->client->extend($this->lease, $leaseSeconds);
    }

    public function fire()
    {
        $stop = $this->keeper?->keep($this->lease, fn(Client $client) => $this->timedOut($client));
        $this->firing = true;

        try {
            parent::fire();
        } catch (Throwable $e) {
            $this->exception = $e;

            throw $e;
        } finally {
            $this->firing = false;

            if ($stop !== null) {
                $stop();
            }
        }
    }

    /**
     * The job succeeded (or deleted itself): ack.
     */
    public function delete()
    {
        parent::delete();

        if ($this->reported) {
            return;
        }

        $this->reported = true;
        $this->report('ack', fn() => $this->client->ack($this->lease));
    }

    /**
     * From inside the job (or a middleware such as RateLimited): give the message back
     * for $delay seconds; the attempt does not count. From the worker, after an exception,
     * with its own backoff: the exception is reported instead, and the queue's backoff
     * applies.
     */
    public function release($delay = 0)
    {
        parent::release($delay);

        if ($this->reported) {
            return;
        }

        if (!$this->firing) {
            // Normally reported already, by the JobExceptionOccurred listener.
            $this->failedWith($this->exception ?? new \RuntimeException('The job failed'));

            return;
        }

        $this->reported = true;
        $seconds = max(0, (int) $this->secondsUntil($delay));
        $this->report('release', fn() => $this->client->release($this->lease, $seconds));
    }

    /**
     * Report a permanent failure: the job (or Laravel, for a model that no longer exists)
     * says the input cannot be processed. The queue's on_permanent decides what follows;
     * Laravel's failure handling runs when the server gave up on the message.
     *
     * @param Throwable|null $e
     */
    public function fail($e = null)
    {
        if ($this->reported) {
            $this->markAsFailed();

            return;
        }

        $result = $this->reportFailure(
            $e ?? new ManuallyFailedException('The job called fail()'),
            true,
            $this->client,
        );

        if ($result === null || $result->outcome === FailResult::RETRYING) {
            // Back in the queue, or the lease is gone: done with it here, but not failed.
            $this->released = true;

            return;
        }

        parent::fail($e);
    }

    /**
     * @internal An exception escaped the job: called by the JobExceptionOccurred listener,
     *           after the worker had its say (it may have called fail() already).
     */
    public function failedWith(Throwable $e): void
    {
        if ($this->reported) {
            return;
        }

        $result = $this->reportFailure($e, $e instanceof PermanentFailure, $this->client);

        if ($result !== null && $result->outcome !== FailResult::RETRYING) {
            parent::fail($e);
        }
    }

    /**
     * The worker's timeout for this job is up; its handler ends the process next. Runs in
     * the signal handler, so it reports through the keeper's client.
     */
    private function timedOut(Client $client): void
    {
        if ($this->reported) {
            return;
        }

        $e = TimeoutExceededException::forJob($this);
        $result = $this->reportFailure($e, false, $client);

        if ($result !== null && $result->outcome !== FailResult::RETRYING) {
            parent::fail($e);
        }
    }

    /**
     * @return FailResult|null what the server did; null when it could not be told
     */
    private function reportFailure(Throwable $e, bool $permanent, Client $client): ?FailResult
    {
        $this->reported = true;
        $result = null;

        $this->report('fail', function () use ($client, $e, $permanent, &$result): void {
            $result = $client->fail($this->lease, $e, $permanent);
            $this->logger->info('LessorMQ job failed', [
                'message_id' => $this->lease->message->id,
                'job' => $this->resolveName(),
                'error' => $e->getMessage(),
                'permanent' => $permanent,
                'outcome' => $result->outcome,
                'attempt' => $result->attempt,
                'max_attempts' => $result->maxAttempts,
            ]);
        });

        return $result;
    }

    /**
     * Ack, fail and release never take the worker down: a lost lease means the message is
     * someone else's now, and any other failure lets the lease expire so it comes back.
     */
    private function report(string $what, callable $call): void
    {
        try {
            $call();
        } catch (LeaseLostException) {
            $this->logger->warning('LessorMQ lease lost before ' . $what . '; the message is no longer ours', [
                'message_id' => $this->lease->message->id,
            ]);
        } catch (LessorMQException $e) {
            $this->logger->error('LessorMQ ' . $what . ' failed; the lease will expire and the message come back', [
                'message_id' => $this->lease->message->id,
                'error' => $e->getMessage(),
            ]);
        }
    }
}
