<?php

declare(strict_types=1);

namespace LessorMQ\Laravel;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use LessorMQ\ClaimOptions;
use LessorMQ\Client;
use LessorMQ\Exception\LessorMQException;
use LessorMQ\Exception\NotFoundException;
use LessorMQ\Lease;
use LessorMQ\PublishOptions;
use Psr\Log\LoggerInterface;

/**
 * A Laravel queue connection on a LessorMQ server.
 *
 * push() and later() publish, pop() claims (waiting up to wait_seconds for a message,
 * like Redis's block_for) and returns a LessorMQJob, which reports to the server. A job's
 * $tries becomes the message's max_attempts, its deduplicationId() (or deduplicator) the
 * dedup key; everything else about retries is the queue's.
 */
class LessorMQQueue extends Queue implements QueueContract
{
    public function __construct(
        private readonly Client $client,
        private readonly ?LeaseKeeper $keeper,
        private readonly ConnectionOptions $options,
        private readonly LoggerInterface $logger,
    ) {
        $this->dispatchAfterCommit = $options->afterCommit;
    }

    /**
     * Ready, delayed and processing messages, like the size of the other drivers. Needs a
     * key with the consume (or admin) scope.
     */
    public function size($queue = null)
    {
        $stats = $this->stats($queue);

        return ($stats['ready'] ?? 0) + ($stats['delayed'] ?? 0) + ($stats['processing'] ?? 0);
    }

    public function pendingSize($queue = null)
    {
        return $this->stats($queue)['ready'] ?? 0;
    }

    public function delayedSize($queue = null)
    {
        return $this->stats($queue)['delayed'] ?? 0;
    }

    public function reservedSize($queue = null)
    {
        return $this->stats($queue)['processing'] ?? 0;
    }

    /**
     * When the oldest ready message became ready (published, or put back), as a Unix time.
     */
    public function creationTimeOfOldestPendingJob($queue = null)
    {
        $stats = $this->stats($queue);

        return ($stats['ready'] ?? 0) > 0 ? time() - ($stats['oldest_ready_seconds'] ?? 0) : null;
    }

    public function push($job, $data = '', $queue = null)
    {
        $queue = $this->getQueue($queue);

        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $queue, $data),
            $queue,
            null,
            fn(string $payload, string $queue) => $this->publish($payload, $queue, null, $this->dedupKey(
                $job,
                $payload,
                $queue,
            )),
        );
    }

    /**
     * @param array<string, mixed> $options `delay` (seconds, a DateTimeInterface or a
     *                                      DateInterval) and `dedup_key`
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $dedupKey = isset($options['dedup_key']) ? (string) $options['dedup_key'] : null;

        return $this->publish($payload, $this->getQueue($queue), $options['delay'] ?? null, $dedupKey);
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        $queue = $this->getQueue($queue);

        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $queue, $data, $delay),
            $queue,
            $delay,
            fn(string $payload, string $queue, mixed $delay) => $this->publish(
                $payload,
                $queue,
                $delay,
                $this->dedupKey($job, $payload, $queue),
            ),
        );
    }

    /**
     * Claim the next message, waiting up to wait_seconds for one. A server that cannot be
     * reached throws, and the worker reports it and tries again a second later.
     *
     * @return LessorMQJob|null
     */
    public function pop($queue = null)
    {
        $queue = $this->getQueue($queue);
        $lease = $this->client->claim($queue, new ClaimOptions(
            $this->options->consumer,
            $this->options->route,
            $this->options->waitSeconds,
            $this->options->leaseSeconds,
        ));

        if ($lease === null) {
            return null;
        }

        $rawBody = $this->rawBody($lease);

        if ($rawBody === null) {
            // Not a Laravel job (published by something else): no worker here can run it.
            $this->rejectForeign($lease);

            return null;
        }

        return new LessorMQJob(
            $this->container,
            $this->client,
            $this->keeper,
            $this->logger,
            $lease,
            $rawBody,
            $this->connectionName,
            $queue,
        );
    }

    /**
     * The queue's name on the server: the one given, or the connection's.
     */
    public function getQueue($queue): string
    {
        if ($queue instanceof \BackedEnum) {
            $queue = $queue->value;
        } elseif ($queue instanceof \UnitEnum) {
            $queue = $queue->name;
        }

        $queue = $queue === null || $queue === '' ? $this->options->queue : (string) $queue;

        // Laravel 13 can send a job to another queue than it names (Queue::route()).
        return method_exists($this, 'resolveQueue') ? (string) $this->resolveQueue($queue) : $queue;
    }

    public function getClient(): Client
    {
        return $this->client;
    }

    private function publish(string $payload, string $queue, mixed $delay, ?string $dedupKey): string
    {
        // A JSON payload is stored as JSON, so the queue's messages can be read; objects
        // stay objects. Anything else travels as a string.
        $decoded = json_decode($payload);
        $body = is_object($decoded) || is_array($decoded) ? $decoded : $payload;
        $tries = is_object($decoded) ? ($decoded->maxTries ?? null) : null;
        $seconds = $delay === null ? 0 : (int) $this->secondsUntil($delay);

        $published = $this->client->publish($queue, $body, new PublishOptions(
            [],
            $dedupKey,
            $seconds > 0 ? $seconds : null,
            null,
            // The job's $tries is a request for this message's max_attempts; the server
            // caps it at the queue's max_attempts_limit. Zero (no limit) asks nothing.
            is_int($tries) && $tries > 0 ? $tries : null,
        ));

        return $published->id;
    }

    /**
     * The dedup key of a job, from Laravel's own conventions: a deduplicator callback, or a
     * deduplicationId() method, given the payload and the queue.
     */
    private function dedupKey(mixed $job, string $payload, string $queue): ?string
    {
        if (!is_object($job)) {
            return null;
        }

        $key = match (true) {
            isset($job->deduplicator) && is_callable($job->deduplicator) => ($job->deduplicator)($payload, $queue),
            method_exists($job, 'deduplicationId') => $job->deduplicationId($payload, $queue),
            default => null,
        };

        return $key === null || $key === '' ? null : (string) $key;
    }

    /**
     * The job's payload as Laravel wrote it, or null when the message is not one.
     */
    private function rawBody(Lease $lease): ?string
    {
        $body = $lease->message->body;
        $raw = is_string($body)
            ? $body
            : json_encode($body, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION);

        if (!is_string($raw)) {
            return null;
        }

        $payload = json_decode($raw, true);

        return is_array($payload) && is_string($payload['job'] ?? null) ? $raw : null;
    }

    private function rejectForeign(Lease $lease): void
    {
        try {
            $this->client->fail(
                $lease,
                'Not a Laravel job: the body has no "job" to run',
                true,
                null,
                'LessorMQ\\Laravel\\InvalidPayload',
            );
        } catch (LessorMQException $e) {
            $this->logger->error('LessorMQ fail of a message that is not a Laravel job failed', [
                'message_id' => $lease->message->id,
                'error' => $e->getMessage(),
            ]);
        }
    }

    /**
     * @return array<string, int>
     */
    private function stats(mixed $queue): array
    {
        try {
            return $this->client->stats($this->getQueue($queue));
        } catch (NotFoundException) {
            // A queue exists from its first message on.
            return [];
        }
    }
}
