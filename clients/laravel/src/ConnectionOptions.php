<?php

declare(strict_types=1);

namespace LessorMQ\Laravel;

use InvalidArgumentException;
use LessorMQ\Worker;

/**
 * The settings of a `lessormq` connection in config/queue.php. Nothing here decides what
 * happens to a failed job: retries, backoff and the dead-letter set are the queue's.
 */
final class ConnectionOptions
{
    private const KNOWN = [
        'driver', 'connection', 'name', 'url', 'key', 'queue', 'wait_seconds', 'lease_seconds', 'keepalive',
        'consumer', 'route', 'http_retries', 'http_timeout', 'after_commit',
    ];

    /**
     * @param string      $url          the server, such as https://mq.example.com
     * @param string      $key          an API key with the publish and/or consume scope
     * @param string      $queue        the queue when a job names none
     * @param int         $waitSeconds  how long a claim waits for a message (at most 20)
     * @param int|null    $leaseSeconds lease per claim and per extend; the queue's when null
     * @param bool        $keepalive    extend the lease while a job runs (ext-pcntl)
     * @param string      $consumer     shown to administrators; host:pid by default
     * @param string|null $route        the path this worker runs jobs through
     * @param int         $httpRetries  retries after network errors, 5xx and 429
     * @param float       $httpTimeout  seconds one HTTP request may take, above waitSeconds
     */
    public function __construct(
        public readonly string $url,
        #[\SensitiveParameter]
        public readonly string $key,
        public readonly string $queue = 'default',
        public readonly int $waitSeconds = 5,
        public readonly ?int $leaseSeconds = null,
        public readonly bool $keepalive = true,
        public readonly string $consumer = '',
        public readonly ?string $route = null,
        public readonly int $httpRetries = 3,
        public readonly float $httpTimeout = 30.0,
        public readonly ?bool $afterCommit = null,
    ) {
    }

    /**
     * @param array<string, mixed> $config the connection's entry in config/queue.php
     */
    public static function fromConfig(array $config): self
    {
        $unknown = array_diff(array_keys($config), self::KNOWN);

        if ($unknown !== []) {
            // A misspelt option (tries, backoff, retry_after) would otherwise be ignored
            // without a word; retries in particular are the queue's, not the connection's.
            throw new InvalidArgumentException(sprintf(
                'Unknown option(s) on a lessormq queue connection: %s. Known: %s. Retries, backoff and '
                . 'dead-lettering are settings of the queue on the server.',
                implode(', ', $unknown),
                implode(', ', array_diff(self::KNOWN, ['driver', 'connection', 'name'])),
            ));
        }

        foreach (['url', 'key'] as $required) {
            if (!is_string($config[$required] ?? null) || $config[$required] === '') {
                throw new InvalidArgumentException(sprintf(
                    'A lessormq queue connection needs "%s" (LESSORMQ_%s in the example config).',
                    $required,
                    strtoupper($required),
                ));
            }
        }

        $wait = (int) ($config['wait_seconds'] ?? 5);

        if ($wait < 0 || $wait > 20) {
            throw new InvalidArgumentException('wait_seconds of a lessormq connection is 0 to 20.');
        }

        $lease = isset($config['lease_seconds']) && $config['lease_seconds'] !== ''
            ? (int) $config['lease_seconds']
            : null;
        $consumer = isset($config['consumer']) && $config['consumer'] !== ''
            ? (string) $config['consumer']
            : (Worker::defaultConsumer() ?? '');

        return new self(
            $config['url'],
            $config['key'],
            isset($config['queue']) && $config['queue'] !== '' ? (string) $config['queue'] : 'default',
            $wait,
            $lease,
            filter_var($config['keepalive'] ?? true, FILTER_VALIDATE_BOOLEAN),
            $consumer,
            isset($config['route']) && $config['route'] !== '' ? (string) $config['route'] : null,
            (int) ($config['http_retries'] ?? 3),
            // A claim holds the request for up to wait_seconds; the timeout must outlast it.
            max((float) ($config['http_timeout'] ?? 30), $wait + 10.0),
            isset($config['after_commit']) ? (bool) $config['after_commit'] : null,
        );
    }
}
