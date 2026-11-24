<?php

declare(strict_types=1);

namespace LessorMQ\Laravel;

use Illuminate\Queue\Connectors\ConnectorInterface;
use LessorMQ\Client;
use LessorMQ\ClientOptions;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Makes `lessormq` connections from their entry in config/queue.php.
 */
final class LessorMQConnector implements ConnectorInterface
{
    /**
     * The keeper's requests run in a signal handler, so the job waits for them: short,
     * and retried once; a failed extend is tried again at the next alarm anyway.
     */
    private const KEEPALIVE_TIMEOUT = 10.0;
    private const KEEPALIVE_RETRIES = 1;

    public function __construct(
        private readonly HttpClientFactory $http,
        private readonly ?LoggerInterface $logger = null,
    ) {
    }

    /**
     * @param array<string, mixed> $config
     */
    public function connect(array $config): LessorMQQueue
    {
        $options = ConnectionOptions::fromConfig($config);
        $logger = $this->logger ?? new NullLogger();
        $client = $this->client($options, $options->httpTimeout, $options->httpRetries, $logger);
        $keeper = null;

        if ($options->keepalive && LeaseKeeper::supported()) {
            $keeper = new LeaseKeeper(
                $this->client(
                    $options,
                    min(self::KEEPALIVE_TIMEOUT, $options->httpTimeout),
                    min(self::KEEPALIVE_RETRIES, $options->httpRetries),
                    $logger,
                ),
                $options->leaseSeconds,
                $logger,
            );
        }

        return new LessorMQQueue($client, $keeper, $options, $logger);
    }

    private function client(ConnectionOptions $options, float $timeout, int $retries, LoggerInterface $logger): Client
    {
        return new Client(
            $options->url,
            $options->key,
            new ClientOptions(httpRetries: $retries, userAgent: 'lessormq-laravel/1'),
            $this->http->create($timeout),
            null,
            null,
            $logger,
        );
    }
}
