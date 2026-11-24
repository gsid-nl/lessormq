<?php

declare(strict_types=1);

namespace LessorMQ\Laravel;

use Http\Discovery\Psr18ClientDiscovery;
use Psr\Http\Client\ClientInterface;

/**
 * Makes the HTTP clients of a connection: a new one every time, never the app's.
 *
 * The lease is extended from a SIGALRM handler, in the middle of whatever the job is
 * doing, and that may be a request of its own: on the app's HTTP client, or on this
 * connection's when the job dispatches another job. A client that is still processing a
 * request must not be asked for a second one (Symfony HttpClient refuses it), so the
 * extends get a client that nothing else uses.
 *
 * Guzzle, which every Laravel app has, gets a timeout, so a claim on a connection that
 * died does not hang the worker; without Guzzle any discovered PSR-18 client is used.
 * Bind a subclass in the container to bring your own.
 */
class HttpClientFactory
{
    /**
     * @param float $timeout seconds a request may take in all; longer than a claim's wait
     */
    public function create(float $timeout): ClientInterface
    {
        if (class_exists(\GuzzleHttp\Client::class)) {
            return new \GuzzleHttp\Client([
                'timeout' => $timeout,
                'connect_timeout' => min(10.0, $timeout),
            ]);
        }

        return Psr18ClientDiscovery::find();
    }
}
