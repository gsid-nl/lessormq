<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests\Fixtures;

use LessorMQ\Laravel\HttpClientFactory;
use LessorMQ\Tests\Fake\FakeHttp;
use Psr\Http\Client\ClientInterface;

/**
 * Hands out the fake HTTP clients of a connection: the main one first, then the one the
 * lease keeper uses from its signal handler.
 */
final class FakeHttpFactory extends HttpClientFactory
{
    /** @var list<float> */
    public array $timeouts = [];

    public function __construct(
        public readonly FakeHttp $main = new FakeHttp(),
        public readonly FakeHttp $keepalive = new FakeHttp(),
    ) {
    }

    public function create(float $timeout): ClientInterface
    {
        $this->timeouts[] = $timeout;

        return count($this->timeouts) === 1 ? $this->main : $this->keepalive;
    }
}
