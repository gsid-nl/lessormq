<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests;

use LessorMQ\Laravel\LessorMQServiceProvider;
use LessorMQ\Laravel\Tests\Fixtures\Recorder;
use Orchestra\Testbench\TestCase as Orchestra;

abstract class TestCase extends Orchestra
{
    protected function setUp(): void
    {
        parent::setUp();
        Recorder::reset();
    }

    protected function getPackageProviders($app): array
    {
        return [LessorMQServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('queue.default', 'lessormq');
        $app['config']->set('queue.connections.lessormq', $this->connection());
        // The tests have no database; the failed_jobs table is not what they look at.
        $app['config']->set('queue.failed.driver', 'null');
    }

    /**
     * @return array<string, mixed>
     */
    protected function connection(): array
    {
        return [
            'driver' => 'lessormq',
            'url' => 'https://mq.example.com',
            'key' => 'lmq_test',
            'queue' => 'default',
            'wait_seconds' => 5,
            'consumer' => 'host-1',
        ];
    }
}
