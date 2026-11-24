<?php

declare(strict_types=1);

namespace LessorMQ\Laravel;

use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use Psr\Log\LoggerInterface;

/**
 * Registers the `lessormq` queue driver. Discovered by Laravel through composer.json.
 */
class LessorMQServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->callAfterResolving('queue', function (QueueManager $manager): void {
            $manager->addConnector('lessormq', fn() => new LessorMQConnector(
                $this->app->make(HttpClientFactory::class),
                $this->app->bound(LoggerInterface::class) ? $this->app->make(LoggerInterface::class) : null,
            ));
        });
    }

    public function boot(): void
    {
        // The worker raises this after it had its say about the exception (and before it
        // would release the job with its own backoff): the moment to tell the server.
        $this->app['events']->listen(JobExceptionOccurred::class, static function (JobExceptionOccurred $event): void {
            if ($event->job instanceof LessorMQJob) {
                $event->job->failedWith($event->exception);
            }
        });
    }
}
