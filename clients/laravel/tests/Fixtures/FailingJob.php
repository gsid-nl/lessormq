<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests\Fixtures;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;

/**
 * Throws, with Laravel's retry settings set so that Laravel itself would fail the job at
 * once: none of them may decide here. ($tries is left out: it asks the server for the
 * message's max_attempts, see OptionsJob.)
 */
class FailingJob implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;

    public int $maxExceptions = 1;

    public array $backoff = [30, 60];

    public bool $failOnTimeout = true;

    public function retryUntil(): \DateTimeInterface
    {
        return now()->subMinute();
    }

    public function handle(): void
    {
        Recorder::record('attempt', $this->attempts());

        throw new \RuntimeException('upstream 503');
    }

    public function failed(?\Throwable $e): void
    {
        Recorder::record('failed', $e === null ? null : $e::class);
    }
}
