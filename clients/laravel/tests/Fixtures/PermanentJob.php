<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests\Fixtures;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use LessorMQ\PermanentFailureException;

class PermanentJob implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;

    public function handle(): void
    {
        throw new PermanentFailureException('no such customer');
    }

    public function failed(?\Throwable $e): void
    {
        Recorder::record('failed', $e === null ? null : $e::class);
    }
}
