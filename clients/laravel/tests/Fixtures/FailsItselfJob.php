<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests\Fixtures;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;

class FailsItselfJob implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;

    public function handle(): void
    {
        $this->fail(new \InvalidArgumentException('order 7 has no lines'));
    }

    public function failed(?\Throwable $e): void
    {
        Recorder::record('failed', $e === null ? null : $e::class);
    }
}
