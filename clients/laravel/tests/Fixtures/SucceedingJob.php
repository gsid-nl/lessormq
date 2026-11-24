<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests\Fixtures;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;

class SucceedingJob implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;

    public function __construct(public string $name = 'Ada')
    {
    }

    public function handle(): void
    {
        Recorder::record('handled', ['name' => $this->name, 'attempt' => $this->attempts()]);
    }
}
