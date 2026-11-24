<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests\Fixtures;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;

class SlowJob implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;

    public function __construct(public float $seconds)
    {
    }

    public function handle(): void
    {
        // The alarm interrupts usleep(), so wait by the clock.
        $until = microtime(true) + $this->seconds;

        while (microtime(true) < $until) {
            usleep(50_000);
        }

        Recorder::record('handled', $this->attempts());
    }
}
