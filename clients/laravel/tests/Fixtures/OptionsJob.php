<?php

declare(strict_types=1);

namespace LessorMQ\Laravel\Tests\Fixtures;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;

class OptionsJob implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;

    public int $tries = 3;

    public function __construct(public int $order = 7)
    {
    }

    public function deduplicationId(string $payload, string $queue): string
    {
        return 'order-' . $this->order . '@' . $queue;
    }

    public function handle(): void
    {
    }
}
