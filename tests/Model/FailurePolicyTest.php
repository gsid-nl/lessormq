<?php

declare(strict_types=1);

namespace App\Tests\Model;

use App\Model\FailurePolicy;
use App\Model\QueueConfig;
use App\Model\QueueSettings;
use PHPUnit\Framework\TestCase;

class FailurePolicyTest extends TestCase
{
    public function testRetryLimitComesFromTheMessageOrTheQueueCappedByTheLimit(): void
    {
        $config = $this->config(['max_attempts' => 7, 'max_attempts_limit' => 10]);

        $this->assertSame(6, FailurePolicy::retryLimit($config, null));
        $this->assertSame(2, FailurePolicy::retryLimit($config, 3));
        $this->assertSame(9, FailurePolicy::retryLimit($config, 50));
    }

    public function testRetriesUntilTheLimitThenFollowsOnExhausted(): void
    {
        $retry = FailurePolicy::decide($this->config(), 0, 1, false);
        $this->assertSame(FailurePolicy::RETRYING, $retry['outcome']);
        $this->assertSame(10, $retry['delay']);

        $this->assertSame(FailurePolicy::DEAD_LETTERED, FailurePolicy::decide($this->config(), 1, 1, false)['outcome']);
        $this->assertSame(
            FailurePolicy::DISCARDED,
            FailurePolicy::decide($this->config(['on_exhausted' => 'discard']), 1, 1, false)['outcome'],
        );
    }

    public function testPermanentFailureFollowsOnPermanent(): void
    {
        $this->assertSame(FailurePolicy::DEAD_LETTERED, FailurePolicy::decide($this->config(), 0, 6, true)['outcome']);
        $this->assertSame(
            FailurePolicy::RETRYING,
            FailurePolicy::decide($this->config(['on_permanent' => 'retry']), 0, 6, true)['outcome'],
        );
    }

    public function testDelayIsExponentialCappedAndJittered(): void
    {
        $backoff = ['initial_seconds' => 10, 'multiplier' => 2, 'max_seconds' => 60, 'jitter' => 0];

        $this->assertSame([10, 20, 40, 60, 60], array_map(
            static fn(int $retries) => FailurePolicy::delay($backoff, $retries),
            [0, 1, 2, 3, 10],
        ));

        $backoff['jitter'] = 0.5;

        for ($i = 0; $i < 50; $i++) {
            $delay = FailurePolicy::delay($backoff, 0);
            $this->assertGreaterThanOrEqual(5, $delay);
            $this->assertLessThanOrEqual(15, $delay);
        }
    }

    /**
     * @param array<string, mixed> $patch
     */
    private function config(array $patch = []): QueueConfig
    {
        return new QueueConfig(QueueSettings::resolve(['backoff' => ['jitter' => 0]], $patch), null, true);
    }
}
