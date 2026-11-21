<?php

declare(strict_types=1);

namespace App\Tests\Service;

use App\Repository\MessageRepository;
use App\Service\QueueStatsService;
use App\Service\WorkerRegistry;
use App\Tests\Support\FakeRedis;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

final class QueueStatsServiceTest extends TestCase
{
    /** @var array<string, int> how often each queue's completed messages were counted */
    private array $counted = [];

    private QueueStatsService $stats;

    protected function setUp(): void
    {
        $messages = $this->createStub(MessageRepository::class);
        $figures = [
            'ready' => 1, 'delayed' => 0, 'processing' => 0, 'completed' => 0, 'failed' => 2,
            'oldest_ready_seconds' => 5, 'oldest_processing_seconds' => 0, 'max_retry' => 0,
        ];
        $messages->method('queueStats')->willReturnCallback(
            static fn (string $tenant, array $queues) => array_fill_keys($queues, $figures),
        );
        $messages->method('completedCount')->willReturnCallback(function (string $tenant, string $queue): int {
            $this->counted[$queue] = ($this->counted[$queue] ?? 0) + 1;

            return 1000 * $this->counted[$queue];
        });

        $messages->method('processingByConsumer')->willReturn([]);
        $workers = $this->createStub(WorkerRegistry::class);
        $workers->method('counts')->willReturnCallback(
            static fn (string $tenant, array $queues) => array_fill_keys($queues, 2),
        );

        $this->stats = new QueueStatsService($messages, new FakeRedis(), $workers, new NullLogger());
    }

    public function testCountsCompletedMessagesOnceThenFromTheCache(): void
    {
        $first = $this->stats->forQueues('acme', ['mail', 'render'], new \DateTimeImmutable());
        $second = $this->stats->forQueues('acme', ['mail', 'render'], new \DateTimeImmutable());

        self::assertSame(1000, $first['mail']['completed']);
        self::assertSame(1000, $second['mail']['completed']);
        self::assertSame(['mail' => 1, 'render' => 1], $this->counted);
        // The rest is live
        self::assertSame(2, $second['render']['failed']);
        self::assertSame(2, $second['render']['workers']);
    }

    public function testAnAdminActionDropsTheCachedCount(): void
    {
        $this->stats->forQueues('acme', ['mail', 'render'], new \DateTimeImmutable());
        $this->stats->forget('acme', 'mail');
        $after = $this->stats->forQueues('acme', ['mail', 'render'], new \DateTimeImmutable());

        self::assertSame(2000, $after['mail']['completed']);
        self::assertSame(1000, $after['render']['completed']);
    }

    public function testNoQueuesNoQueries(): void
    {
        self::assertSame([], $this->stats->forQueues('acme', [], new \DateTimeImmutable()));
        self::assertSame([], $this->counted);
    }
}
