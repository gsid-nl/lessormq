<?php

declare(strict_types=1);

namespace App\Tests\Service;

use App\Repository\HistoryRepository;
use App\Service\QueueHistoryService;
use App\Tests\Support\FakeRedis;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

final class QueueHistoryServiceTest extends TestCase
{
    /** @var list<array{string, list<string>, int, int}> what was asked of the repository */
    private array $asked = [];

    private QueueHistoryService $history;

    private FakeRedis $redis;

    protected function setUp(): void
    {
        $repository = $this->createStub(HistoryRepository::class);
        $published = function (string $tenant, array $queues, int $from, int $size): array {
            $this->asked[] = [$tenant, $queues, $from, $size];

            // The first bucket, the last, and one past the period that is not shown.
            return [0 => 3, 59 => 2, 60 => 7];
        };
        $repository->method('published')->willReturnCallback($published);
        $repository->method('deadLettered')->willReturn([10 => 1]);
        $repository->method('completed')->willReturn([
            ['bucket' => 0, 'wait_class' => 0, 'run_class' => 1, 'count' => 2,
                'wait_sum' => 0.0, 'wait_max' => 0, 'run_sum' => 2.0, 'run_max' => 1],
            ['bucket' => 0, 'wait_class' => 2, 'run_class' => 1, 'count' => 1,
                'wait_sum' => 4.0, 'wait_max' => 4, 'run_sum' => 1.0, 'run_max' => 1],
            ['bucket' => 59, 'wait_class' => 0, 'run_class' => 0, 'count' => 1,
                'wait_sum' => 0.0, 'wait_max' => 0, 'run_sum' => 0.0, 'run_max' => 0],
        ]);

        $this->redis = new FakeRedis();
        $this->history = new QueueHistoryService($repository, $this->redis, new NullLogger());
    }

    public function testTheBucketsAreWholeMinutesUpToTheCurrentOne(): void
    {
        $result = $this->history->history('acme', ['mail'], '1h', new \DateTimeImmutable('2026-11-20 10:30:45 UTC'));

        self::assertSame(60, $result['bucket_seconds']);
        self::assertCount(60, $result['buckets']);
        self::assertSame('2026-11-20T09:31:00Z', $result['from']);
        self::assertSame('2026-11-20T09:31:00Z', $result['buckets'][0]['at']);
        self::assertSame('2026-11-20T10:30:00Z', $result['buckets'][59]['at']);
        self::assertSame('2026-11-20T10:30:45Z', $result['to']);
        self::assertSame([['acme', ['mail'], strtotime('2026-11-20 09:31:00 UTC'), 60]], $this->asked);
    }

    public function testCountsAndDurationsPerBucketAndInTotal(): void
    {
        $result = $this->history->history('acme', ['mail'], '1h', new \DateTimeImmutable('2026-11-20 10:30:45 UTC'));
        $first = $result['buckets'][0];

        self::assertSame(3, $first['published']);
        self::assertSame(3, $first['completed']);
        self::assertSame(0, $first['dead_lettered']);
        self::assertSame(1.33, $first['wait_seconds']['avg']);
        self::assertSame(4, $first['wait_seconds']['max']);
        self::assertSame(1.0, $first['run_seconds']['avg']);
        self::assertNull($result['buckets'][1]['run_seconds']);
        self::assertSame(1, $result['buckets'][10]['dead_lettered']);

        // Bucket 60 lies past the period and is left out.
        self::assertSame(5, $result['totals']['published']);
        self::assertSame(4, $result['totals']['completed']);
        self::assertSame(1, $result['totals']['dead_lettered']);
        self::assertSame(0.75, $result['totals']['run_seconds']['avg']);
        self::assertSame(4, $result['totals']['wait_seconds']['max']);
    }

    public function testAskedAgainSoonItComesFromTheCache(): void
    {
        $now = new \DateTimeImmutable('2026-11-20 10:30:45 UTC');
        $this->history->history('acme', ['mail', 'render'], '24h', $now);
        $again = $this->history->history('acme', ['render', 'mail'], '24h', $now->modify('+5 seconds'));

        self::assertCount(1, $this->asked);
        self::assertSame(96, count($again['buckets']));
        self::assertSame(900, $this->asked[0][3]);
        // A tenth of a bucket of 15 minutes.
        self::assertSame([90], array_values($this->redis->ttl));
    }

    public function testANewBucketIsNotServedFromTheCache(): void
    {
        $this->history->history('acme', ['mail'], '1h', new \DateTimeImmutable('2026-11-20 10:30:45 UTC'));
        $this->history->history('acme', ['mail'], '1h', new \DateTimeImmutable('2026-11-20 10:31:01 UTC'));

        self::assertCount(2, $this->asked);
    }

    public function testATenantWithoutQueuesHasEmptyBuckets(): void
    {
        $result = $this->history->history('acme', [], '7d', new \DateTimeImmutable('2026-11-20 10:30:45 UTC'));

        self::assertSame([], $this->asked);
        self::assertCount(84, $result['buckets']);
        self::assertSame(0, $result['totals']['published']);
        self::assertNull($result['totals']['wait_seconds']);
    }
}
