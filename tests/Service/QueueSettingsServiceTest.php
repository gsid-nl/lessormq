<?php

declare(strict_types=1);

namespace App\Tests\Service;

use App\Api\ApiProblem;
use App\Repository\QueueRepository;
use App\Service\QueueSettingsService;
use App\Tests\Support\FakeRedis;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

#[AllowMockObjectsWithoutExpectations]
class QueueSettingsServiceTest extends TestCase
{
    private QueueRepository&MockObject $queues;
    private FakeRedis $redis;

    /** @var array<string, array{defaults: array, settings: array, paused_at: ?string, exists: bool}> */
    private array $rows = [];

    protected function setUp(): void
    {
        $this->queues = $this->createMock(QueueRepository::class);
        $this->redis = new FakeRedis();

        $this->queues->method('load')->willReturnCallback(
            fn(string $tenant, string $queue) => $this->rows[$tenant . '/' . $queue]
                ?? ['defaults' => ['lease_seconds' => 900], 'settings' => [], 'paused_at' => null, 'exists' => false],
        );
    }

    public function testResolvesLayersAndCachesForAMinute(): void
    {
        $this->rows['acme/mail'] = [
            'defaults' => ['lease_seconds' => 120, 'max_attempts' => 3],
            'settings' => ['max_attempts' => 5],
            'paused_at' => null,
            'exists' => true,
        ];

        $config = $this->service()->forQueue('acme', 'mail');

        $this->assertSame(120, $config->settings['lease_seconds']);
        $this->assertSame(5, $config->settings['max_attempts']);
        $this->assertSame('dead_letter', $config->settings['on_exhausted']);
        $this->assertTrue($config->exists);
        $this->assertFalse($config->isPaused());
        $this->assertSame(60, $this->redis->ttl['queue_settings:acme:mail']);

        // A fresh request reads the cache, not the database
        $this->queues->expects($this->never())->method('load');
        $this->assertSame(5, $this->service()->forQueue('acme', 'mail')->settings['max_attempts']);
    }

    public function testOneRoundTripPerRequest(): void
    {
        $service = $this->service();
        $service->forQueue('acme', 'mail');
        $service->forQueue('acme', 'mail');
        $service->forQueues('acme', ['mail', 'import']);

        $this->assertSame(['mget', 'set', 'mget', 'set'], $this->redis->log);
    }

    public function testWorksFromTheDatabaseWhenRedisIsDown(): void
    {
        $this->redis->down = true;

        $this->assertSame(900, $this->service()->forQueue('acme', 'mail')->settings['lease_seconds']);
    }

    public function testPublishCreatesAMissingQueueOnce(): void
    {
        $this->queues->expects($this->once())->method('createIfMissing')->with('acme', 'mail', null);

        $service = $this->service();
        $service->ensureExists('acme', 'mail');

        $this->rows['acme/mail'] = ['defaults' => [], 'settings' => [], 'paused_at' => null, 'exists' => true];
        $service->ensureExists('acme', 'mail');
    }

    public function testNewQueuesStartPausedWhenTheTenantSaysSo(): void
    {
        $this->rows['acme/mail'] = [
            'defaults' => ['paused' => true],
            'settings' => [],
            'paused_at' => null,
            'exists' => false,
        ];

        $this->queues->expects($this->once())->method('createIfMissing')
            ->with('acme', 'mail', $this->isString());

        $this->service()->ensureExists('acme', 'mail');
    }

    public function testUpdateValidatesAgainstTheEffectiveSettings(): void
    {
        $this->rows['acme/mail'] = [
            'defaults' => ['max_lease_seconds' => 600],
            'settings' => [],
            'paused_at' => null,
            'exists' => true,
        ];
        $this->queues->expects($this->never())->method('save');

        try {
            $this->service()->update('acme', 'mail', ['lease_seconds' => 900]);
            $this->fail('Expected a validation problem');
        } catch (ApiProblem $problem) {
            $this->assertSame('lease_seconds', $problem->errors[0]['field']);
        }
    }

    public function testUpdateMergesIntoTheQueuesOwnSettingsAndKeepsThePause(): void
    {
        $this->rows['acme/mail'] = [
            'defaults' => [],
            'settings' => ['backoff' => ['initial_seconds' => 5]],
            'paused_at' => '2026-11-19 09:00:00',
            'exists' => true,
        ];

        $this->queues->expects($this->once())->method('save')
            ->with('acme', 'mail', ['backoff' => ['initial_seconds' => 5, 'max_seconds' => 60]], '2026-11-19 09:00:00');

        $service = $this->service();
        $service->forQueue('acme', 'mail');
        $service->update('acme', 'mail', ['backoff' => ['max_seconds' => 60]]);

        // The stale copy is dropped, then the fresh settings are read back and cached
        $this->assertSame(['mget', 'set', 'del', 'mget', 'set'], $this->redis->log);
    }

    public function testTenantDefaultsChangeDropsEveryCachedQueueOfThatTenant(): void
    {
        $this->redis->data = [
            'queue_settings:acme:mail' => '{}',
            'queue_settings:acme:import' => '{}',
            'queue_settings:other:mail' => '{}',
        ];

        $this->service()->invalidateTenant('acme');

        $this->assertSame(['queue_settings:other:mail'], array_keys($this->redis->data));
    }

    private function service(): QueueSettingsService
    {
        return new QueueSettingsService($this->queues, $this->redis, new NullLogger());
    }
}
