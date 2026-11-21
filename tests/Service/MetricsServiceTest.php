<?php

declare(strict_types=1);

namespace App\Tests\Service;

use App\Model\QueueConfig;
use App\Model\QueueSettings;
use App\Repository\QueueRepository;
use App\Service\MetricsService;
use App\Service\QueueSettingsService;
use App\Service\QueueStatsService;
use PHPUnit\Framework\TestCase;

final class MetricsServiceTest extends TestCase
{
    private const EMPTY = [
        'ready' => 0, 'delayed' => 0, 'processing' => 0, 'completed' => 0, 'failed' => 0,
        'oldest_ready_seconds' => 0, 'oldest_processing_seconds' => 0, 'max_retry' => 0,
    ];

    private MetricsService $metrics;

    protected function setUp(): void
    {
        $queues = $this->createStub(QueueRepository::class);
        $queues->method('names')->willReturnMap([
            ['acme', ['render', 'thumbnails']],
            ['other', ['mail']],
            ['idle', []],
        ]);

        $stats = $this->createStub(QueueStatsService::class);
        $stats->method('forQueues')->willReturnCallback(
            static fn (string $tenant, array $names): array => array_intersect_key([
                'render' => [
                    'ready' => 936, 'delayed' => 2, 'processing' => 10, 'failed' => 4,
                    'oldest_ready_seconds' => 7200, 'oldest_processing_seconds' => 3000, 'max_retry' => 6,
                ] + self::EMPTY,
                'thumbnails' => ['processing' => 1, 'oldest_processing_seconds' => 30] + self::EMPTY,
                'mail' => self::EMPTY,
            ], array_flip($names)),
        );

        $settings = $this->createStub(QueueSettingsService::class);
        $settings->method('forQueues')->willReturnCallback(
            static fn (string $tenant, array $names): array => array_combine($names, array_map(
                static fn (string $queue) => new QueueConfig(
                    QueueSettings::DEFAULTS,
                    $queue === 'thumbnails' ? new \DateTimeImmutable() : null,
                    true,
                ),
                $names,
            )),
        );

        $this->metrics = new MetricsService($stats, $queues, $settings);
    }

    public function testReportsCountsAgesDeadLettersRetriesAndPauseState(): void
    {
        $body = $this->metrics->render(['acme']);
        $render = 'tenant="acme",queue="render"';

        $this->assertStringContainsString('lessormq_messages{' . $render . ',state="ready"} 936', $body);
        $this->assertStringContainsString('lessormq_messages{' . $render . ',state="delayed"} 2', $body);
        $this->assertStringContainsString('lessormq_messages{' . $render . ',state="processing"} 10', $body);
        $this->assertStringContainsString('lessormq_dead_letter_messages{' . $render . '} 4', $body);
        $this->assertStringContainsString('lessormq_oldest_ready_seconds{' . $render . '} 7200', $body);
        $this->assertStringContainsString('lessormq_oldest_processing_seconds{' . $render . '} 3000', $body);
        $this->assertStringContainsString('lessormq_max_retry_count{' . $render . '} 6', $body);
        $this->assertStringContainsString('lessormq_paused{' . $render . '} 0', $body);
        $this->assertStringContainsString('lessormq_paused{tenant="acme",queue="thumbnails"} 1', $body);

        // Every metric carries a HELP and TYPE line, and the body ends in a newline
        $this->assertSame(7, substr_count($body, '# TYPE '));
        $this->assertStringEndsWith("\n", $body);
    }

    public function testListsOnlyTheTenantsAndQueuesAskedFor(): void
    {
        $body = $this->metrics->render(['acme', 'other'], static fn (string $queue) => $queue !== 'thumbnails');

        $this->assertStringContainsString('queue="render"', $body);
        $this->assertStringContainsString('lessormq_dead_letter_messages{tenant="other",queue="mail"} 0', $body);
        $this->assertStringNotContainsString('thumbnails', $body);
    }

    public function testATenantWithoutQueuesGivesOnlyTheHeaders(): void
    {
        $body = $this->metrics->render(['idle']);

        $this->assertSame(0, preg_match('/^lessormq_/m', $body));
        $this->assertSame(7, substr_count($body, '# TYPE '));
    }
}
