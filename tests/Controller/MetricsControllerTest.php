<?php

declare(strict_types=1);

namespace App\Tests\Controller;

use App\Controller\MetricsController;
use App\Repository\MessageRepository;
use App\Model\QueueConfig;
use App\Model\QueueSettings;
use App\Service\QueueSettingsService;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Response;

#[\PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations]
class MetricsControllerTest extends TestCase
{
    private MessageRepository&MockObject $messageRepository;
    private QueueSettingsService&MockObject $queueSettings;
    private MetricsController $controller;

    protected function setUp(): void
    {
        $this->messageRepository = $this->createMock(MessageRepository::class);
        $this->queueSettings = $this->createMock(QueueSettingsService::class);
        $this->queueSettings->method('forQueues')->willReturnCallback(
            static fn (string $tenant, array $queues): array => array_combine($queues, array_map(
                static fn (string $queue) => new QueueConfig(
                    QueueSettings::DEFAULTS,
                    $queue === 'thumbnails' ? new \DateTimeImmutable() : null,
                    true,
                ),
                $queues,
            )),
        );

        $this->controller = new MetricsController($this->messageRepository, $this->queueSettings);
        $this->controller->setContainer(new \Symfony\Component\DependencyInjection\Container());
    }

    public function testReportsAgesCountsRetriesAndPauseState(): void
    {
        $this->messageRepository->method('getOpenMessageHealth')->willReturn([
            [
                'tenant' => 'acme',
                'queue' => 'render',
                'ready' => 936,
                'delayed' => 2,
                'processing' => 10,
                'oldest_ready' => (new \DateTimeImmutable('-2 hours'))->format('Y-m-d H:i:s'),
                'oldest_processing' => (new \DateTimeImmutable('-50 minutes'))->format('Y-m-d H:i:s'),
                'max_retry' => 6,
            ],
            [
                'tenant' => 'acme',
                'queue' => 'thumbnails',
                'ready' => 0,
                'delayed' => 0,
                'processing' => 1,
                'oldest_ready' => null,
                'oldest_processing' => (new \DateTimeImmutable('-30 seconds'))->format('Y-m-d H:i:s'),
                'max_retry' => 0,
            ],
        ]);

        $response = $this->controller->metrics();
        $body = (string) $response->getContent();

        $this->assertSame(Response::HTTP_OK, $response->getStatusCode());
        $this->assertStringStartsWith('text/plain; version=0.0.4', (string) $response->headers->get('Content-Type'));

        $video = 'tenant="acme",queue="render"';
        $this->assertStringContainsString('lessormq_messages{' . $video . ',state="ready"} 936', $body);
        $this->assertStringContainsString('lessormq_messages{' . $video . ',state="delayed"} 2', $body);
        $this->assertStringContainsString('lessormq_messages{' . $video . ',state="processing"} 10', $body);
        $this->assertStringContainsString('lessormq_max_retry_count{tenant="acme",queue="render"} 6', $body);
        $this->assertStringContainsString('lessormq_paused{tenant="acme",queue="render"} 0', $body);
        $this->assertStringContainsString('lessormq_paused{tenant="acme",queue="thumbnails"} 1', $body);

        $ready = $this->sample($body, 'lessormq_oldest_ready_seconds{tenant="acme",queue="render"}');
        $processing = $this->sample($body, 'lessormq_oldest_processing_seconds{tenant="acme",queue="render"}');
        $this->assertEqualsWithDelta(7200, $ready, 5);
        $this->assertEqualsWithDelta(3000, $processing, 5);

        // Nothing ready means nothing is waiting, not that something waited forever
        $this->assertSame(0, $this->sample($body, 'lessormq_oldest_ready_seconds{tenant="acme",queue="thumbnails"}'));

        // Every series carries a HELP and TYPE line, and the body ends in a newline
        $this->assertSame(5, substr_count($body, '# TYPE '));
        $this->assertStringEndsWith("\n", $body);
    }

    public function testEmptyWhenNothingIsOpen(): void
    {
        $this->messageRepository->method('getOpenMessageHealth')->willReturn([]);

        $body = (string) $this->controller->metrics()->getContent();

        $this->assertSame(0, preg_match('/^lessormq_/m', $body));
        $this->assertSame(5, substr_count($body, '# TYPE '));
    }

    private function sample(string $body, string $series): int
    {
        $this->assertSame(1, preg_match('/^' . preg_quote($series, '/') . ' (\d+)$/m', $body, $match), $series);

        return (int) $match[1];
    }
}
