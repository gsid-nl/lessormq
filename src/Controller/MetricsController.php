<?php

declare(strict_types=1);

namespace App\Controller;

use App\Repository\MessageRepository;
use App\Service\QueueSettingsService;
use App\Time\Utc;
use DateTimeImmutable;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

/**
 * Queue health in the Prometheus text format, for alerting on stuck queues.
 *
 * A queue is stuck when work waits for a worker that never comes (all workers
 * busy, hung or scaled away) or when a worker holds a message far longer than
 * any job takes. Both show up as an age, not as a count: a long queue that
 * drains is fine, a short one whose head never moves is not.
 *
 * Only queues with open (pending or processing) messages are listed.
 */
class MetricsController extends AbstractController
{
    public function __construct(
        private readonly MessageRepository $messageRepository,
        private readonly QueueSettingsService $queueSettings,
    ) {
    }

    #[Route('/metrics', name: 'metrics', methods: ['GET'])]
    public function metrics(): Response
    {
        $now = new DateTimeImmutable('now', Utc::zone());
        $health = $this->messageRepository->getOpenMessageHealth($now);

        $queuesByTenant = [];

        foreach ($health as $queue) {
            $queuesByTenant[$queue['tenant']][] = $queue['queue'];
        }

        $configs = [];

        foreach ($queuesByTenant as $tenant => $queues) {
            $configs[$tenant] = $this->queueSettings->forQueues((string) $tenant, $queues);
        }

        $messages = [];
        $oldestReady = [];
        $oldestProcessing = [];
        $maxRetry = [];
        $paused = [];

        foreach ($health as $queue) {
            $label = sprintf(
                'tenant="%s",queue="%s"',
                $this->escapeLabel($queue['tenant']),
                $this->escapeLabel($queue['queue']),
            );

            foreach (['ready', 'delayed', 'processing'] as $state) {
                $messages[] = sprintf('lessormq_messages{%s,state="%s"} %d', $label, $state, $queue[$state]);
            }

            $oldestReady[] = sprintf(
                'lessormq_oldest_ready_seconds{%s} %d',
                $label,
                $this->ageInSeconds($queue['oldest_ready'], $now)
            );
            $oldestProcessing[] = sprintf(
                'lessormq_oldest_processing_seconds{%s} %d',
                $label,
                $this->ageInSeconds($queue['oldest_processing'], $now)
            );
            $maxRetry[] = sprintf('lessormq_max_retry_count{%s} %d', $label, $queue['max_retry']);
            $paused[] = sprintf(
                'lessormq_paused{%s} %d',
                $label,
                $configs[$queue['tenant']][$queue['queue']]->isPaused() ? 1 : 0
            );
        }

        $body = implode("\n", [
            '# HELP lessormq_messages Open messages per queue: ready for a worker, delayed until later, or processing.',
            '# TYPE lessormq_messages gauge',
            ...$messages,
            '# HELP lessormq_oldest_ready_seconds How long the oldest message a worker could take has been waiting.',
            '# TYPE lessormq_oldest_ready_seconds gauge',
            ...$oldestReady,
            '# HELP lessormq_oldest_processing_seconds How long the longest-held claimed message has been processing.',
            '# TYPE lessormq_oldest_processing_seconds gauge',
            ...$oldestProcessing,
            '# HELP lessormq_max_retry_count Highest retry count among open messages.',
            '# TYPE lessormq_max_retry_count gauge',
            ...$maxRetry,
            '# HELP lessormq_paused Whether the queue is paused (1) or not (0).',
            '# TYPE lessormq_paused gauge',
            ...$paused,
        ]) . "\n";

        return new Response($body, Response::HTTP_OK, [
            'Content-Type' => 'text/plain; version=0.0.4; charset=utf-8',
        ]);
    }

    /**
     * Stored timestamps are UTC.
     */
    private function ageInSeconds(?string $timestamp, DateTimeImmutable $now): int
    {
        if ($timestamp === null) {
            return 0;
        }

        return max(0, $now->getTimestamp() - Utc::parse($timestamp)->getTimestamp());
    }

    private function escapeLabel(string $value): string
    {
        return str_replace(['\\', '"', "\n"], ['\\\\', '\\"', '\\n'], $value);
    }
}
