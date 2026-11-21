<?php

declare(strict_types=1);

namespace App\Service;

use App\Repository\QueueRepository;
use App\Time\Utc;

/**
 * Queue health in the Prometheus text format, for alerting on stuck queues: for the whole
 * installation (/metrics) or for one tenant (/v1/metrics).
 *
 * A queue is stuck when work waits for a worker that never comes (all workers busy, hung
 * or scaled away) or when a worker holds a message far longer than any job takes. Both
 * show up as an age, not as a count: a long queue that drains is fine, a short one whose
 * head never moves is not. A growing dead-letter set is the other thing to watch.
 *
 * Every queue of the tenants asked for is listed, idle ones with zeros, so a series stays
 * rather than vanishing when a queue empties. The figures come per tenant, along the
 * tenant's part of the index, rather than from a scan of every message.
 */
final class MetricsService
{
    public const CONTENT_TYPE = 'text/plain; version=0.0.4; charset=utf-8';

    private const HELP = [
        'lessormq_messages' => 'Open messages per queue: ready for a worker, delayed until later, or processing.',
        'lessormq_dead_letter_messages' => 'Messages that ran out of attempts or failed permanently, kept to replay.',
        'lessormq_oldest_ready_seconds' => 'How long the oldest message a worker could take has been waiting.',
        'lessormq_oldest_processing_seconds' => 'How long the longest-held claimed message has been processing.',
        'lessormq_max_retry_count' => 'Highest retry count among open messages.',
        'lessormq_paused' => 'Whether the queue is paused (1) or not (0).',
        'lessormq_workers' => 'Workers that claimed from the queue in the last minute or hold one of its messages.',
    ];

    public function __construct(
        private readonly QueueStatsService $queueStats,
        private readonly QueueRepository $queues,
        private readonly QueueSettingsService $queueSettings,
    ) {
    }

    /**
     * @param list<string> $tenants
     * @param (callable(string): bool)|null $mayUseQueue only the queues this allows
     */
    public function render(array $tenants, ?callable $mayUseQueue = null): string
    {
        $now = Utc::now();
        $series = array_fill_keys(array_keys(self::HELP), []);

        foreach ($tenants as $tenant) {
            $names = $this->queues->names($tenant);

            if ($mayUseQueue !== null) {
                $names = array_values(array_filter($names, $mayUseQueue));
            }

            if ($names === []) {
                continue;
            }

            $configs = $this->queueSettings->forQueues($tenant, $names);
            $stats = $this->queueStats->forQueues($tenant, $names, $now);

            foreach ($names as $queue) {
                $s = $stats[$queue];
                $label = sprintf('tenant="%s",queue="%s"', self::escape($tenant), self::escape($queue));

                foreach (['ready', 'delayed', 'processing'] as $state) {
                    $series['lessormq_messages'][] = sprintf('{%s,state="%s"} %d', $label, $state, $s[$state]);
                }

                $series['lessormq_dead_letter_messages'][] = sprintf('{%s} %d', $label, $s['failed']);
                $series['lessormq_oldest_ready_seconds'][] = sprintf('{%s} %d', $label, $s['oldest_ready_seconds']);
                $series['lessormq_oldest_processing_seconds'][] = sprintf(
                    '{%s} %d',
                    $label,
                    $s['oldest_processing_seconds'],
                );
                $series['lessormq_max_retry_count'][] = sprintf('{%s} %d', $label, $s['max_retry']);
                $series['lessormq_paused'][] = sprintf('{%s} %d', $label, $configs[$queue]->isPaused() ? 1 : 0);
                $series['lessormq_workers'][] = sprintf('{%s} %d', $label, $s['workers'] ?? 0);
            }
        }

        $lines = [];

        foreach (self::HELP as $metric => $help) {
            $lines[] = "# HELP {$metric} {$help}";
            $lines[] = "# TYPE {$metric} gauge";

            foreach ($series[$metric] as $sample) {
                $lines[] = $metric . $sample;
            }
        }

        return implode("\n", $lines) . "\n";
    }

    private static function escape(string $value): string
    {
        return str_replace(['\\', '"', "\n"], ['\\\\', '\\"', '\\n'], $value);
    }
}
