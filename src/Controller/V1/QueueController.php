<?php

declare(strict_types=1);

namespace App\Controller\V1;

use App\Api\ApiProblem;
use App\Api\Input;
use App\Api\Time;
use App\Entity\Enum\Scope;
use App\Model\QueueConfig;
use App\Repository\MessageRepository;
use App\Repository\QueueRepository;
use App\Security\Principal;
use App\Security\RequireScope;
use App\Service\ClaimWaiter;
use App\Service\QueueSettingsService;
use App\Service\QueueStatsService;
use App\Service\WorkerRegistry;
use App\Time\Utc;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Attribute\AsController;
use Symfony\Component\Routing\Attribute\Route;

/**
 * A tenant's queues: settings and statistics.
 */
#[Route('/v1/queues', name: 'v1_queues_')]
#[AsController]
final class QueueController
{
    public const NAME = '[A-Za-z0-9][A-Za-z0-9._-]{0,199}';

    public function __construct(
        private readonly QueueSettingsService $queueSettings,
        private readonly QueueRepository $queues,
        private readonly QueueStatsService $queueStats,
        private readonly ClaimWaiter $waiter,
        private readonly MessageRepository $messages,
        private readonly WorkerRegistry $workers,
    ) {
    }

    #[Route('', name: 'list', methods: ['GET'])]
    #[RequireScope(Scope::ADMIN)]
    public function list(Principal $principal): JsonResponse
    {
        $tenant = $principal->tenant();
        $names = array_values(array_filter($this->queues->names($tenant), $principal->mayUseQueue(...)));
        $configs = $this->queueSettings->forQueues($tenant, $names);
        $stats = $this->queueStats->forQueues($tenant, $names, Utc::now());

        return new JsonResponse([
            'items' => array_map(fn(string $name) => $this->view($name, $configs[$name], $stats[$name]), $names),
        ]);
    }

    #[Route('/{queue}', name: 'get', requirements: ['queue' => self::NAME], methods: ['GET'])]
    #[RequireScope(Scope::CONSUME, Scope::METRICS, Scope::ADMIN)]
    public function get(string $queue, Principal $principal): JsonResponse
    {
        $tenant = $this->authorise($principal, $queue);
        $config = $this->queueSettings->forQueue($tenant, $queue);

        if (!$config->exists) {
            throw ApiProblem::notFound(sprintf('No queue "%s".', $queue));
        }

        return new JsonResponse($this->view($queue, $config, $this->stats($tenant, $queue)));
    }

    /**
     * The workers of the queue: those that claimed from it in the last ten minutes and
     * those that hold one of its messages.
     */
    #[Route('/{queue}/workers', name: 'workers', requirements: ['queue' => self::NAME], methods: ['GET'])]
    #[RequireScope(Scope::CONSUME, Scope::METRICS, Scope::ADMIN)]
    public function workers(string $queue, Principal $principal): JsonResponse
    {
        $tenant = $this->authorise($principal, $queue);
        $holding = $this->messages->processingByConsumer($tenant, [$queue])[$queue];

        return new JsonResponse(['items' => $this->workers->list($tenant, $queue, $holding)]);
    }

    #[Route('/{queue}', name: 'update', requirements: ['queue' => self::NAME], methods: ['PATCH'])]
    #[RequireScope(Scope::ADMIN)]
    public function update(string $queue, Principal $principal, Request $request): JsonResponse
    {
        $tenant = $this->authorise($principal, $queue);
        $config = $this->queueSettings->updateFromInput($tenant, $queue, Input::fromRequest($request));

        // A resumed queue, or one whose settings changed, wakes the claims waiting on it.
        if (!$config->isPaused()) {
            $this->waiter->notify($tenant, $queue);
        }

        return new JsonResponse($this->view($queue, $config, $this->stats($tenant, $queue)));
    }

    private function authorise(Principal $principal, string $queue): string
    {
        if (!$principal->mayUseQueue($queue)) {
            throw ApiProblem::forbidden(sprintf('This key may not use queue "%s".', $queue));
        }

        return $principal->tenant();
    }

    /**
     * @return array<string, int>
     */
    private function stats(string $tenant, string $queue): array
    {
        return $this->queueStats->forQueues($tenant, [$queue], Utc::now())[$queue];
    }

    /**
     * @param array<string, int> $stats
     * @return array<string, mixed>
     */
    private function view(string $name, QueueConfig $config, array $stats): array
    {
        return [
            'name' => $name,
            'settings' => $config->view(),
            // max_retry is for the metrics, not part of QueueStats.
            'stats' => array_diff_key($stats, ['max_retry' => true]),
            'paused_at' => Time::format($config->pausedAt),
        ];
    }
}
