<?php

declare(strict_types=1);

namespace App\Controller\V1;

use App\Api\ApiProblem;
use App\Entity\Enum\Scope;
use App\Repository\QueueRepository;
use App\Security\Principal;
use App\Security\RequireScope;
use App\Service\QueueHistoryService;
use App\Service\QueueSettingsService;
use App\Time\Utc;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Attribute\AsController;
use Symfony\Component\Routing\Attribute\Route;

/**
 * What happened in a tenant's queues over a recent period, for charts: per queue, or for
 * every queue the key may use together.
 */
#[AsController]
#[RequireScope(Scope::METRICS, Scope::ADMIN)]
final class HistoryController
{
    public function __construct(
        private readonly QueueHistoryService $history,
        private readonly QueueRepository $queues,
        private readonly QueueSettingsService $queueSettings,
    ) {
    }

    #[Route('/v1/history', name: 'v1_history', methods: ['GET'])]
    public function tenant(Principal $principal, Request $request): JsonResponse
    {
        $period = $this->period($request);
        $tenant = $principal->tenant();
        $names = array_values(array_filter($this->queues->names($tenant), $principal->mayUseQueue(...)));

        return new JsonResponse(
            ['queue' => null] + $this->history->history($tenant, $names, $period, Utc::now()),
        );
    }

    #[Route(
        '/v1/queues/{queue}/history',
        name: 'v1_queue_history',
        requirements: ['queue' => QueueController::NAME],
        methods: ['GET'],
    )]
    public function queue(string $queue, Principal $principal, Request $request): JsonResponse
    {
        $period = $this->period($request);

        if (!$principal->mayUseQueue($queue)) {
            throw ApiProblem::forbidden(sprintf('This key may not use queue "%s".', $queue));
        }

        $tenant = $principal->tenant();

        if (!$this->queueSettings->forQueue($tenant, $queue)->exists) {
            throw ApiProblem::notFound(sprintf('No queue "%s".', $queue));
        }

        return new JsonResponse(
            ['queue' => $queue] + $this->history->history($tenant, [$queue], $period, Utc::now()),
        );
    }

    private function period(Request $request): string
    {
        $period = $request->query->getString('period', QueueHistoryService::DEFAULT_PERIOD);

        if (!isset(QueueHistoryService::PERIODS[$period])) {
            throw ApiProblem::validation([[
                'field' => 'period',
                'message' => 'One of ' . implode(', ', array_keys(QueueHistoryService::PERIODS)) . '.',
            ]]);
        }

        return $period;
    }
}
