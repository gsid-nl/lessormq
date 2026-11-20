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
use App\Service\QueueSettingsService;
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
        private readonly MessageRepository $messages,
    ) {
    }

    #[Route('', name: 'list', methods: ['GET'])]
    #[RequireScope(Scope::ADMIN)]
    public function list(Principal $principal): JsonResponse
    {
        $tenant = $principal->tenant();
        $names = array_values(array_filter($this->queues->names($tenant), $principal->mayUseQueue(...)));
        $configs = $this->queueSettings->forQueues($tenant, $names);
        $stats = $this->messages->queueStats($tenant, $names, Utc::now());

        return new JsonResponse([
            'items' => array_map(fn(string $name) => $this->view($name, $configs[$name], $stats[$name]), $names),
        ]);
    }

    #[Route('/{queue}', name: 'get', requirements: ['queue' => self::NAME], methods: ['GET'])]
    #[RequireScope(Scope::CONSUME, Scope::ADMIN)]
    public function get(string $queue, Principal $principal): JsonResponse
    {
        $tenant = $this->authorise($principal, $queue);
        $config = $this->queueSettings->forQueue($tenant, $queue);

        if (!$config->exists) {
            throw ApiProblem::notFound(sprintf('No queue "%s".', $queue));
        }

        return new JsonResponse($this->view($queue, $config, $this->stats($tenant, $queue)));
    }

    #[Route('/{queue}', name: 'update', requirements: ['queue' => self::NAME], methods: ['PATCH'])]
    #[RequireScope(Scope::ADMIN)]
    public function update(string $queue, Principal $principal, Request $request): JsonResponse
    {
        $tenant = $this->authorise($principal, $queue);
        $config = $this->queueSettings->updateFromInput($tenant, $queue, Input::fromRequest($request));

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
        return $this->messages->queueStats($tenant, [$queue], Utc::now())[$queue];
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
            'stats' => $stats,
            'paused_at' => Time::format($config->pausedAt),
        ];
    }
}
