<?php

declare(strict_types=1);

namespace App\Controller\V1;

use App\Api\ApiProblem;
use App\Entity\Enum\Scope;
use App\Repository\TenantRepository;
use App\Security\Principal;
use App\Service\MetricsService;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Attribute\AsController;
use Symfony\Component\Routing\Attribute\Route;

/**
 * Prometheus metrics behind a key: a tenant's own queues for a key with the metrics scope
 * (or admin), as far as the key's queue patterns reach; every tenant's for an operator
 * key, or one tenant's with the LessorMQ-Tenant header.
 */
#[AsController]
final class MetricsController
{
    public function __construct(
        private readonly MetricsService $metrics,
        private readonly TenantRepository $tenants,
    ) {
    }

    #[Route('/v1/metrics', name: 'v1_metrics', methods: ['GET'])]
    public function metrics(Principal $principal): Response
    {
        if ($principal->operator && $principal->tenantId === null) {
            $body = $this->metrics->render($this->tenants->ids());
        } elseif ($principal->has(Scope::METRICS)) {
            $body = $this->metrics->render([$principal->tenant()], $principal->mayUseQueue(...));
        } else {
            throw ApiProblem::forbidden('This key needs the metrics scope.');
        }

        return new Response($body, Response::HTTP_OK, ['Content-Type' => MetricsService::CONTENT_TYPE]);
    }
}
