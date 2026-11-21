<?php

declare(strict_types=1);

namespace App\Controller;

use App\Repository\TenantRepository;
use App\Service\MetricsService;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Attribute\AsController;
use Symfony\Component\Routing\Attribute\Route;

/**
 * The metrics of every tenant's queues, for the installation's own monitoring. It takes
 * no key, so it is kept inside the installation's network; from outside, an operator
 * key reads the same at /v1/metrics.
 */
#[AsController]
final class MetricsController
{
    public function __construct(
        private readonly MetricsService $metrics,
        private readonly TenantRepository $tenants,
    ) {
    }

    #[Route('/metrics', name: 'metrics', methods: ['GET'])]
    public function metrics(): Response
    {
        return new Response(
            $this->metrics->render($this->tenants->ids()),
            Response::HTTP_OK,
            ['Content-Type' => MetricsService::CONTENT_TYPE],
        );
    }
}
