<?php

declare(strict_types=1);

namespace App\Controller\V1;

use App\Api\ApiProblem;
use Doctrine\DBAL\Connection;
use Predis\Client as RedisClient;
use Psr\Log\LoggerInterface;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpKernel\Attribute\AsController;
use Symfony\Component\Routing\Attribute\Route;

/**
 * Liveness with the two dependencies every request needs. No authentication.
 */
#[AsController]
final class HealthController
{
    public function __construct(
        private readonly Connection $connection,
        private readonly RedisClient $redis,
        private readonly LoggerInterface $logger,
    ) {
    }

    #[Route('/healthz', name: 'healthz', methods: ['GET'])]
    public function __invoke(): JsonResponse
    {
        try {
            $this->connection->fetchOne('SELECT 1');
            $this->redis->ping();
        } catch (\Throwable $e) {
            $this->logger->warning('Health check failed', ['error' => $e->getMessage()]);

            throw new ApiProblem(503, 'unavailable', 'The database or Redis is unreachable.');
        }

        return new JsonResponse(['status' => 'ok']);
    }
}
