<?php

declare(strict_types=1);

namespace App\Controller\V1;

use App\Api\ApiProblem;
use App\Api\Input;
use App\Api\Pagination;
use App\Entity\Enum\TenantStatus;
use App\Entity\Tenant;
use App\Repository\TenantRepository;
use App\Security\OperatorOnly;
use App\Security\Principal;
use App\Service\ApiKeyService;
use App\Service\TenantService;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Attribute\AsController;
use Symfony\Component\Routing\Attribute\Route;

/**
 * Tenant management for operators (a web interface, a provisioning tool).
 */
#[Route('/v1/tenants', name: 'v1_tenants_')]
#[OperatorOnly]
#[AsController]
final class TenantController
{
    private const ID = Tenant::ID_PATTERN;

    public function __construct(
        private readonly TenantService $tenants,
        private readonly TenantRepository $repository,
        private readonly ApiKeyService $apiKeys,
    ) {
    }

    #[Route('', name: 'list', methods: ['GET'])]
    public function list(Request $request): JsonResponse
    {
        $status = $request->query->get('status');
        $statusEnum = $status === null ? null : TenantStatus::tryFrom($status);

        if ($status !== null && $statusEnum === null) {
            throw ApiProblem::validation([['field' => 'status', 'message' => 'unknown status']]);
        }

        $limit = Pagination::limit($request);
        $page = $this->repository->page($statusEnum, $limit + 1, Pagination::after($request, self::ID));
        $next = null;

        if (count($page) > $limit) {
            $page = array_slice($page, 0, $limit);
            $next = Pagination::cursor(end($page)->getId());
        }

        return new JsonResponse([
            'items' => array_map(fn(Tenant $tenant) => $this->tenants->view($tenant), $page),
            'next_cursor' => $next,
        ]);
    }

    #[Route('', name: 'create', methods: ['POST'])]
    public function create(Request $request): JsonResponse
    {
        return new JsonResponse(
            $this->tenants->createFromInput(Input::fromRequest($request)),
            Response::HTTP_CREATED,
        );
    }

    #[Route('/{tenantId}', name: 'get', requirements: ['tenantId' => self::ID], methods: ['GET'])]
    public function get(string $tenantId): JsonResponse
    {
        return new JsonResponse($this->tenants->view($this->tenants->get($tenantId)));
    }

    #[Route('/{tenantId}', name: 'update', requirements: ['tenantId' => self::ID], methods: ['PATCH'])]
    public function update(string $tenantId, Request $request): JsonResponse
    {
        $tenant = $this->tenants->get($tenantId);
        $this->tenants->updateFromInput($tenant, Input::fromRequest($request));

        return new JsonResponse($this->tenants->view($tenant));
    }

    #[Route('/{tenantId}', name: 'delete', requirements: ['tenantId' => self::ID], methods: ['DELETE'])]
    public function delete(string $tenantId): JsonResponse
    {
        $tenant = $this->tenants->get($tenantId);
        $this->tenants->requestDeletion($tenant);

        return new JsonResponse($this->tenants->view($tenant), Response::HTTP_ACCEPTED);
    }

    #[Route('/{tenantId}/api-keys', name: 'keys_list', requirements: ['tenantId' => self::ID], methods: ['GET'])]
    public function listKeys(string $tenantId): JsonResponse
    {
        $this->tenants->get($tenantId);

        return new JsonResponse(['items' => $this->apiKeys->listForTenant($tenantId)]);
    }

    #[Route('/{tenantId}/api-keys', name: 'keys_create', requirements: ['tenantId' => self::ID], methods: ['POST'])]
    public function createKey(string $tenantId, Request $request): JsonResponse
    {
        $this->tenants->get($tenantId);

        return new JsonResponse(
            $this->apiKeys->createFromInput($tenantId, Input::fromRequest($request)),
            Response::HTTP_CREATED,
        );
    }

    #[Route(
        '/{tenantId}/api-keys/{keyId}',
        name: 'keys_revoke',
        requirements: ['tenantId' => self::ID],
        methods: ['DELETE'],
    )]
    public function revokeKey(string $tenantId, string $keyId, Principal $principal): Response
    {
        $this->tenants->get($tenantId);
        $this->apiKeys->revoke($tenantId, $keyId, $principal->keyId);

        return new Response(null, Response::HTTP_NO_CONTENT);
    }
}
