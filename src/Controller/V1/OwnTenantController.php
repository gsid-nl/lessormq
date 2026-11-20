<?php

declare(strict_types=1);

namespace App\Controller\V1;

use App\Api\Input;
use App\Entity\Enum\Scope;
use App\Security\Principal;
use App\Security\RequireScope;
use App\Service\TenantService;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Attribute\AsController;
use Symfony\Component\Routing\Attribute\Route;

/**
 * The calling tenant's own profile, quotas, usage and queue defaults.
 */
#[Route('/v1/tenant', name: 'v1_own_tenant_')]
#[RequireScope(Scope::ADMIN)]
#[AsController]
final class OwnTenantController
{
    public function __construct(private readonly TenantService $tenants)
    {
    }

    #[Route('', name: 'get', methods: ['GET'])]
    public function get(Principal $principal): JsonResponse
    {
        return new JsonResponse($this->tenants->view($this->tenants->get($principal->tenant())));
    }

    #[Route('', name: 'update', methods: ['PATCH'])]
    public function update(Principal $principal, Request $request): JsonResponse
    {
        $tenant = $this->tenants->get($principal->tenant());
        $this->tenants->updateOwnFromInput($tenant, Input::fromRequest($request));

        return new JsonResponse($this->tenants->view($tenant));
    }
}
