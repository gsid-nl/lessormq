<?php

declare(strict_types=1);

namespace App\Controller\V1;

use App\Api\Input;
use App\Entity\Enum\Scope;
use App\Security\Principal;
use App\Security\RequireScope;
use App\Service\ApiKeyService;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Attribute\AsController;
use Symfony\Component\Routing\Attribute\Route;

/**
 * A tenant managing its own keys.
 */
#[Route('/v1/api-keys', name: 'v1_api_keys_')]
#[RequireScope(Scope::ADMIN)]
#[AsController]
final class ApiKeyController
{
    public function __construct(private readonly ApiKeyService $apiKeys)
    {
    }

    #[Route('', name: 'list', methods: ['GET'])]
    public function list(Principal $principal): JsonResponse
    {
        return new JsonResponse(['items' => $this->apiKeys->listForTenant($principal->tenant())]);
    }

    #[Route('', name: 'create', methods: ['POST'])]
    public function create(Principal $principal, Request $request): JsonResponse
    {
        return new JsonResponse(
            $this->apiKeys->createFromInput($principal->tenant(), Input::fromRequest($request)),
            Response::HTTP_CREATED,
        );
    }

    #[Route('/{keyId}', name: 'revoke', methods: ['DELETE'])]
    public function revoke(string $keyId, Principal $principal): Response
    {
        $this->apiKeys->revoke($principal->tenant(), $keyId, $principal->keyId);

        return new Response(null, Response::HTTP_NO_CONTENT);
    }
}
