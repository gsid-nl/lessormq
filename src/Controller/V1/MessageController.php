<?php

declare(strict_types=1);

namespace App\Controller\V1;

use App\Api\Input;
use App\Entity\Enum\Scope;
use App\Security\Principal;
use App\Security\RequireScope;
use App\Service\MessageService;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Attribute\AsController;
use Symfony\Component\Routing\Attribute\Route;

/**
 * What clients do: publish, claim, then extend, ack, fail or release.
 */
#[AsController]
final class MessageController
{
    public function __construct(private readonly MessageService $messages)
    {
    }

    #[Route(
        '/v1/queues/{queue}/messages',
        name: 'v1_publish',
        requirements: ['queue' => QueueController::NAME],
        methods: ['POST'],
    )]
    #[RequireScope(Scope::PUBLISH)]
    public function publish(string $queue, Principal $principal, Request $request): JsonResponse
    {
        $published = $this->messages->publish($principal, $queue, Input::fromRequest($request));

        return new JsonResponse($published, $published['duplicate'] ? Response::HTTP_OK : Response::HTTP_CREATED);
    }

    #[Route(
        '/v1/queues/{queue}/claims',
        name: 'v1_claim',
        requirements: ['queue' => QueueController::NAME],
        methods: ['POST'],
    )]
    #[RequireScope(Scope::CONSUME)]
    public function claim(string $queue, Principal $principal, Request $request): Response
    {
        $lease = $this->messages->claim($principal, $queue, Input::fromRequest($request, allowEmpty: true));

        return $lease === null ? new Response(null, Response::HTTP_NO_CONTENT) : new JsonResponse($lease);
    }

    #[Route('/v1/messages/{id}/extend', name: 'v1_extend', methods: ['POST'])]
    #[RequireScope(Scope::CONSUME)]
    public function extend(string $id, Principal $principal, Request $request): JsonResponse
    {
        return new JsonResponse($this->messages->extend($principal, $id, Input::fromRequest($request)));
    }

    #[Route('/v1/messages/{id}/ack', name: 'v1_ack', methods: ['POST'])]
    #[RequireScope(Scope::CONSUME)]
    public function ack(string $id, Principal $principal, Request $request): Response
    {
        $this->messages->ack($principal, $id, Input::fromRequest($request));

        return new Response(null, Response::HTTP_NO_CONTENT);
    }

    #[Route('/v1/messages/{id}/fail', name: 'v1_fail', methods: ['POST'])]
    #[RequireScope(Scope::CONSUME)]
    public function fail(string $id, Principal $principal, Request $request): JsonResponse
    {
        return new JsonResponse($this->messages->fail($principal, $id, Input::fromRequest($request)));
    }

    #[Route('/v1/messages/{id}/release', name: 'v1_release', methods: ['POST'])]
    #[RequireScope(Scope::CONSUME)]
    public function release(string $id, Principal $principal, Request $request): Response
    {
        $this->messages->release($principal, $id, Input::fromRequest($request));

        return new Response(null, Response::HTTP_NO_CONTENT);
    }
}
