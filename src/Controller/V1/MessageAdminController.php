<?php

declare(strict_types=1);

namespace App\Controller\V1;

use App\Api\Input;
use App\Entity\Enum\Scope;
use App\Security\Principal;
use App\Security\RequireScope;
use App\Service\MessageAdminService;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Attribute\AsController;
use Symfony\Component\Routing\Attribute\Route;

/**
 * Inspect, replay and delete messages, the dead-letter set included.
 */
#[AsController]
#[RequireScope(Scope::ADMIN)]
final class MessageAdminController
{
    public function __construct(private readonly MessageAdminService $messages)
    {
    }

    #[Route(
        '/v1/queues/{queue}/messages',
        name: 'v1_messages_list',
        requirements: ['queue' => QueueController::NAME],
        methods: ['GET'],
    )]
    public function list(string $queue, Principal $principal, Request $request): JsonResponse
    {
        return new JsonResponse($this->messages->list($principal, $queue, $request));
    }

    #[Route('/v1/messages/{id}', name: 'v1_message_get', methods: ['GET'])]
    public function get(string $id, Principal $principal): JsonResponse
    {
        return new JsonResponse($this->messages->get($principal, $id));
    }

    #[Route('/v1/messages/{id}', name: 'v1_message_delete', methods: ['DELETE'])]
    public function delete(string $id, Principal $principal): Response
    {
        $this->messages->delete($principal, $id);

        return new Response(null, Response::HTTP_NO_CONTENT);
    }

    #[Route('/v1/messages/{id}/replay', name: 'v1_message_replay', methods: ['POST'])]
    public function replay(string $id, Principal $principal): JsonResponse
    {
        return new JsonResponse($this->messages->replay($principal, $id));
    }

    #[Route(
        '/v1/queues/{queue}/replay',
        name: 'v1_messages_replay',
        requirements: ['queue' => QueueController::NAME],
        methods: ['POST'],
    )]
    public function replayMany(string $queue, Principal $principal, Request $request): JsonResponse
    {
        return new JsonResponse($this->messages->bulk($principal, $queue, 'replay', Input::fromRequest($request)));
    }

    #[Route(
        '/v1/queues/{queue}/purge',
        name: 'v1_messages_purge',
        requirements: ['queue' => QueueController::NAME],
        methods: ['POST'],
    )]
    public function purge(string $queue, Principal $principal, Request $request): JsonResponse
    {
        return new JsonResponse($this->messages->bulk($principal, $queue, 'purge', Input::fromRequest($request)));
    }
}
