<?php

declare(strict_types=1);

namespace App\Service;

use App\Api\ApiProblem;
use App\Api\Input;
use App\Api\Pagination;
use App\Entity\Enum\MessageStatus;
use App\Entity\Message;
use App\Exception\DuplicateOpenMessageException;
use App\Repository\MessageRepository;
use App\Security\Principal;
use DateTimeImmutable;
use Psr\Log\LoggerInterface;
use Symfony\Component\HttpFoundation\Request;

/**
 * Inspecting and repairing messages, the dead-letter set above all: list, look, replay,
 * delete, one at a time or in bulk. Requires the admin scope.
 */
class MessageAdminService
{
    private const UUID = '[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}';

    /** The longest search a message list takes. */
    private const SEARCH_MAX = 200;

    public function __construct(
        private readonly MessageRepository $messages,
        private readonly QueueSettingsService $queueSettings,
        private readonly QueueStatsService $queueStats,
        private readonly ClaimWaiter $waiter,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * @return array{items: list<array<string, mixed>>, next_cursor: ?string}
     */
    public function list(Principal $principal, string $queue, Request $request): array
    {
        $tenant = $this->authorise($principal, $queue);
        $status = $this->status($request->query->get('status'));
        $errorType = $request->query->get('error_type');
        $search = $this->search($request->query->get('search'));
        $limit = Pagination::limit($request);
        $page = $this->messages->page(
            $tenant,
            $queue,
            $status,
            $errorType === '' ? null : $errorType,
            $limit + 1,
            Pagination::after($request, self::UUID),
            $search,
        );
        $next = null;

        if (count($page) > $limit) {
            $page = array_slice($page, 0, $limit);
            $next = Pagination::cursor(end($page)->getId()->toString());
        }

        $config = $this->queueSettings->forQueue($tenant, $queue);

        return [
            'items' => array_map(static fn(Message $message) => MessageView::render($message, $config), $page),
            'next_cursor' => $next,
        ];
    }

    /**
     * @return array<string, mixed>
     */
    public function get(Principal $principal, string $id): array
    {
        $message = $this->find($principal, $id);

        return MessageView::render(
            $message,
            $this->queueSettings->forQueue($message->getTenantId(), $message->getQueue()),
        );
    }

    public function delete(Principal $principal, string $id): void
    {
        $message = $this->find($principal, $id);

        if (!$this->messages->deleteUnlessProcessing($message)) {
            throw ApiProblem::invalidState('A message that is processing cannot be deleted; its lease will end.');
        }

        $this->queueStats->forget($message->getTenantId(), $message->getQueue());
    }

    /**
     * @return array<string, mixed> the message, pending again
     */
    public function replay(Principal $principal, string $id): array
    {
        $message = $this->find($principal, $id);

        try {
            $replayed = $this->messages->replay($message);
        } catch (DuplicateOpenMessageException $e) {
            throw ApiProblem::invalidState($e->getMessage());
        }

        if (!$replayed) {
            throw ApiProblem::invalidState('Only a failed or completed message can be replayed.');
        }

        $this->queueStats->forget($message->getTenantId(), $message->getQueue());
        $this->waiter->notify($message->getTenantId(), $message->getQueue());

        $this->logger->info('Message replayed', ['message_id' => $id]);

        return $this->get($principal, $id);
    }

    /**
     * @param string $action 'replay' or 'purge'
     * @return array{count: int}
     */
    public function bulk(Principal $principal, string $queue, string $action, Input $input): array
    {
        $tenant = $this->authorise($principal, $queue);
        $input->allowOnly('status', 'error_type', 'before', 'limit');
        $status = $input->choice(
            'status',
            [MessageStatus::COMPLETED->value, MessageStatus::FAILED->value],
            required: true,
        );
        $errorType = $input->string('error_type');
        $before = $this->before($input);
        $limit = $input->has('limit') ? $input->raw('limit') : 1000;

        if (!is_int($limit) || $limit < 1 || $limit > 10000) {
            $input->addError('limit', 'must be an integer from 1 to 10000');
        }

        $input->validate();

        $count = $this->messages->bulk(
            $action,
            $tenant,
            $queue,
            MessageStatus::from($status),
            $errorType,
            $before,
            $limit,
        );

        $this->queueStats->forget($tenant, $queue);

        if ($action === 'replay' && $count > 0) {
            $this->waiter->notify($tenant, $queue);
        }

        $this->logger->info('Bulk ' . $action, [
            'tenant' => $tenant,
            'queue' => $queue,
            'status' => $status,
            'error_type' => $errorType,
            'count' => $count,
        ]);

        return ['count' => $count];
    }

    private function find(Principal $principal, string $id): Message
    {
        $tenant = $principal->tenant();
        $message = preg_match('/^' . self::UUID . '$/D', $id) === 1
            ? $this->messages->findInTenant($tenant, $id)
            : null;

        if ($message === null) {
            throw ApiProblem::notFound(sprintf('No message "%s".', $id));
        }

        $this->authorise($principal, $message->getQueue());

        return $message;
    }

    private function authorise(Principal $principal, string $queue): string
    {
        if (!$principal->mayUseQueue($queue)) {
            throw ApiProblem::forbidden(sprintf('This key may not use queue "%s".', $queue));
        }

        return $principal->tenant();
    }

    private function search(?string $value): ?string
    {
        $value = trim((string) $value);

        if ($value === '') {
            return null;
        }

        if (mb_strlen($value) > self::SEARCH_MAX) {
            throw ApiProblem::validation([
                ['field' => 'search', 'message' => 'at most ' . self::SEARCH_MAX . ' characters'],
            ]);
        }

        return $value;
    }

    private function status(?string $value): ?MessageStatus
    {
        if ($value === null || $value === '') {
            return null;
        }

        $status = MessageStatus::tryFrom($value);

        if ($status === null) {
            throw ApiProblem::validation([['field' => 'status', 'message' => 'unknown status']]);
        }

        return $status;
    }

    private function before(Input $input): ?DateTimeImmutable
    {
        if (!$input->has('before')) {
            return null;
        }

        $value = $input->raw('before');
        $time = is_string($value) ? DateTimeImmutable::createFromFormat(DATE_RFC3339, $value) : false;

        if ($time === false) {
            $input->addError('before', 'must be an RFC 3339 date-time');

            return null;
        }

        return $time;
    }
}
