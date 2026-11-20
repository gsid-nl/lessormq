<?php

declare(strict_types=1);

namespace App\Service;

use App\Api\ApiProblem;
use App\Api\Input;
use App\Api\Time;
use App\Entity\Enum\MessageStatus;
use App\Entity\Message;
use App\Model\FailurePolicy;
use App\Model\QueueConfig;
use App\Model\Route;
use App\Repository\MessageRepository;
use App\Security\Principal;
use App\Time\Utc;
use DateTimeImmutable;
use Psr\Log\LoggerInterface;

/**
 * The v1 message protocol (docs/protocol.md): publish, claim, extend, ack, fail, release.
 * Everything a client may not decide is decided here, from the queue's settings.
 */
class MessageService
{
    private const RECEIPT = '[0-9a-f]{32}';
    private const MAX_WAIT = 20;

    public function __construct(
        private readonly MessageRepository $messages,
        private readonly MessageCreator $messageCreator,
        private readonly QueueSettingsService $queueSettings,
        private readonly LeaseKeeper $leaseKeeper,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * @return array<string, mixed> PublishResponse
     */
    public function publish(Principal $principal, string $queue, Input $input): array
    {
        $tenant = $this->authorise($principal, $queue);
        $input->allowOnly('body', 'headers', 'dedup_key', 'delay_seconds', 'available_at', 'max_attempts');

        if (!$input->has('body')) {
            $input->addError('body', 'is required');
        } elseif ($input->raw('body') === null) {
            $input->addError('body', 'must not be null');
        }

        $headers = $this->headers($input);
        $delay = $input->has('delay_seconds') ? $this->integer($input, 'delay_seconds', 0) : null;
        $availableAt = $this->availableAt($input);
        $maxAttempts = $input->has('max_attempts') ? $this->integer($input, 'max_attempts', 1) : null;

        if ($delay !== null && $availableAt !== null) {
            $input->addError('available_at', 'not together with delay_seconds');
        }

        $dedupKey = $input->string('dedup_key');
        $input->validate();

        $config = $this->queueSettings->forQueue($tenant, $queue);
        $size = strlen((string) json_encode($input->raw('body'))) + strlen((string) json_encode((object) $headers));

        if ($size > $config->int('max_message_bytes')) {
            throw ApiProblem::payloadTooLarge($config->int('max_message_bytes'));
        }

        $this->queueSettings->ensureExists($tenant, $queue);

        if ($delay !== null && $delay > 0) {
            $availableAt = new DateTimeImmutable('+' . $delay . ' seconds', Utc::zone());
        }

        $maxAttempts = $maxAttempts === null ? null : min($maxAttempts, $config->int('max_attempts_limit'));

        [$message, $duplicate] = $this->messageCreator->create(
            $tenant,
            $queue,
            $input->raw('body'),
            static function (Message $message) use ($headers, $availableAt, $maxAttempts): void {
                $message->setHeaders($headers);
                $message->setMaxAttempts($maxAttempts);

                if ($availableAt !== null) {
                    $message->setProcessingAfter(
                        \DateTime::createFromImmutable($availableAt->setTimezone(Utc::zone())),
                    );
                }
            },
            $dedupKey,
        );

        return [
            'id' => $message->getId()->toString(),
            'queue' => $queue,
            'status' => $message->getStatus()->value,
            'created_at' => Time::format($message->getCreatedAt()),
            'available_at' => Time::format($message->getProcessingAfter() ?? $message->getCreatedAt()),
            'duplicate' => $duplicate,
        ];
    }

    /**
     * @return array<string, mixed>|null a Lease, or null when nothing became ready in time
     */
    public function claim(Principal $principal, string $queue, Input $input): ?array
    {
        $tenant = $this->authorise($principal, $queue);
        $input->allowOnly('consumer', 'route', 'wait_seconds', 'lease_seconds');
        $consumer = $input->string('consumer') ?? '';
        $route = $input->string('route', pattern: Route::PATTERN);
        $wait = $input->has('wait_seconds') ? $this->integer($input, 'wait_seconds', 0, self::MAX_WAIT) : 0;
        $leaseSeconds = $input->has('lease_seconds') ? $this->integer($input, 'lease_seconds', 1) : null;
        $input->validate();

        $config = $this->queueSettings->forQueue($tenant, $queue);
        $lease = min($leaseSeconds ?? $config->int('lease_seconds'), $config->int('max_lease_seconds'));
        $deadline = microtime(true) + ($wait ?? 0);
        $swept = false;

        // Waiting checks for work once a second. Each waiting claim holds a PHP worker,
        // so this is the simple form of long polling; a Redis wake-up replaces it later.
        while (true) {
            if (!$config->isPaused()) {
                if (!$swept) {
                    $this->leaseKeeper->sweepIfDue($tenant, $queue, $config);
                    $swept = true;
                }

                $message = $this->messages->claimNextMessage(
                    $tenant,
                    $queue,
                    $consumer,
                    $route,
                    Route::avoidance($route, $config),
                    $lease,
                );

                if ($message !== null) {
                    return $this->lease($message, $config);
                }
            }

            $remaining = $deadline - microtime(true);

            if ($remaining <= 0) {
                return null;
            }

            usleep((int) (min(1.0, $remaining) * 1_000_000));
        }
    }

    /**
     * @return array{lease_until: string}
     */
    public function extend(Principal $principal, string $id, Input $input): array
    {
        $input->allowOnly('receipt', 'lease_seconds');
        $receipt = $this->receipt($input);
        $leaseSeconds = $input->has('lease_seconds') ? $this->integer($input, 'lease_seconds', 1) : null;
        $input->validate();

        [$tenant, $state] = $this->state($principal, $id);
        $config = $this->queueSettings->forQueue($tenant, $state['queue']);
        $lease = min($leaseSeconds ?? $config->int('lease_seconds'), $config->int('max_lease_seconds'));
        $until = new DateTimeImmutable('+' . $lease . ' seconds', Utc::zone());

        if (!$this->messages->extendLease($tenant, $id, $receipt, $until)) {
            throw ApiProblem::leaseLost();
        }

        return ['lease_until' => Time::format($until)];
    }

    public function ack(Principal $principal, string $id, Input $input): void
    {
        $input->allowOnly('receipt');
        $receipt = $this->receipt($input);
        $input->validate();

        [$tenant, $state] = $this->state($principal, $id);
        $message = $this->messages->findInTenant($tenant, $id);
        $now = Utc::now();

        $done = $message !== null && $this->messages->updateMessageRow($message, [
            'status' => MessageStatus::COMPLETED->value,
            'processed_at' => Utc::sql($now),
            'lease_until' => null,
        ], MessageStatus::PROCESSING, $receipt);

        if ($done) {
            return;
        }

        // A repeated ack of the same lease is answered as the first one was.
        $state = $this->messages->leaseState($tenant, $id) ?? $state;

        if ($state['status'] !== MessageStatus::COMPLETED->value || $state['receipt'] !== $receipt) {
            throw ApiProblem::leaseLost();
        }
    }

    /**
     * @return array<string, mixed> FailResponse
     */
    public function fail(Principal $principal, string $id, Input $input): array
    {
        $input->allowOnly('receipt', 'error', 'permanent');
        $receipt = $this->receipt($input);
        $error = $this->errorReport($input);
        $permanent = $input->bool('permanent') ?? false;
        $input->validate();

        [$tenant, $state] = $this->state($principal, $id);
        $config = $this->queueSettings->forQueue($tenant, $state['queue']);
        $message = $this->messages->findInTenant($tenant, $id);

        if ($message === null || !$this->holdsLease($message, $receipt)) {
            return $this->repeatedFail($tenant, $id, $receipt, $config, $state);
        }

        $retriesSoFar = $message->getRetryCount();
        $retryLimit = FailurePolicy::retryLimit($config, $message->getMaxAttempts());
        $decision = FailurePolicy::decide($config, $retriesSoFar, $retryLimit, $permanent);
        $now = Utc::now();
        $details = json_encode([
            'type' => $error['type'],
            'message' => $error['message'],
            'details' => $error['details'],
            'at' => Time::format($now),
            'attempt' => $retriesSoFar + 1,
            'permanent' => $permanent,
            'expired' => false,
            'consumer' => $message->getConsumedBy(),
            'route' => $message->getRoute(),
        ]);
        $values = ['error_message' => $error['message'], 'error_details' => $details, 'lease_until' => null];
        $failedRoutes = $message->failedRoutesWithCurrent($now);

        if ($failedRoutes !== null) {
            $values['failed_routes'] = $failedRoutes;
        }
        $availableAt = null;

        if ($decision['outcome'] === FailurePolicy::DISCARDED) {
            $done = $this->messages->deleteIfStatus($message, MessageStatus::PROCESSING, $receipt);
        } elseif ($decision['outcome'] === FailurePolicy::DEAD_LETTERED) {
            $done = $this->messages->updateMessageRow($message, $values + [
                'status' => MessageStatus::FAILED->value,
                'failed_at' => Utc::sql($now),
            ], MessageStatus::PROCESSING, $receipt);
        } else {
            $availableAt = (clone $now)->modify('+' . $decision['delay'] . ' seconds');
            $done = $this->messages->updateMessageRow($message, $values + [
                'status' => MessageStatus::PENDING->value,
                'retry_count' => $retriesSoFar + 1,
                'processing_after' => Utc::sql($availableAt),
                'consumed_by' => null,
                'processing_started_at' => null,
            ], MessageStatus::PROCESSING, $receipt);
        }

        if (!$done) {
            throw ApiProblem::leaseLost();
        }

        $this->logger->info('Message failed', [
            'message_id' => $id,
            'outcome' => $decision['outcome'],
            'attempt' => $retriesSoFar + 1,
            'permanent' => $permanent,
            'error_type' => $error['type'],
        ]);

        return [
            'outcome' => $decision['outcome'],
            'available_at' => Time::format($availableAt),
            'attempt' => $retriesSoFar + 1,
            'max_attempts' => $retryLimit + 1,
        ];
    }

    public function release(Principal $principal, string $id, Input $input): void
    {
        $input->allowOnly('receipt', 'delay_seconds');
        $receipt = $this->receipt($input);
        $delay = $input->has('delay_seconds') ? $this->integer($input, 'delay_seconds', 0) : 0;
        $input->validate();

        [$tenant] = $this->state($principal, $id);
        $message = $this->messages->findInTenant($tenant, $id);
        $availableAt = $delay > 0 ? Utc::sql(Utc::now('+' . $delay . ' seconds')) : null;

        // The attempt is not counted: retry_count stays as it is.
        $done = $message !== null && $this->messages->updateMessageRow($message, [
            'status' => MessageStatus::PENDING->value,
            'processing_after' => $availableAt,
            'consumed_by' => null,
            'processing_started_at' => null,
            'lease_until' => null,
        ], MessageStatus::PROCESSING, $receipt);

        if (!$done) {
            throw ApiProblem::leaseLost();
        }
    }

    /**
     * A fail with a receipt that no longer holds the lease. If that receipt's own fail
     * already went through (and nobody claimed the message since), answer as then.
     *
     * @param array<string, mixed> $state
     * @return array<string, mixed>
     */
    private function repeatedFail(string $tenant, string $id, string $receipt, QueueConfig $config, array $state): array
    {
        $state = $this->messages->leaseState($tenant, $id) ?? $state;
        $retryLimit = FailurePolicy::retryLimit($config, $state['max_attempts']);

        // The lease ran out before this fail: a late report, not a repeated one.
        if ($state['expired']) {
            throw ApiProblem::leaseLost();
        }

        if ($state['receipt'] === $receipt && $state['status'] === MessageStatus::PENDING->value) {
            return [
                'outcome' => FailurePolicy::RETRYING,
                'available_at' => $state['processing_after'] === null
                    ? null
                    : Time::format(Utc::parse($state['processing_after'])),
                'attempt' => $state['retry_count'],
                'max_attempts' => $retryLimit + 1,
            ];
        }

        if ($state['receipt'] === $receipt && $state['status'] === MessageStatus::FAILED->value) {
            return [
                'outcome' => FailurePolicy::DEAD_LETTERED,
                'available_at' => null,
                'attempt' => $state['retry_count'] + 1,
                'max_attempts' => $retryLimit + 1,
            ];
        }

        throw ApiProblem::leaseLost();
    }

    private function holdsLease(Message $message, string $receipt): bool
    {
        return $message->getStatus() === MessageStatus::PROCESSING
            && $message->getReceipt() === $receipt
            && $message->getLeaseUntil() !== null
            && $message->getLeaseUntil() > Utc::now();
    }

    /**
     * @return array<string, mixed> Lease
     */
    private function lease(Message $message, QueueConfig $config): array
    {
        $maxAttempts = min(
            $message->getMaxAttempts() ?? $config->int('max_attempts'),
            $config->int('max_attempts_limit'),
        );

        return [
            'receipt' => $message->getReceipt(),
            'lease_until' => Time::format($message->getLeaseUntil()),
            'message' => [
                'id' => $message->getId()->toString(),
                'queue' => $message->getQueue(),
                'body' => $message->getBody(),
                'headers' => (object) $message->getHeaders(),
                'attempt' => $message->getRetryCount() + 1,
                'max_attempts' => $maxAttempts,
                'created_at' => Time::format($message->getCreatedAt()),
            ],
        ];
    }

    /**
     * The message's tenant and lease state, after checking the key may use its queue.
     *
     * @return array{0: string, 1: array<string, mixed>}
     */
    private function state(Principal $principal, string $id): array
    {
        $tenant = $principal->tenant();
        $state = $this->messages->leaseState($tenant, $id);

        if ($state === null) {
            throw ApiProblem::notFound(sprintf('No message "%s".', $id));
        }

        $this->authorise($principal, $state['queue']);

        return [$tenant, $state];
    }

    private function authorise(Principal $principal, string $queue): string
    {
        if (!$principal->mayUseQueue($queue)) {
            throw ApiProblem::forbidden(sprintf('This key may not use queue "%s".', $queue));
        }

        return $principal->tenant();
    }

    private function receipt(Input $input): string
    {
        return $input->string('receipt', required: true, max: 64, pattern: self::RECEIPT) ?? '';
    }

    private function integer(Input $input, string $field, int $min, ?int $max = null): ?int
    {
        $value = $input->raw($field);

        if (!is_int($value) || $value < $min || ($max !== null && $value > $max)) {
            $input->addError($field, $max === null
                ? 'must be an integer of at least ' . $min
                : sprintf('must be an integer from %d to %d', $min, $max));

            return null;
        }

        return $value;
    }

    /**
     * @return array<string, string>
     */
    private function headers(Input $input): array
    {
        if (!$input->has('headers')) {
            return [];
        }

        $headers = $input->raw('headers');

        if (!is_array($headers) || ($headers !== [] && array_is_list($headers))) {
            $input->addError('headers', 'must be an object of strings');

            return [];
        }

        foreach ($headers as $name => $value) {
            if (!is_string($value)) {
                $input->addError('headers.' . $name, 'must be a string');
            }
        }

        return $headers;
    }

    private function availableAt(Input $input): ?DateTimeImmutable
    {
        if (!$input->has('available_at')) {
            return null;
        }

        $value = $input->raw('available_at');
        $time = is_string($value) ? DateTimeImmutable::createFromFormat(DATE_RFC3339, $value) : false;

        if ($time === false) {
            $time = is_string($value) ? DateTimeImmutable::createFromFormat(DATE_RFC3339_EXTENDED, $value) : false;
        }

        if ($time === false) {
            $input->addError('available_at', 'must be an RFC 3339 date-time');

            return null;
        }

        return $time->setTimezone(Utc::zone());
    }

    /**
     * @return array{type: ?string, message: string, details: mixed}
     */
    private function errorReport(Input $input): array
    {
        $error = $input->raw('error');

        if (!is_array($error) || ($error !== [] && array_is_list($error))) {
            $input->addError('error', $input->has('error') ? 'must be an object' : 'is required');

            return ['type' => null, 'message' => '', 'details' => null];
        }

        $report = Input::fromArray($error);
        $report->allowOnly('type', 'message', 'details');
        $type = $report->string('type');
        $message = $report->string('message', required: true, max: 65535);

        try {
            $report->validate();
        } catch (ApiProblem $problem) {
            foreach ($problem->errors as $problemError) {
                $input->addError('error.' . $problemError['field'], $problemError['message']);
            }
        }

        return ['type' => $type, 'message' => $message ?? '', 'details' => $error['details'] ?? null];
    }
}
