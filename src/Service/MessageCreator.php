<?php

declare(strict_types=1);

namespace App\Service;

use App\Entity\Message;
use App\Repository\MessageRepository;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Psr\Log\LoggerInterface;

/** Puts a new message in the table, safely under concurrent publishes. */
class MessageCreator
{
    private const INSERT_ATTEMPTS = 3;

    public function __construct(
        private readonly MessageRepository $messageRepository,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * Insert a new message, or return the open one with the same dedup_key.
     *
     * The key is enforced by a unique index, not by checking first: two publishes with
     * the same key at the same moment both try to insert, one loses on the index and
     * returns the winner's message.
     *
     * Ids are UUIDv1 (microsecond + random 14-bit clock sequence + node), so two
     * workers in one pod can draw the same id in the same microsecond. The failed
     * insert wrote nothing, so a retry with a new id is the same publish, not a
     * duplicate. Doctrine closes the EntityManager on the failed flush, hence the reset.
     *
     * @param (callable(Message): void)|null $configure sets the optional fields of each
     *                                                  attempt's fresh message
     * @return array{0: Message, 1: bool} the message, and whether it already existed
     */
    public function create(
        string $tenant,
        string $queue,
        mixed $body,
        ?callable $configure = null,
        ?string $dedupKey = null,
    ): array {
        if ($dedupKey !== null) {
            $existing = $this->messageRepository->findOpenByDedupKey($tenant, $queue, $dedupKey);

            if ($existing !== null) {
                return [$existing, true];
            }
        }

        $attempt = 0;

        while (true) {
            $message = new Message($tenant, $queue, $body);
            $message->setDedupKey($dedupKey);

            if ($configure !== null) {
                $configure($message);
            }

            try {
                $this->messageRepository->add($message, true);

                return [$message, false];
            } catch (UniqueConstraintViolationException $e) {
                $this->messageRepository->resetManager();

                if ($dedupKey !== null) {
                    $existing = $this->messageRepository->findOpenByDedupKey($tenant, $queue, $dedupKey);

                    if ($existing !== null) {
                        return [$existing, true];
                    }
                }

                if (++$attempt >= self::INSERT_ATTEMPTS) {
                    throw $e;
                }

                $this->logger->warning('Message id collided, retrying with a fresh id', [
                    'queue' => $queue,
                    'attempt' => $attempt,
                ]);
            }
        }
    }
}
