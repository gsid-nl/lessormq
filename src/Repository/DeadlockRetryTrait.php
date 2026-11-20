<?php

declare(strict_types=1);

namespace App\Repository;

use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Exception\LockWaitTimeoutException;

/**
 * Retry helper for single-statement writes that InnoDB may pick as a deadlock victim.
 *
 * Only safe for statements running in autocommit: MariaDB rolls the whole transaction
 * back when it reports 1213, so the caller must have nothing else pending. For the claim
 * UPDATE that holds — a rolled-back claim means no row was taken, and re-running it is
 * the same operation, not a duplicate.
 *
 * Do NOT wrap ORM flushes with this: Doctrine closes the EntityManager on a DBAL failure,
 * so retrying needs the manager reset and the entities reloaded first.
 */
trait DeadlockRetryTrait
{
    private const DEADLOCK_ATTEMPTS = 3;

    /**
     * @template T
     * @param callable():T $operation
     * @return T
     */
    private function retryOnDeadlock(callable $operation): mixed
    {
        $attempt = 0;

        while (true) {
            try {
                return $operation();
            } catch (DeadlockException | LockWaitTimeoutException $e) {
                if (++$attempt >= self::DEADLOCK_ATTEMPTS) {
                    throw $e;
                }

                // Jittered backoff: the two workers that just collided must not line up
                // again on the retry.
                usleep(random_int(1_000, 5_000) * $attempt);
            }
        }
    }
}
