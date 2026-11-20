<?php

declare(strict_types=1);

namespace App\Service;

use App\Model\QueueConfig;
use App\Repository\MessageRepository;
use App\Time\Utc;
use Psr\Log\LoggerInterface;

/**
 * Ends expired leases. Runs on the claim path of a queue, at most once a minute per
 * queue across all workers, whether or not the queue has work: a busy queue's crashed
 * worker no longer waits for the backlog to drain before its message comes back.
 */
class LeaseKeeper
{
    /**
     * @param int $leaseSweepSeconds how often a queue's expired leases are ended, at most
     *                               (LEASE_SWEEP_SECONDS); the conformance suite runs at 1
     */
    public function __construct(
        private readonly MessageRepository $messages,
        private readonly QueueThrottleService $throttle,
        private readonly LoggerInterface $logger,
        private readonly int $leaseSweepSeconds = 60,
    ) {
    }

    public function sweepIfDue(string $tenant, string $queue, QueueConfig $config): void
    {
        if (!$this->throttle->attempt('recovery:' . $tenant . ':' . $queue, max(1, $this->leaseSweepSeconds))) {
            return;
        }

        try {
            $result = $this->messages->expireLeases($tenant, $queue, $config, Utc::now());

            if (array_sum($result) > 0) {
                $this->logger->info('Expired leases ended', ['tenant' => $tenant, 'queue' => $queue] + $result);
            }
        } catch (\Exception $e) {
            $this->logger->warning('Lease sweep failed', [
                'tenant' => $tenant,
                'queue' => $queue,
                'error' => $e->getMessage(),
            ]);
        }
    }
}
