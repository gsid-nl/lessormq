<?php

declare(strict_types=1);

namespace LessorMQ\Laravel;

use LessorMQ\Client;
use LessorMQ\Exception\LeaseLostException;
use LessorMQ\Exception\LessorMQException;
use LessorMQ\Lease;
use Psr\Log\LoggerInterface;

/**
 * Keeps a lease alive while its job runs, on SIGALRM (ext-pcntl), a third of the lease
 * at a time.
 *
 * queue:work uses the same alarm for its job timeout (--timeout, or the job's $timeout):
 * it arms it just before the job starts. The keeper takes the alarm over for the job's
 * run, remembers when the worker's alarm was due, and at that moment hands the signal to
 * the worker's own handler, which ends the process as it always does. Afterwards the
 * worker gets its alarm back, still set for its own deadline.
 *
 * The extends come from a signal handler, in the middle of whatever the job is doing, so
 * they go through a Client of their own (see HttpClientFactory). An extend that fails is
 * logged and tried again at the next alarm; it never fails the job.
 */
final class LeaseKeeper
{
    /** The job's lease, while it runs. */
    private ?Lease $lease = null;

    /** @var (callable(Client): void)|null */
    private $onTimeout = null;

    /** When the worker's own alarm is due (Unix time), if it set one. */
    private ?int $deadline = null;

    /** @var callable|int|null the worker's SIGALRM handler */
    private $previousHandler = null;

    private bool $previousAsync = false;
    private int $interval = 1;
    private bool $extending = false;
    private bool $handedOver = false;

    /**
     * @param Client   $client       used only from the signal handler
     * @param int|null $leaseSeconds each extend's lease; the queue's when null
     */
    public function __construct(
        private readonly Client $client,
        private readonly ?int $leaseSeconds,
        private readonly LoggerInterface $logger,
    ) {
    }

    public static function supported(): bool
    {
        return function_exists('pcntl_alarm')
            && function_exists('pcntl_signal')
            && function_exists('pcntl_signal_get_handler')
            && function_exists('pcntl_async_signals');
    }

    /**
     * Extend the lease until the returned callable is called. One job at a time.
     *
     * @param callable(Client): void $onTimeout runs when the worker's timeout for the job is
     *                                         reached, before the worker's handler, with the
     *                                         keeper's client (it runs in the signal handler)
     * @return callable(): void stops extending and gives the alarm back
     */
    public function keep(Lease $lease, callable $onTimeout): callable
    {
        if (!self::supported()) {
            return static function (): void {
            };
        }

        // pcntl_alarm(0) cancels the worker's alarm and says how far off it was.
        $left = pcntl_alarm(0);
        $this->lease = $lease;
        $this->onTimeout = $onTimeout;
        $this->deadline = $left > 0 ? time() + $left : null;
        $this->previousHandler = pcntl_signal_get_handler(SIGALRM);
        $this->previousAsync = pcntl_async_signals(true);
        $this->interval = max(1, (int) floor($lease->secondsLeft() / 3));
        $this->extending = true;
        $this->handedOver = false;

        pcntl_signal(SIGALRM, fn(int $signal, mixed $info = null) => $this->alarm($signal, $info));
        $this->arm();

        return fn() => $this->stop();
    }

    private function alarm(int $signal, mixed $info): void
    {
        if ($this->deadline !== null && time() >= $this->deadline) {
            // The worker's timeout: report it, then let the worker's handler end the job.
            $this->handedOver = true;
            pcntl_signal(SIGALRM, $this->previousHandler ?? SIG_DFL);

            try {
                if ($this->onTimeout !== null) {
                    ($this->onTimeout)($this->client);
                }
            } finally {
                if (is_callable($this->previousHandler)) {
                    ($this->previousHandler)($signal, $info);
                } elseif ($this->previousHandler === SIG_DFL && function_exists('posix_kill')) {
                    posix_kill(getmypid(), SIGALRM);
                }
            }

            return;
        }

        if ($this->extending && $this->lease !== null) {
            $this->extending = $this->extend($this->lease);
        }

        $this->arm();
    }

    /**
     * The next alarm: the next extend, or the worker's deadline when that comes first.
     */
    private function arm(): void
    {
        if ($this->deadline === null) {
            pcntl_alarm($this->extending ? $this->interval : 0);

            return;
        }

        $untilDeadline = max(1, $this->deadline - time());
        pcntl_alarm($this->extending ? min($this->interval, $untilDeadline) : $untilDeadline);
    }

    private function stop(): void
    {
        pcntl_alarm(0);
        pcntl_signal(SIGALRM, $this->previousHandler ?? SIG_DFL);
        pcntl_async_signals($this->previousAsync);

        if ($this->deadline !== null && !$this->handedOver) {
            // The worker's alarm, set for its own deadline again.
            pcntl_alarm(max(1, $this->deadline - time()));
        }

        $this->lease = null;
        $this->onTimeout = null;
    }

    /**
     * @return bool whether to keep extending: not once the lease is gone
     */
    private function extend(Lease $lease): bool
    {
        try {
            $this->client->extend($lease, $this->leaseSeconds);
        } catch (LeaseLostException) {
            $this->logger->warning('LessorMQ lease lost while the job ran; the message is no longer this worker\'s', [
                'message_id' => $lease->message->id,
            ]);

            return false;
        } catch (LessorMQException $e) {
            $this->logger->warning('Extending the LessorMQ lease failed; the next alarm tries again', [
                'message_id' => $lease->message->id,
                'error' => $e->getMessage(),
            ]);
        }

        return true;
    }
}
