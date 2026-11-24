package dev.lessormq;

import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.jspecify.annotations.Nullable;

/**
 * Claims messages of one queue and runs a handler on each.
 *
 * <pre>{@code
 * Worker worker = new Worker(client, "mail", WorkerOptions.builder().concurrency(4).build(), job -> {
 *     send(job.message().decode(Mail.class));
 * });
 * worker.run(); // until SIGTERM or Ctrl+C, after which the current messages finish
 * }</pre>
 *
 * <p>The handler returns: the message is acked. It throws: the message is failed with the
 * exception's class and text, as permanent when {@link PermanentFailure#isPermanent}. What
 * happens next (another attempt, when, or the dead-letter set) is decided by the queue.
 * While the handler runs, the lease is extended on a timer, every third of the lease.
 *
 * <p>A worker runs once: {@link #run()} or {@link #start()}, then {@link #stop()} or
 * {@link #close()}; run also stops when the JVM shuts down. Stopping lets the current messages finish: handlers are not
 * interrupted, and a claim that is already waiting is not cut off (that could leave a
 * leased message to run out). It ends within the claim's wait, and a message it brings is
 * given back with a release.
 */
public final class Worker implements AutoCloseable {
    private final LessorMQClient client;
    private final String queue;
    private final Handler handler;
    private final WorkerOptions options;
    private final ClaimOptions claimOptions;
    private final System.Logger logger;
    private final AtomicInteger handled = new AtomicInteger();
    // Messages taken on, so concurrent threads never pass maxMessages.
    private final AtomicInteger started = new AtomicInteger();
    private final AtomicBoolean launched = new AtomicBoolean();
    private final CountDownLatch stopSignal = new CountDownLatch(1);
    private volatile boolean stopping;
    private volatile @Nullable CountDownLatch running;
    private @Nullable ScheduledThreadPoolExecutor keepers;

    /** A worker for queue with the default options. */
    public Worker(LessorMQClient client, String queue, Handler handler) {
        this(client, queue, WorkerOptions.DEFAULTS, handler);
    }

    /** A worker for queue on client, running handler on every message. */
    public Worker(LessorMQClient client, String queue, WorkerOptions options, Handler handler) {
        this.client = client;
        this.queue = queue;
        this.handler = handler;
        this.options = options;
        this.logger = client.logger();
        this.claimOptions = ClaimOptions.builder()
                .consumer(options.consumer())
                .route(options.route())
                .waitSeconds(options.waitSeconds())
                .leaseSeconds(options.leaseSeconds())
                .build();
    }

    public String queue() {
        return queue;
    }

    /**
     * Claims and handles messages until {@link #stop()}, an interrupt of the calling
     * thread, maxMessages, or the JVM shutting down, and returns once every message in hand
     * is reported.
     */
    public void run() {
        Thread hook = options.stopOnShutdown() ? shutdownHook() : null;
        try {
            start();
            CountDownLatch latch = running;
            boolean interrupted = false;
            while (latch != null) {
                try {
                    latch.await();
                    break;
                } catch (InterruptedException e) {
                    // An interrupt asks to stop; the messages in hand still finish.
                    interrupted = true;
                    stop();
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        } finally {
            if (hook != null) {
                try {
                    Runtime.getRuntime().removeShutdownHook(hook);
                } catch (IllegalStateException e) {
                    // The JVM is shutting down: the hook is running.
                }
            }
        }
    }

    /** A hook that stops the worker and holds the JVM's shutdown until its messages are reported. */
    private @Nullable Thread shutdownHook() {
        Thread hook = new Thread(this::close, "lessormq-" + queue + "-shutdown");
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            return hook;
        } catch (IllegalStateException e) {
            return null;
        }
    }

    /** Starts the worker's threads and returns at once. */
    public void start() {
        if (!launched.compareAndSet(false, true)) {
            throw new IllegalStateException("A worker runs once; make a new one for queue " + queue);
        }
        CountDownLatch latch = new CountDownLatch(options.concurrency());
        running = latch;
        ThreadFactory factory = options.threadFactory();
        for (int i = 1; i <= options.concurrency(); i++) {
            Runnable loop = () -> {
                try {
                    loop();
                } finally {
                    latch.countDown();
                    if (latch.getCount() == 0) {
                        stopKeepers();
                    }
                }
            };
            Thread thread = factory != null ? factory.newThread(loop) : new Thread(loop, "lessormq-" + queue + "-" + i);
            thread.start();
        }
    }

    /** Asks the worker to stop after the current messages, and returns at once. Safe from any thread. */
    public void stop() {
        stopping = true;
        stopSignal.countDown();
    }

    /**
     * Waits until the worker has stopped and every message in hand is reported.
     *
     * @return false when the timeout ran out first
     */
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        CountDownLatch latch = running;
        return latch == null || latch.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** Stops the worker and waits until every message in hand is reported. */
    @Override
    public void close() {
        stop();
        CountDownLatch latch = running;
        if (latch != null) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        stopKeepers();
    }

    /** Whether the worker's threads are still running. */
    public boolean isRunning() {
        CountDownLatch latch = running;
        return latch != null && latch.getCount() > 0;
    }

    /** The number of messages handled so far. */
    public int handled() {
        return handled.get();
    }

    private void loop() {
        while (!stopping && !done() && !Thread.currentThread().isInterrupted()) {
            runOnce();
        }
    }

    private boolean done() {
        return options.maxMessages() > 0 && started.get() >= options.maxMessages();
    }

    /** Reserves one of maxMessages for a claimed message. */
    private boolean take() {
        if (options.maxMessages() <= 0) {
            return true;
        }
        if (started.incrementAndGet() > options.maxMessages()) {
            started.decrementAndGet();
            return false;
        }
        return true;
    }

    /**
     * Makes one claim and runs the handler on what it got. Once the worker is stopping, a
     * message the claim brings is given back.
     *
     * @return whether a message was handled
     */
    public boolean runOnce() {
        Lease lease;
        try {
            lease = client.claim(queue, claimOptions);
        } catch (RuntimeException e) {
            if (Thread.currentThread().isInterrupted()) {
                return false;
            }
            logger.log(Level.ERROR, "Claim failed for queue " + queue + "; trying again in "
                    + options.errorPause().toMillis() + " ms", e);
            try {
                stopSignal.await(options.errorPause().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
        if (lease == null) {
            return false;
        }
        if (stopping || !take()) {
            // Asked to stop while the claim waited, or the other threads reached
            // maxMessages: give the message back untouched.
            report(lease, "release", () -> client.release(lease));
            return false;
        }

        handle(lease);
        handled.incrementAndGet();
        return true;
    }

    private void handle(Lease lease) {
        Job job = new Job(client, lease);
        Keeper keeper = options.autoExtend() ? new Keeper(job) : null;
        Throwable error = null;
        try {
            handler.handle(job);
        } catch (Throwable e) {
            error = e;
        } finally {
            if (keeper != null) {
                keeper.stop();
            }
        }

        // A handler that was interrupted is reported first; the loop then sees the flag.
        boolean interrupted = Thread.interrupted() || error instanceof InterruptedException;
        try {
            if (job.isLeaseLost()) {
                logger.log(Level.WARNING, () -> "Lease lost while the handler ran; not reporting message "
                        + lease.message().id());
                return;
            }
            if (error == null) {
                report(lease, "ack", () -> client.ack(lease));
                return;
            }
            Throwable failure = error;
            report(lease, "fail", () -> {
                FailResult result = client.fail(lease, failure);
                logger.log(Level.INFO, () -> "Message " + lease.message().id() + " failed (" + failure
                        + "): " + result.outcome() + ", attempt " + result.attempt() + " of " + result.maxAttempts());
            });
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Acks, fails or releases, keeping the worker alive whatever happens: a lost lease means
     * the message is someone else's now; any other failure means the lease will expire and
     * the server will make the message available again.
     */
    private void report(Lease lease, String what, Runnable call) {
        try {
            call.run();
        } catch (LeaseLostException e) {
            logger.log(Level.WARNING, () -> "Lease lost before " + what + "; message " + lease.message().id()
                    + " is no longer ours");
        } catch (RuntimeException e) {
            logger.log(Level.ERROR, "The " + what + " of message " + lease.message().id()
                    + " failed; the lease will expire and the message come back", e);
        }
    }

    private synchronized ScheduledThreadPoolExecutor keepers() {
        if (keepers == null) {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(options.concurrency(), task -> {
                Thread thread = new Thread(task, "lessormq-" + queue + "-extend");
                thread.setDaemon(true);
                return thread;
            });
            executor.setRemoveOnCancelPolicy(true);
            keepers = executor;
        }
        return keepers;
    }

    private synchronized void stopKeepers() {
        if (keepers != null) {
            keepers.shutdown();
            keepers = null;
        }
    }

    /**
     * Extends one lease every interval until stopped. A lost lease marks the job and ends
     * the extending. Stopping waits for an extend in flight, so it never crosses the report.
     */
    private final class Keeper implements Runnable {
        private final Job job;
        private final ReentrantLock lock = new ReentrantLock();
        private final ScheduledThreadPoolExecutor executor;
        private boolean stopped;
        private @Nullable ScheduledFuture<?> next;

        Keeper(Job job) {
            this.job = job;
            this.executor = keepers();
            lock.lock();
            try {
                schedule();
            } finally {
                lock.unlock();
            }
        }

        private void schedule() {
            Duration interval = options.extendEvery();
            if (interval == null || interval.isZero() || interval.isNegative()) {
                Duration third = job.lease().remaining().dividedBy(3);
                interval = third.compareTo(Duration.ofSeconds(1)) < 0 ? Duration.ofSeconds(1) : third;
            }
            next = executor.schedule(this, interval.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public void run() {
            lock.lock();
            try {
                if (stopped) {
                    return;
                }
                try {
                    client.extend(job.lease(), options.leaseSeconds());
                } catch (LeaseLostException e) {
                    job.markLeaseLost();
                    return;
                } catch (RuntimeException e) {
                    logger.log(Level.WARNING, () -> "Extend failed for message " + job.message().id()
                            + "; trying again: " + e);
                }
                if (!stopped) {
                    schedule();
                }
            } finally {
                lock.unlock();
            }
        }

        void stop() {
            lock.lock();
            try {
                stopped = true;
                if (next != null) {
                    next.cancel(false);
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
