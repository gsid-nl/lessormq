package dev.lessormq.spring;

import dev.lessormq.LessorMQClient;
import dev.lessormq.Worker;
import dev.lessormq.WorkerOptions;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;

/**
 * The workers of the @LessorMQListener methods: one per method, started with the
 * application context and stopped before it closes. Stopping lets every handler finish
 * and report; a claim that is waiting ends within lessormq.worker.wait-seconds, and a
 * message it brings is given back.
 */
public class LessorMQListenerContainer implements SmartLifecycle {
    /** Started after, and stopped before, everything in the default phase but the web server. */
    public static final int PHASE = SmartLifecycle.DEFAULT_PHASE - 100;

    private final ObjectProvider<LessorMQClient> client;
    private final LessorMQProperties properties;
    private final List<Listener> listeners = new ArrayList<>();
    private volatile List<Worker> workers = List.of();

    /** One listener: its method and the options of its worker. */
    record Listener(String queue, WorkerOptions options, ListenerMethod method) {
    }

    public LessorMQListenerContainer(ObjectProvider<LessorMQClient> client, LessorMQProperties properties) {
        this.client = client;
        this.properties = properties;
    }

    synchronized void register(Listener listener) {
        listeners.add(listener);
    }

    /** The workers of the current run, one per listener; empty while stopped. */
    public List<Worker> getWorkers() {
        return workers;
    }

    @Override
    public synchronized void start() {
        if (isRunning() || listeners.isEmpty()) {
            return;
        }
        LessorMQClient lessorMQ = client.getObject();
        List<Worker> started = new ArrayList<>();
        for (Listener listener : listeners) {
            Worker worker = new Worker(lessorMQ, listener.queue(), listener.options(), listener.method());
            worker.start();
            started.add(worker);
        }
        workers = List.copyOf(started);
    }

    @Override
    public void stop() {
        List<Worker> stopping = workers;
        stopping.forEach(Worker::stop);
        stopping.forEach(Worker::close);
        stopped(stopping);
    }

    /** Forgets the workers of a run, unless a new run has started meanwhile. */
    private synchronized void stopped(List<Worker> stopping) {
        if (workers == stopping) {
            workers = List.of();
        }
    }

    @Override
    public void stop(Runnable callback) {
        List<Worker> stopping = workers;
        if (stopping.isEmpty()) {
            callback.run();
            return;
        }
        stopping.forEach(Worker::stop);
        // Handlers finish on their own threads; Spring waits for the callback up to
        // spring.lifecycle.timeout-per-shutdown-phase.
        Thread waiter = new Thread(() -> {
            stopping.forEach(Worker::close);
            stopped(stopping);
            callback.run();
        }, "lessormq-stop");
        waiter.setDaemon(true);
        waiter.start();
    }

    @Override
    public boolean isRunning() {
        return workers.stream().anyMatch(Worker::isRunning);
    }

    @Override
    public boolean isAutoStartup() {
        return properties.getWorker().isAutoStartup();
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
