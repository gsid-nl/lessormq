package dev.lessormq;

import static dev.lessormq.FakeServer.FAILED;
import static dev.lessormq.FakeServer.LEASE;
import static dev.lessormq.FakeServer.noContent;
import static dev.lessormq.FakeServer.ok;
import static dev.lessormq.FakeServer.problem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import tools.jackson.databind.JsonNode;

@Timeout(20)
class WorkerTest {
    private FakeServer fake;

    @BeforeEach
    void start() throws Exception {
        fake = new FakeServer();
    }

    @AfterEach
    void stop() {
        fake.close();
    }

    private Worker worker(WorkerOptions.Builder options, Handler handler) {
        WorkerOptions built = options.build();
        return new Worker(fake.client(), "mail", WorkerOptions.builder()
                .consumer("w1")
                .waitSeconds(1)
                .maxMessages(built.maxMessages() == 0 ? 1 : built.maxMessages())
                .concurrency(built.concurrency())
                .extendEvery(built.extendEvery())
                .autoExtend(built.autoExtend())
                .errorPause(Duration.ofMillis(10))
                .build(), handler);
    }

    @Test
    void aHandlerThatReturnsAcks() {
        fake.on("/v1/queues/mail/claims", ok(LEASE)).on("/v1/messages/m1/ack", noContent());
        AtomicReference<Message> seen = new AtomicReference<>();

        Worker worker = worker(WorkerOptions.builder(), job -> seen.set(job.message()));
        worker.run();

        assertEquals(1, worker.handled());
        assertEquals("m1", seen.get().id());
        assertEquals(1, fake.calls("/v1/messages/m1/ack").size());
        JsonNode claim = fake.calls("/v1/queues/mail/claims").get(0);
        assertEquals("w1", claim.path("consumer").asString());
        assertEquals(1, claim.path("wait_seconds").asInt());
        assertFalse(worker.isRunning());
    }

    @Test
    void aHandlerThatThrowsIsReportedAndTheWorkerGoesOn() {
        fake.on("/v1/queues/mail/claims", ok(LEASE)).on("/v1/messages/m1/fail", ok(FAILED));
        AtomicInteger calls = new AtomicInteger();

        Worker worker = worker(WorkerOptions.builder().maxMessages(3), job -> {
            switch (calls.incrementAndGet()) {
                case 1 -> throw new PermanentFailure("no recipient");
                case 2 -> throw new java.io.IOException("upstream slow");
                default -> throw new StackOverflowError();
            }
        });
        worker.run();

        List<JsonNode> sent = fake.calls("/v1/messages/m1/fail");
        assertEquals(3, worker.handled());
        assertEquals(3, sent.size());
        assertTrue(sent.get(0).path("permanent").asBoolean());
        assertEquals("dev.lessormq.PermanentFailure", sent.get(0).path("error").path("type").asString());
        assertFalse(sent.get(1).path("permanent").asBoolean());
        assertEquals("java.io.IOException", sent.get(1).path("error").path("type").asString());
        assertEquals("upstream slow", sent.get(1).path("error").path("message").asString());
        assertEquals("java.lang.StackOverflowError", sent.get(2).path("error").path("type").asString());
    }

    @Test
    void theLeaseIsExtendedWhileTheHandlerRuns() {
        fake.on("/v1/queues/mail/claims", ok(LEASE))
                .on("/v1/messages/m1/extend", ok("{\"lease_until\":\"2030-01-01T00:05:00Z\"}"))
                .on("/v1/messages/m1/ack", noContent());

        Worker worker = worker(WorkerOptions.builder().extendEvery(Duration.ofMillis(50)),
                job -> Thread.sleep(400));
        worker.run();

        assertTrue(fake.calls("/v1/messages/m1/extend").size() >= 3);
        assertEquals("r1", fake.calls("/v1/messages/m1/extend").get(0).path("receipt").asString());
        assertEquals(1, fake.calls("/v1/messages/m1/ack").size());
        List<FakeServer.Request> requests = fake.requests();
        assertEquals("/v1/messages/m1/ack", requests.get(requests.size() - 1).path(),
                "no extend after the report");
    }

    @Test
    void aLostLeaseIsSeenByTheHandlerAndNotReported() {
        fake.on("/v1/queues/mail/claims", ok(LEASE)).on("/v1/messages/m1/extend", problem(409, "lease_lost"));
        AtomicBoolean sawLoss = new AtomicBoolean();

        Worker worker = worker(WorkerOptions.builder().extendEvery(Duration.ofMillis(20)), job -> {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!job.isLeaseLost() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            sawLoss.set(job.isLeaseLost());
            throw new IllegalStateException("gave up");
        });
        worker.run();

        assertTrue(sawLoss.get());
        assertEquals(1, fake.calls("/v1/messages/m1/extend").size());
        assertTrue(fake.calls("/v1/messages/m1/fail").isEmpty());
        assertTrue(fake.calls("/v1/messages/m1/ack").isEmpty());
    }

    @Test
    void aLostAckDoesNotStopTheWorker() {
        fake.on("/v1/queues/mail/claims", ok(LEASE)).on("/v1/messages/m1/ack", problem(409, "lease_lost"));

        Worker worker = worker(WorkerOptions.builder().maxMessages(2), job -> { });
        worker.run();

        assertEquals(2, worker.handled());
    }

    @Test
    void aClaimFailurePausesAndGoesOn() {
        fake.on("/v1/queues/mail/claims", problem(401, "unauthorized"), ok(LEASE))
                .on("/v1/messages/m1/ack", noContent());

        Worker worker = worker(WorkerOptions.builder(), job -> { });
        worker.run();

        assertEquals(1, worker.handled());
        assertEquals(2, fake.calls("/v1/queues/mail/claims").size());
    }

    @Test
    void stoppingDuringAClaimGivesTheMessageBack() throws Exception {
        CountDownLatch claiming = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        fake.on("/v1/queues/mail/claims", ok(LEASE)).on("/v1/messages/m1/release", noContent()).onRequest(path -> {
            if (path.endsWith("/claims")) {
                claiming.countDown();
                try {
                    stopped.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        AtomicBoolean ran = new AtomicBoolean();

        Worker worker = worker(WorkerOptions.builder().maxMessages(5), job -> ran.set(true));
        worker.start();
        assertTrue(claiming.await(5, TimeUnit.SECONDS));
        worker.stop();
        stopped.countDown();
        assertTrue(worker.awaitTermination(Duration.ofSeconds(5)));

        assertFalse(ran.get());
        assertEquals(0, worker.handled());
        assertEquals(1, fake.calls("/v1/messages/m1/release").size());
        assertEquals(1, fake.calls("/v1/queues/mail/claims").size());
    }

    @Test
    void interruptingRunStopsAfterTheCurrentMessage() throws Exception {
        CountDownLatch handling = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        fake.on("/v1/queues/mail/claims", ok(LEASE)).on("/v1/messages/m1/ack", noContent());

        Worker worker = worker(WorkerOptions.builder().maxMessages(100), job -> {
            handling.countDown();
            release.await();
        });
        Thread runner = new Thread(worker::run);
        runner.start();
        assertTrue(handling.await(5, TimeUnit.SECONDS));
        runner.interrupt();
        release.countDown();
        runner.join(5000);

        assertFalse(runner.isAlive());
        assertEquals(1, worker.handled());
        assertEquals(1, fake.calls("/v1/messages/m1/ack").size());
    }

    @Test
    void concurrencyRunsHandlersSideBySideAndKeepsToMaxMessages() {
        fake.on("/v1/queues/mail/claims", ok(LEASE))
                .on("/v1/messages/m1/ack", noContent())
                .on("/v1/messages/m1/release", noContent());
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger most = new AtomicInteger();

        Worker worker = worker(WorkerOptions.builder().concurrency(4).maxMessages(8), job -> {
            most.accumulateAndGet(inside.incrementAndGet(), Math::max);
            Thread.sleep(100);
            inside.decrementAndGet();
        });
        worker.run();

        assertEquals(8, worker.handled());
        assertEquals(8, fake.calls("/v1/messages/m1/ack").size());
        assertTrue(most.get() > 1, "handlers ran side by side: " + most.get());
    }

    @Test
    void aWorkerRunsOnce() {
        fake.on("/v1/queues/mail/claims", ok(LEASE)).on("/v1/messages/m1/ack", noContent());
        Worker worker = worker(WorkerOptions.builder(), job -> { });
        worker.run();

        assertThrows(IllegalStateException.class, worker::start);
    }
}
