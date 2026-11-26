package dev.lessormq;

import static dev.lessormq.FakeServer.FAILED;
import static dev.lessormq.FakeServer.LEASE;
import static dev.lessormq.FakeServer.PUBLISHED;
import static dev.lessormq.FakeServer.answer;
import static dev.lessormq.FakeServer.drop;
import static dev.lessormq.FakeServer.noContent;
import static dev.lessormq.FakeServer.ok;
import static dev.lessormq.FakeServer.problem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.NullNode;

class ClientTest {
    private FakeServer fake;

    @BeforeEach
    void start() throws Exception {
        fake = new FakeServer();
    }

    @AfterEach
    void stop() {
        fake.close();
    }

    record Mail(String to) {
    }

    @Test
    void publishSendsTheBodyAndAnAutomaticDedupKey() {
        fake.on("/v1/queues/mail/messages", answer(201, PUBLISHED, Map.of()));

        Published published = fake.client().publish("mail", new Mail("ada@example.com"),
                PublishOptions.builder().header("type", "Welcome").delaySeconds(5).maxAttempts(3).build());

        assertEquals("m1", published.id());
        assertEquals(Instant.parse("2030-01-01T00:00:00Z"), published.createdAt());
        assertFalse(published.duplicate());
        FakeServer.Request request = fake.requests().get(0);
        assertEquals("POST", request.method());
        assertEquals(List.of("Bearer lmq_key"), request.headers().get("Authorization"));
        assertEquals(List.of(LessorMQClient.USER_AGENT), request.headers().get("User-agent"));
        assertTrue(LessorMQClient.USER_AGENT.matches("lessormq-java/\\d+\\.\\d+\\.\\d+.*"), LessorMQClient.USER_AGENT);
        JsonNode sent = request.body();
        assertEquals("ada@example.com", sent.path("body").path("to").asString());
        assertEquals("Welcome", sent.path("headers").path("type").asString());
        assertEquals(5, sent.path("delay_seconds").asInt());
        assertEquals(3, sent.path("max_attempts").asInt());
        assertTrue(sent.path("dedup_key").asString().matches("lmq-auto-[0-9a-f]{24}"), sent.toString());
    }

    @Test
    void publishKeepsNullBodiesEncodesQueuesAndWritesUtcToTheSecond() {
        fake.on("/v1/queues/a%20b%2Fc/messages", answer(201, PUBLISHED, Map.of()));
        Instant at = OffsetDateTime.of(2030, 1, 1, 2, 0, 0, 500_000_000, ZoneOffset.ofHours(2)).toInstant();

        fake.client().publish("a b/c", null, PublishOptions.builder().availableAt(at).dedupKey("k").build());

        JsonNode sent = fake.calls("/v1/queues/a%20b%2Fc/messages").get(0);
        assertTrue(sent.has("body") && sent.path("body").isNull(), sent.toString());
        assertEquals("2030-01-01T00:00:00Z", sent.path("available_at").asString());
        assertEquals("k", sent.path("dedup_key").asString());
        assertFalse(sent.has("headers"));
    }

    @Test
    void publishWithoutADedupKeyIsNotRetried() {
        fake.on("/v1/queues/mail/messages", problem(503, "unavailable"));
        LessorMQClient client = fake.builder().autoDedupKey(false).build();

        assertThrows(ServerException.class, () -> client.publish("mail", Map.of()));
        assertEquals(1, fake.count());
        assertFalse(fake.calls("/v1/queues/mail/messages").get(0).has("dedup_key"));
    }

    @Test
    void claimReturnsNullWhenNothingArrived() {
        fake.on("/v1/queues/mail/claims", noContent());

        assertNull(fake.client().claim("mail", ClaimOptions.builder().waitSeconds(1).consumer("w1").build()));
        JsonNode sent = fake.calls("/v1/queues/mail/claims").get(0);
        assertEquals(1, sent.path("wait_seconds").asInt());
        assertEquals("w1", sent.path("consumer").asString());
        assertFalse(sent.has("route"));
    }

    @Test
    void claimReturnsTheLease() {
        fake.on("/v1/queues/mail/claims", ok(LEASE));

        Lease lease = fake.client().claim("mail");

        assertNotNull(lease);
        assertEquals("r1", lease.receipt());
        assertEquals(Instant.parse("2030-01-01T00:00:30Z"), lease.until());
        Message message = lease.message();
        assertEquals("m1", message.id());
        assertEquals(Map.of("type", "Welcome"), message.headers());
        assertEquals(1, message.attempt());
        assertEquals(7, message.maxAttempts());
        assertFalse(message.isLastAttempt());
        assertEquals(new Mail("ada@example.com"), message.decode(Mail.class));
        assertEquals(Map.of("to", "ada@example.com"), message.decode(new TypeReference<Map<String, String>>() { }));
    }

    @Test
    void aBodyThatDoesNotDecodeIsAPermanentFailure() {
        fake.on("/v1/queues/mail/claims", ok(LEASE));
        Lease lease = fake.client().claim("mail");
        assertNotNull(lease);

        PermanentFailure e = assertThrows(PermanentFailure.class, () -> lease.message().decode(Integer.class));
        assertTrue(PermanentFailure.isPermanent(e));
        Message empty = new Message("m2", "mail", NullNode.getInstance(), Map.of(), 1, 7, Instant.now());
        assertThrows(PermanentFailure.class, () -> empty.decode(Mail.class));
    }

    @Test
    void extendMovesTheLease() {
        fake.on("/v1/queues/mail/claims", ok(LEASE))
                .on("/v1/messages/m1/extend", ok("{\"lease_until\":\"2030-01-01T00:05:00Z\"}"));
        LessorMQClient client = fake.client();
        Lease lease = client.claim("mail");
        assertNotNull(lease);

        client.extend(lease, 300);

        assertEquals(Instant.parse("2030-01-01T00:05:00Z"), lease.until());
        JsonNode sent = fake.calls("/v1/messages/m1/extend").get(0);
        assertEquals("r1", sent.path("receipt").asString());
        assertEquals(300, sent.path("lease_seconds").asInt());
    }

    static final class BadInput extends IllegalArgumentException implements Permanent {
        private static final long serialVersionUID = 1L;

        BadInput(String message) {
            super(message);
        }
    }

    @Test
    void failReportsTheErrorItsTypeAndPermanence() {
        fake.on("/v1/messages/m1/fail", ok(FAILED));
        LessorMQClient client = fake.client();
        Lease lease = Lease.of("m1", "r1");

        FailResult result = client.fail(lease, new IllegalStateException("upstream slow"));
        client.fail(lease, new RuntimeException(new BadInput("no recipient")));
        client.fail(lease, new PermanentFailure(new NumberFormatException("not a number")));
        client.fail(lease, new IllegalStateException("x".repeat(70_000)),
                FailOptions.builder().permanent(true).type("Timeout").details(Map.of("ms", 3000)).build());

        assertEquals(FailResult.RETRYING, result.outcome());
        assertEquals(Instant.parse("2030-01-01T00:00:10Z"), result.availableAt());
        assertEquals(7, result.maxAttempts());
        List<JsonNode> sent = fake.calls("/v1/messages/m1/fail");
        assertEquals("r1", sent.get(0).path("receipt").asString());
        assertEquals("upstream slow", sent.get(0).path("error").path("message").asString());
        assertEquals("java.lang.IllegalStateException", sent.get(0).path("error").path("type").asString());
        assertFalse(sent.get(0).path("permanent").asBoolean());
        assertTrue(sent.get(1).path("permanent").asBoolean(), "a permanent cause makes the failure permanent");
        assertEquals("java.lang.NumberFormatException", sent.get(2).path("error").path("type").asString());
        assertEquals("not a number", sent.get(2).path("error").path("message").asString());
        assertTrue(sent.get(2).path("permanent").asBoolean());
        assertEquals("Timeout", sent.get(3).path("error").path("type").asString());
        assertEquals(65535, sent.get(3).path("error").path("message").asString().length());
        assertEquals(3000, sent.get(3).path("error").path("details").path("ms").asInt());
        assertTrue(sent.get(3).path("permanent").asBoolean());
    }

    @Test
    void aLostLeaseIsALeaseLostException() {
        fake.on("/v1/messages/m1/ack", problem(409, "lease_lost"));

        LeaseLostException e = assertThrows(LeaseLostException.class, () -> fake.client().ack(Lease.of("m1", "r1")));

        assertEquals(409, e.status());
        assertEquals("lease_lost", e.code());
        assertEquals(1, fake.count(), "a lost lease is not retried");
    }

    @Test
    void serverAndNetworkErrorsAreRetried() {
        fake.on("/v1/messages/m1/ack", problem(503, "unavailable"), drop(), noContent());

        fake.client().ack(Lease.of("m1", "r1"));

        assertEquals(3, fake.count());
    }

    @Test
    void retriesRunOut() {
        fake.on("/v1/messages/m1/ack", problem(500, "internal"));

        ServerException e = assertThrows(ServerException.class,
                () -> fake.builder().retries(2).build().ack(Lease.of("m1", "r1")));

        assertEquals(500, e.status());
        assertEquals(3, fake.count());
    }

    @Test
    void anUnreachableServerIsATransportException() {
        String url = fake.url();
        fake.close();
        LessorMQClient client = LessorMQClient.builder(url, "lmq_key").retries(1).retryDelay(Duration.ZERO).build();

        TransportException e = assertThrows(TransportException.class, () -> client.ack(Lease.of("m1", "r1")));

        assertEquals("/v1/messages/m1/ack", e.path());
    }

    @Test
    void aRateLimitWaitsForRetryAfter() {
        fake.on("/v1/queues/mail/claims",
                answer(429, "{\"code\":\"rate_limited\"}", Map.of("Retry-After", "1")), noContent());

        long started = System.nanoTime();
        assertNull(fake.client().claim("mail"));

        assertTrue(Duration.ofNanos(System.nanoTime() - started).toMillis() >= 900);
        assertEquals(2, fake.count());
    }

    @Test
    void aRateLimitAfterTheRetriesCarriesRetryAfter() {
        fake.on("/v1/queues/mail/claims", answer(429, "{\"code\":\"rate_limited\"}", Map.of("Retry-After", "0")));

        RateLimitedException e = assertThrows(RateLimitedException.class,
                () -> fake.builder().retries(1).build().claim("mail"));

        assertEquals(0, e.retryAfterSeconds());
        assertEquals(2, fake.count());
    }

    @Test
    void validationErrorsCarryTheFields() {
        fake.on("/v1/queues/mail/claims", answer(400, """
                {"code":"validation_failed","detail":"Invalid request.",\
                "errors":[{"field":"wait_seconds","message":"must be at most 20"}]}""", Map.of()));

        ValidationException e = assertThrows(ValidationException.class,
                () -> fake.client().claim("mail", ClaimOptions.builder().waitSeconds(99).build()));

        assertEquals(List.of(new FieldError("wait_seconds", "must be at most 20")), e.errors());
        assertEquals("Invalid request.", e.getMessage());
    }

    @Test
    void problemCodesBecomeTheirExceptions() {
        fake.on("/v1/messages/m1/ack", problem(403, "tenant_suspended"))
                .on("/v1/messages/m2/ack", answer(404, "<html>not here</html>", Map.of()))
                .on("/v1/messages/m3/ack", problem(403, "quota_exceeded"));
        LessorMQClient client = fake.client();

        assertEquals("tenant_suspended",
                assertThrows(ForbiddenException.class, () -> client.ack(Lease.of("m1", "r"))).code());
        assertEquals("not_found", assertThrows(NotFoundException.class, () -> client.ack(Lease.of("m2", "r"))).code());
        ApiException other = assertThrows(ApiException.class, () -> client.ack(Lease.of("m3", "r")));
        assertEquals(ApiException.class, other.getClass());
        assertInstanceOf(LessorMQException.class, other);
    }

    @Test
    void stats() {
        fake.on("/v1/queues/mail", ok("""
                {"name":"mail","stats":{"ready":2,"delayed":1,"processing":3,"completed":4,"failed":5,\
                "oldest_ready_seconds":6,"oldest_processing_seconds":7,"workers":8},"settings":{}}"""));

        assertEquals(new Stats(2, 1, 3, 4, 5, 6, 7, 8), fake.client().stats("mail"));
        assertEquals("GET", fake.requests().get(0).method());
    }

    @Test
    void theKeyStaysOutOfTheString() {
        LessorMQClient client = new LessorMQClient("https://mq.example.com/", "lmq_secret");

        assertEquals("LessorMQClient(https://mq.example.com)", client.toString());
    }

    @Test
    void claimBatchAndAckBatch() {
        fake.on("/v1/queues/mail/claims", ok("{\"items\":[" + LEASE + "," + LEASE.replace("m1", "m2") + "]}"))
                .on("/v1/acks", ok("""
                        {"items":[{"message":"m1","outcome":"acked"},{"message":"m2","outcome":"lease_lost"}]}"""));
        LessorMQClient client = fake.client();

        List<Lease> leases = client.claimBatch("mail", 10, ClaimOptions.builder().waitSeconds(1).build());
        List<AckOutcome> outcomes = client.ackBatch(leases);

        assertEquals(2, leases.size());
        assertEquals("m2", leases.get(1).message().id());
        assertEquals(10, fake.calls("/v1/queues/mail/claims").get(0).path("max_messages").asInt());
        assertEquals(List.of(AckOutcome.ACKED, AckOutcome.LEASE_LOST), outcomes);
        JsonNode sent = fake.calls("/v1/acks").get(0).path("leases");
        assertEquals("m2", sent.path(1).path("message").asString());
        assertEquals("r1", sent.path(1).path("receipt").asString());
    }

    @Test
    void anEmptyBatchClaimIsAnEmptyList() {
        fake.on("/v1/queues/mail/claims", noContent());

        assertTrue(fake.client().claimBatch("mail", 10, ClaimOptions.NONE).isEmpty());
    }

    @Test
    void releaseSendsTheDelay() {
        fake.on("/v1/messages/m1/release", noContent());

        fake.client().release(Lease.of("m1", "r1"), 30);

        assertEquals(30, fake.calls("/v1/messages/m1/release").get(0).path("delay_seconds").asInt());
    }
}
