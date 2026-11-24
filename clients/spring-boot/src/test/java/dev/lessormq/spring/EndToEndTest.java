package dev.lessormq.spring;

import static org.assertj.core.api.Assertions.assertThat;

import dev.lessormq.LessorMQClient;
import dev.lessormq.Message;
import dev.lessormq.PermanentFailure;
import dev.lessormq.Published;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Against a real server: LESSORMQ_URL, and LESSORMQ_OPERATOR_KEY to make a tenant of its own.
 * A job dispatched through the starter is run by its worker and acked; a failing one comes
 * back after the queue's backoff with the server's attempt count; a permanent failure goes
 * to the dead-letter set.
 */
@EnabledIfEnvironmentVariable(named = "LESSORMQ_OPERATOR_KEY", matches = ".+")
class EndToEndTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final String URL = System.getenv().getOrDefault("LESSORMQ_URL", "http://127.0.0.1:18080");
    private static final String OPERATOR_KEY = System.getenv().getOrDefault("LESSORMQ_OPERATOR_KEY", "");

    private static String tenant = "";
    private static String key = "";

    record Order(int number) {
    }

    /** Every handler call: queue, message id, attempt, when. */
    record Call(String queue, String id, int attempt, Instant at) {
    }

    static final class Calls {
        final List<Call> all = new CopyOnWriteArrayList<>();

        void add(String queue, Message message) {
            all.add(new Call(queue, message.id(), message.attempt(), Instant.now()));
        }

        List<Call> of(String id) {
            return all.stream().filter(call -> call.id().equals(id)).toList();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Listeners {
        @Bean
        Calls calls() {
            return new Calls();
        }

        @Bean
        Shop shop(Calls calls) {
            return new Shop(calls);
        }
    }

    static class Shop {
        private final Calls calls;

        Shop(Calls calls) {
            this.calls = calls;
        }

        @LessorMQListener(queue = "orders")
        void ship(Order order, Message message) {
            calls.add("orders", message);
        }

        @LessorMQListener(queue = "flaky")
        void flaky(Order order, Message message) {
            calls.add("flaky", message);
            if (message.attempt() == 1) {
                throw new IllegalStateException("warehouse offline for order " + order.number());
            }
        }

        @LessorMQListener(queue = "broken")
        void broken(Order order, Message message) {
            calls.add("broken", message);
            throw new PermanentFailure("order " + order.number() + " has no lines");
        }
    }

    @BeforeAll
    static void createTenant() throws Exception {
        byte[] random = new byte[4];
        new SecureRandom().nextBytes(random);
        tenant = "java-spring-" + HexFormat.of().formatHex(random);
        JsonNode created = call("POST", "/v1/tenants", OPERATOR_KEY,
                Map.of("id", tenant, "name", "Spring Boot starter end-to-end"));
        key = created.path("admin_key").path("secret").asString();
        assertThat(key).isNotEmpty();
        call("PATCH", "/v1/queues/flaky", key,
                Map.of("max_attempts", 3, "backoff", Map.of("initial_seconds", 2, "multiplier", 1, "jitter", 0)));
    }

    @AfterAll
    static void deleteTenant() throws Exception {
        if (!tenant.isEmpty()) {
            call("PATCH", "/v1/tenants/" + tenant, OPERATOR_KEY, Map.of("status", "suspended"));
            call("DELETE", "/v1/tenants/" + tenant, OPERATOR_KEY, null);
        }
    }

    @Test
    void jobsRunAreAckedRetriedAfterTheBackoffAndDeadLettered() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(LessorMQAutoConfiguration.class))
                .withUserConfiguration(Listeners.class)
                .withPropertyValues("lessormq.url=" + URL, "lessormq.key=" + key, "lessormq.worker.wait-seconds=1")
                .run(context -> {
                    LessorMQClient client = context.getBean(LessorMQClient.class);
                    Calls calls = context.getBean(Calls.class);
                    Published shipped = client.publish("orders", new Order(1));
                    Published flaky = client.publish("flaky", new Order(2));
                    Published broken = client.publish("broken", new Order(3));

                    waitFor(() -> message(shipped.id()).path("status").asString().equals("completed"));
                    waitFor(() -> message(flaky.id()).path("status").asString().equals("completed"));
                    waitFor(() -> message(broken.id()).path("status").asString().equals("failed"));

                    // Run by the worker and acked.
                    assertThat(calls.of(shipped.id())).hasSize(1);
                    assertThat(message(shipped.id()).path("attempts").asInt()).isEqualTo(1);

                    // Failed once, back after the queue's backoff (2 s), as attempt 2 of 3.
                    List<Call> retried = calls.of(flaky.id());
                    assertThat(retried).extracting(Call::attempt).containsExactly(1, 2);
                    Duration backoff = Duration.between(retried.get(0).at(), retried.get(1).at());
                    assertThat(backoff).isBetween(Duration.ofMillis(1800), Duration.ofSeconds(6));
                    JsonNode done = message(flaky.id());
                    assertThat(done.path("attempts").asInt()).isEqualTo(2);
                    assertThat(done.path("max_attempts").asInt()).isEqualTo(3);
                    assertThat(done.path("last_error").path("type").asString())
                            .isEqualTo("java.lang.IllegalStateException");

                    // Permanent: in the dead-letter set after one attempt.
                    JsonNode dead = message(broken.id());
                    assertThat(calls.of(broken.id())).hasSize(1);
                    assertThat(dead.path("attempts").asInt()).isEqualTo(1);
                    assertThat(dead.path("last_error").path("type").asString())
                            .isEqualTo("dev.lessormq.PermanentFailure");
                    assertThat(dead.path("last_error").path("message").asString()).isEqualTo("order 3 has no lines");
                });
    }

    private static JsonNode message(String id) {
        try {
            return call("GET", "/v1/messages/" + id, key, null);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime()).as("in time").isLessThan(deadline);
            Thread.sleep(100);
        }
    }

    private static JsonNode call(String method, String path, String apiKey, @Nullable Object body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(URL + path))
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json");
        request.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)));
        HttpResponse<String> response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(method + " " + path + ": " + response.body()).isLessThan(300);
        return response.body().isEmpty() ? JSON.missingNode() : JSON.readTree(response.body());
    }
}
