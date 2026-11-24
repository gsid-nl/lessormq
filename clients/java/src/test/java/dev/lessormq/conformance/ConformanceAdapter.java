package dev.lessormq.conformance;

import dev.lessormq.AckOutcome;
import dev.lessormq.ApiException;
import dev.lessormq.ClaimOptions;
import dev.lessormq.FailOptions;
import dev.lessormq.FailResult;
import dev.lessormq.FieldError;
import dev.lessormq.Lease;
import dev.lessormq.LessorMQClient;
import dev.lessormq.Message;
import dev.lessormq.PublishOptions;
import dev.lessormq.Published;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Runs the LessorMQ conformance scenarios through this library
 * (conformance/php/src/ProcessAdapter.php in the server repository).
 *
 * <p>One JSON request per line on stdin, one JSON result per line on stdout. Every call goes
 * through {@link LessorMQClient}, and its results and exceptions are turned back into the
 * {status, body} the scenarios check:
 *
 * <pre>
 * (cd clients/java &amp;&amp; mvn test-compile)
 * LESSORMQ_ADAPTER_CMD="java -cp 'clients/java/target/classes:clients/java/target/test-classes:clients/java/target/dependency/*' dev.lessormq.conformance.ConformanceAdapter" conformance/run.sh
 * </pre>
 */
public final class ConformanceAdapter {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String URL = System.getenv().getOrDefault("LESSORMQ_URL", "http://127.0.0.1:18080");
    private static final Map<String, LessorMQClient> CLIENTS = new HashMap<>();

    private ConformanceAdapter() {
    }

    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        PrintStream out = new PrintStream(System.out, false, StandardCharsets.UTF_8);
        for (String line = in.readLine(); line != null; line = in.readLine()) {
            if (line.isBlank()) {
                continue;
            }
            JsonNode request = JSON.readTree(line);
            ObjectNode result;
            try {
                result = call(request.path("call").asString(), request.path("api_key").asString(),
                        request.path("args"));
            } catch (ApiException e) {
                result = result(e.status(), problem(e));
            } catch (RuntimeException e) {
                System.err.println(request.path("call").asString() + ": " + e);
                System.exit(1);
                return;
            }
            out.println(JSON.writeValueAsString(result));
            out.flush();
        }
    }

    private static ObjectNode call(String name, String apiKey, JsonNode args) {
        LessorMQClient client = CLIENTS.computeIfAbsent(apiKey,
                key -> LessorMQClient.builder(URL, key).retryDelay(Duration.ofMillis(50)).build());
        String queue = args.path("queue").asString("");
        switch (name) {
            case "publish": {
                PublishOptions.Builder options = PublishOptions.builder()
                        .headers(strings(args.path("headers")))
                        .delaySeconds(args.path("delay_seconds").asInt(0))
                        .maxAttempts(args.path("max_attempts").asInt(0));
                if (args.hasNonNull("dedup_key")) {
                    options.dedupKey(args.path("dedup_key").asString());
                }
                if (args.hasNonNull("available_at")) {
                    options.availableAt(OffsetDateTime.parse(args.path("available_at").asString()).toInstant());
                }
                JsonNode body = args.has("body") ? args.path("body") : JSON.nullNode();
                Published published = client.publish(queue, body, options.build());
                ObjectNode out = JSON.createObjectNode()
                        .put("id", published.id())
                        .put("queue", published.queue())
                        .put("status", published.status())
                        .put("created_at", format(published.createdAt()))
                        .put("available_at", format(published.availableAt()))
                        .put("duplicate", published.duplicate());
                return result(published.duplicate() ? 200 : 201, out);
            }
            case "claim": {
                Lease lease = client.claim(queue, claimOptions(args));
                return lease == null ? result(204, JSON.nullNode()) : result(200, leaseBody(lease));
            }
            case "claim_batch": {
                List<Lease> leases = client.claimBatch(queue, args.path("max_messages").asInt(0), claimOptions(args));
                if (leases.isEmpty()) {
                    return result(204, JSON.nullNode());
                }
                ObjectNode out = JSON.createObjectNode();
                ArrayNode items = out.putArray("items");
                leases.forEach(lease -> items.add(leaseBody(lease)));
                return result(200, out);
            }
            case "ack_batch": {
                List<Lease> leases = new ArrayList<>();
                for (JsonNode item : args.path("leases")) {
                    leases.add(leaseOf(item));
                }
                List<AckOutcome> outcomes = client.ackBatch(leases);
                ObjectNode out = JSON.createObjectNode();
                ArrayNode items = out.putArray("items");
                for (int i = 0; i < leases.size(); i++) {
                    items.addObject().put("message", leases.get(i).message().id())
                            .put("outcome", outcomes.get(i).value());
                }
                return result(200, out);
            }
            case "extend": {
                Lease lease = leaseOf(args);
                client.extend(lease, args.path("lease_seconds").asInt(0));
                return result(200, JSON.createObjectNode().put("lease_until", format(lease.until())));
            }
            case "ack":
                client.ack(leaseOf(args));
                return result(204, JSON.nullNode());
            case "release":
                client.release(leaseOf(args), args.path("delay_seconds").asInt(0));
                return result(204, JSON.nullNode());
            case "fail": {
                JsonNode error = args.path("error");
                FailOptions.Builder options = FailOptions.builder().permanent(args.path("permanent").asBoolean(false));
                if (error.hasNonNull("type")) {
                    options.type(error.path("type").asString());
                }
                if (error.has("details")) {
                    options.details(error.path("details"));
                }
                FailResult result = client.fail(leaseOf(args), new RuntimeException(error.path("message").asString("")),
                        options.build());
                ObjectNode out = JSON.createObjectNode().put("outcome", result.outcome());
                if (result.availableAt() != null) {
                    out.put("available_at", format(result.availableAt()));
                } else {
                    out.putNull("available_at");
                }
                out.put("attempt", result.attempt()).put("max_attempts", result.maxAttempts());
                return result(200, out);
            }
            default:
                throw new IllegalArgumentException("unknown call " + name);
        }
    }

    private static ClaimOptions claimOptions(JsonNode args) {
        return ClaimOptions.builder()
                .consumer(args.hasNonNull("consumer") ? args.path("consumer").asString() : null)
                .route(args.hasNonNull("route") ? args.path("route").asString() : null)
                .waitSeconds(args.path("wait_seconds").asInt(0))
                .leaseSeconds(args.path("lease_seconds").asInt(0))
                .build();
    }

    /** A lease as the scenario names it: the message id and the receipt, which may be stale or someone else's. */
    private static Lease leaseOf(JsonNode args) {
        return Lease.of(args.path("message").asString(""), args.path("receipt").asString(""));
    }

    private static ObjectNode leaseBody(Lease lease) {
        Message message = lease.message();
        ObjectNode out = JSON.createObjectNode()
                .put("receipt", lease.receipt())
                .put("lease_until", format(lease.until()));
        ObjectNode m = out.putObject("message")
                .put("id", message.id())
                .put("queue", message.queue());
        m.set("body", message.body());
        m.set("headers", JSON.valueToTree(message.headers()));
        m.put("attempt", message.attempt())
                .put("max_attempts", message.maxAttempts())
                .put("created_at", format(message.createdAt()));
        return out;
    }

    private static ObjectNode problem(ApiException e) {
        ObjectNode body = JSON.createObjectNode().put("code", e.code()).put("detail", e.detail());
        ArrayNode errors = body.putArray("errors");
        for (FieldError error : e.errors()) {
            errors.addObject().put("field", error.field()).put("message", error.message());
        }
        return body;
    }

    private static Map<String, String> strings(JsonNode node) {
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> entry : node.properties()) {
            out.put(entry.getKey(), entry.getValue().asString());
        }
        return out;
    }

    private static ObjectNode result(int status, JsonNode body) {
        ObjectNode result = JSON.createObjectNode().put("status", status);
        result.set("body", body);
        return result;
    }

    private static String format(Instant time) {
        return DateTimeFormatter.ISO_INSTANT.format(time);
    }
}
