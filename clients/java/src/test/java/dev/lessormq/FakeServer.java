package dev.lessormq;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** A scripted LessorMQ server: answers queued per path (the last one repeats), every request recorded. */
final class FakeServer implements AutoCloseable {
    static final String LEASE = """
            {"receipt":"r1","lease_until":"2030-01-01T00:00:30Z","message":{"id":"m1","queue":"mail",\
            "body":{"to":"ada@example.com"},"headers":{"type":"Welcome"},"attempt":1,"max_attempts":7,\
            "created_at":"2030-01-01T00:00:00Z"}}""";
    static final String FAILED = """
            {"outcome":"retrying","available_at":"2030-01-01T00:00:10Z","attempt":1,"max_attempts":7}""";
    static final String PUBLISHED = """
            {"id":"m1","queue":"mail","status":"pending","created_at":"2030-01-01T00:00:00Z",\
            "available_at":"2030-01-01T00:00:00Z","duplicate":false}""";

    private static final JsonMapper JSON = JsonMapper.builder().build();

    /** One scripted response; drop closes the connection instead. */
    record Answer(int status, String body, Map<String, String> headers, boolean drop) {
    }

    /** One recorded request. */
    record Request(String method, String path, Map<String, List<String>> headers, JsonNode body) {
    }

    private final HttpServer server;
    private final Map<String, Deque<Answer>> answers = new HashMap<>();
    private final List<Request> requests = new ArrayList<>();
    private volatile Consumer<String> onRequest = path -> { };

    FakeServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/", this::serve);
        server.start();
    }

    static Answer ok(String body) {
        return new Answer(200, body, Map.of(), false);
    }

    static Answer noContent() {
        return new Answer(204, "", Map.of(), false);
    }

    static Answer problem(int status, String code) {
        return new Answer(status, "{\"code\":\"" + code + "\",\"detail\":\"" + code + "\"}", Map.of(), false);
    }

    static Answer answer(int status, String body, Map<String, String> headers) {
        return new Answer(status, body, headers, false);
    }

    static Answer drop() {
        return new Answer(0, "", Map.of(), true);
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    LessorMQClient client() {
        return builder().build();
    }

    LessorMQClient.Builder builder() {
        return LessorMQClient.builder(url() + "/", "lmq_key").retryDelay(Duration.ZERO);
    }

    synchronized FakeServer on(String path, Answer... more) {
        answers.computeIfAbsent(path, p -> new ArrayDeque<>()).addAll(List.of(more));
        return this;
    }

    FakeServer onRequest(Consumer<String> hook) {
        this.onRequest = hook;
        return this;
    }

    synchronized List<JsonNode> calls(String path) {
        List<JsonNode> out = new ArrayList<>();
        for (Request request : requests) {
            if (request.path().equals(path)) {
                out.add(request.body());
            }
        }
        return out;
    }

    synchronized List<Request> requests() {
        return List.copyOf(requests);
    }

    synchronized int count() {
        return requests.size();
    }

    private void serve(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getRawPath();
        byte[] raw = exchange.getRequestBody().readAllBytes();
        JsonNode body = raw.length == 0 ? JSON.missingNode() : JSON.readTree(raw);
        Answer answer;
        synchronized (this) {
            requests.add(new Request(exchange.getRequestMethod(), path, Map.copyOf(exchange.getRequestHeaders()),
                    body));
            Deque<Answer> queued = answers.get(path);
            if (queued == null || queued.isEmpty()) {
                answer = new Answer(500, "{\"code\":\"internal\",\"detail\":\"no answer for " + path + "\"}",
                        Map.of(), false);
            } else {
                answer = queued.size() > 1 ? queued.poll() : queued.peek();
            }
        }
        onRequest.accept(path);
        if (answer.drop()) {
            exchange.close();
            return;
        }
        byte[] out = answer.body().getBytes(StandardCharsets.UTF_8);
        answer.headers().forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
        if (!answer.body().isEmpty()) {
            exchange.getResponseHeaders().add("Content-Type", answer.status() >= 400
                    ? "application/problem+json" : "application/json");
        }
        exchange.sendResponseHeaders(answer.status(), answer.status() == 204 ? -1 : out.length);
        if (answer.status() != 204) {
            exchange.getResponseBody().write(out);
        }
        exchange.close();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
