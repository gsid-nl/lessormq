package dev.lessormq.spring;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
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

    private static final JsonMapper JSON = JsonMapper.builder().build();

    record Answer(int status, String body) {
    }

    record Request(String path, Map<String, List<String>> headers, JsonNode body) {
    }

    private final HttpServer server;
    private final Map<String, Deque<Answer>> answers = new HashMap<>();
    private final List<Request> requests = new ArrayList<>();

    FakeServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/", this::serve);
        server.start();
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    synchronized FakeServer on(String path, int status, String body) {
        answers.computeIfAbsent(path, p -> new ArrayDeque<>()).add(new Answer(status, body));
        return this;
    }

    /** Claims of the queue hand out one message, then nothing. */
    FakeServer once(String queue, String lease) {
        return on("/v1/queues/" + queue + "/claims", 200, lease).on("/v1/queues/" + queue + "/claims", 204, "");
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

    /** Waits up to 5 seconds for a request to path. */
    void await(String path) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (calls(path).isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
    }

    synchronized List<Request> requests() {
        return List.copyOf(requests);
    }

    private void serve(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getRawPath();
        byte[] raw = exchange.getRequestBody().readAllBytes();
        Answer answer;
        synchronized (this) {
            requests.add(new Request(path, Map.copyOf(exchange.getRequestHeaders()),
                    raw.length == 0 ? JSON.missingNode() : JSON.readTree(raw)));
            Deque<Answer> queued = answers.get(path);
            answer = queued == null || queued.isEmpty() ? new Answer(204, "")
                    : queued.size() > 1 ? queued.poll() : queued.peek();
        }
        if (answer.status() == 204 && path.endsWith("/claims")) {
            // An empty claim waits a little, as a long poll would, so idle workers do not spin.
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        byte[] out = answer.body().getBytes(StandardCharsets.UTF_8);
        if (!answer.body().isEmpty()) {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
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
