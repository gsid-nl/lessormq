package dev.lessormq;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Speaks the LessorMQ protocol. Safe for concurrent use; make one per server and key.
 *
 * <pre>{@code
 * LessorMQClient client = LessorMQClient.builder("https://mq.example.com", key).retries(5).build();
 * }</pre>
 *
 * <p>Every method throws a {@link LessorMQException} (unchecked) when it fails: an
 * {@link ApiException} subclass for an error answer, a {@link TransportException} when
 * the server could not be reached. Network errors, 5xx and 429 are retried first, for
 * every call that is safe to repeat.
 */
public final class LessorMQClient {
    /** The version of this library, such as 0.4.10. */
    public static final String VERSION = Version.VALUE;

    /** Sent with every request unless {@link Builder#userAgent} says otherwise: lessormq-java/0.4.10. */
    public static final String USER_AGENT = "lessormq-java/" + VERSION;

    private static final int MAX_ERROR_MESSAGE = 65535;
    private static final int MAX_ERROR_TYPE = 255;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final String baseUrl;
    private final String apiKey;
    private final HttpClient http;
    private final int retries;
    private final Duration retryDelay;
    private final boolean autoDedupKey;
    private final Duration timeout;
    private final String userAgent;
    private final ObjectMapper mapper;
    private final System.Logger logger;

    /** A client for the server at baseUrl, acting with apiKey, with the defaults. */
    public LessorMQClient(String baseUrl, String apiKey) {
        this(builder(baseUrl, apiKey));
    }

    private LessorMQClient(Builder builder) {
        this.baseUrl = builder.baseUrl.replaceAll("/+$", "");
        this.apiKey = builder.apiKey;
        this.retries = builder.retries;
        this.retryDelay = builder.retryDelay;
        this.autoDedupKey = builder.autoDedupKey;
        this.timeout = builder.timeout;
        this.userAgent = builder.userAgent;
        this.mapper = builder.mapper != null ? builder.mapper : Json.DEFAULT;
        this.logger = builder.logger != null ? builder.logger : System.getLogger("dev.lessormq");
        HttpClient http = builder.http;
        if (http == null) {
            // HTTP/2 where TLS negotiates it; plain HTTP stays 1.1 rather than trying an
            // h2c upgrade that proxies in between may not expect.
            http = HttpClient.newBuilder()
                    .connectTimeout(timeout)
                    .version(this.baseUrl.startsWith("https:") ? HttpClient.Version.HTTP_2 : HttpClient.Version.HTTP_1_1)
                    .build();
        }
        this.http = http;
    }

    /** A builder for a client for the server at baseUrl, acting with apiKey. */
    public static Builder builder(String baseUrl, String apiKey) {
        return new Builder(baseUrl, apiKey);
    }

    /** The JSON mapper bodies are written and read with. */
    public ObjectMapper jsonMapper() {
        return mapper;
    }

    /** Where retries, and a worker's troubles, are logged. */
    public System.Logger logger() {
        return logger;
    }

    /** Puts a message on a queue. The body is anything the JSON mapper takes, or a JsonNode. */
    public Published publish(String queue, @Nullable Object body) {
        return publish(queue, body, PublishOptions.NONE);
    }

    /** Puts a message on a queue, with options. */
    public Published publish(String queue, @Nullable Object body, PublishOptions options) {
        String dedupKey = options.dedupKey();
        if ((dedupKey == null || dedupKey.isEmpty()) && autoDedupKey) {
            dedupKey = "lmq-auto-" + randomHex(12);
        }
        ObjectNode payload = mapper.createObjectNode();
        payload.set("body", tree(body, queue));
        if (!options.headers().isEmpty()) {
            ObjectNode headers = payload.putObject("headers");
            options.headers().forEach(headers::put);
        }
        if (dedupKey != null && !dedupKey.isEmpty()) {
            payload.put("dedup_key", dedupKey);
        }
        if (options.delaySeconds() > 0) {
            payload.put("delay_seconds", options.delaySeconds());
        }
        if (options.availableAt() != null) {
            payload.put("available_at", Json.format(options.availableAt()));
        }
        if (options.maxAttempts() > 0) {
            payload.put("max_attempts", options.maxAttempts());
        }
        // Without a dedup key a repeated publish could create a second message.
        JsonNode answer = send(new Call("POST", "/v1/queues/" + segment(queue) + "/messages", payload,
                dedupKey != null && !dedupKey.isEmpty(), Duration.ZERO)).body();
        return new Published(Json.text(answer, "id"), Json.text(answer, "queue"), Json.text(answer, "status"),
                Json.requiredTime(answer, "created_at"), Json.requiredTime(answer, "available_at"),
                answer.path("duplicate").asBoolean(false));
    }

    /** Leases the next message of a queue without waiting; null when none is ready. */
    public @Nullable Lease claim(String queue) {
        return claim(queue, ClaimOptions.NONE);
    }

    /**
     * Leases the next message of a queue, waiting up to {@link ClaimOptions#waitSeconds()}
     * for one. Null when nothing became available, or the queue is paused.
     *
     * <p>Interrupting the thread while a claim waits can leave a message leased on the
     * server, which then runs out and counts as an attempt; the {@link Worker} therefore
     * lets claims finish.
     */
    public @Nullable Lease claim(String queue, ClaimOptions options) {
        // A claim whose answer got lost leaves a lease that simply expires and counts as an
        // attempt, so claims are retried like the rest.
        Answer answer = send(new Call("POST", "/v1/queues/" + segment(queue) + "/claims", claimPayload(options), true,
                Duration.ofSeconds(Math.max(options.waitSeconds(), 0))));
        return answer.status() == 204 ? null : Json.lease(answer.body(), mapper);
    }

    /**
     * Leases up to maxMessages (1 to 100) ready messages of a queue at once, oldest first,
     * each with a receipt of its own. Waits up to {@link ClaimOptions#waitSeconds()} only
     * while there is none; empty when nothing became available. Report each lease as
     * usual, or ack them together with {@link #ackBatch}.
     */
    public List<Lease> claimBatch(String queue, int maxMessages, ClaimOptions options) {
        ObjectNode payload = claimPayload(options);
        payload.put("max_messages", maxMessages);
        Answer answer = send(new Call("POST", "/v1/queues/" + segment(queue) + "/claims", payload, true,
                Duration.ofSeconds(Math.max(options.waitSeconds(), 0))));
        if (answer.status() == 204) {
            return List.of();
        }
        List<Lease> leases = new ArrayList<>();
        for (JsonNode item : answer.body().path("items")) {
            leases.add(Json.lease(item, mapper));
        }
        return List.copyOf(leases);
    }

    private ObjectNode claimPayload(ClaimOptions options) {
        ObjectNode payload = mapper.createObjectNode();
        if (options.consumer() != null && !options.consumer().isEmpty()) {
            payload.put("consumer", options.consumer());
        }
        if (options.route() != null && !options.route().isEmpty()) {
            payload.put("route", options.route());
        }
        if (options.waitSeconds() > 0) {
            payload.put("wait_seconds", options.waitSeconds());
        }
        if (options.leaseSeconds() > 0) {
            payload.put("lease_seconds", options.leaseSeconds());
        }
        return payload;
    }

    /**
     * Reports success for up to 100 leases in one request. The outcomes are in the order of
     * the leases; one that cannot be acked does not hold up the others.
     */
    public List<AckOutcome> ackBatch(List<Lease> leases) {
        ObjectNode payload = mapper.createObjectNode();
        ArrayNode items = payload.putArray("leases");
        for (Lease lease : leases) {
            items.addObject().put("message", lease.message().id()).put("receipt", lease.receipt());
        }
        JsonNode answer = send(new Call("POST", "/v1/acks", payload, true, Duration.ZERO)).body().path("items");
        List<AckOutcome> outcomes = new ArrayList<>(leases.size());
        for (int i = 0; i < leases.size(); i++) {
            outcomes.add(AckOutcome.of(Json.text(answer.path(i), "outcome")));
        }
        return List.copyOf(outcomes);
    }

    /**
     * Moves the end of the lease to now plus the queue's lease.
     *
     * @throws LeaseLostException when the lease already ended
     */
    public void extend(Lease lease) {
        extend(lease, 0);
    }

    /**
     * Moves the end of the lease to now plus leaseSeconds (0: the queue's lease).
     *
     * @throws LeaseLostException when the lease already ended
     */
    public void extend(Lease lease, int leaseSeconds) {
        ObjectNode payload = receipt(lease);
        if (leaseSeconds > 0) {
            payload.put("lease_seconds", leaseSeconds);
        }
        JsonNode answer = send(new Call("POST", messagePath(lease, "extend"), payload, true, Duration.ZERO)).body();
        lease.extendedTo(Json.requiredTime(answer, "lease_until"));
    }

    /**
     * Reports success.
     *
     * @throws LeaseLostException when the lease already ended
     */
    public void ack(Lease lease) {
        send(new Call("POST", messagePath(lease, "ack"), receipt(lease), true, Duration.ZERO));
    }

    /**
     * Reports failure with the exception: its text, its class, and whether it is
     * {@link PermanentFailure#isPermanent permanent}. The server decides what follows and
     * says so in the result, which is for logging.
     *
     * @throws LeaseLostException when the lease already ended
     */
    public FailResult fail(Lease lease, Throwable error) {
        return fail(lease, error, FailOptions.NONE);
    }

    /**
     * Reports failure with the exception, with options.
     *
     * @throws LeaseLostException when the lease already ended
     */
    public FailResult fail(Lease lease, Throwable error, FailOptions options) {
        String text = error.getMessage();
        ObjectNode report = mapper.createObjectNode();
        report.put("message", text == null || text.isEmpty() ? "(no message)" : truncate(text, MAX_ERROR_MESSAGE));
        String type = options.type() != null && !options.type().isEmpty() ? options.type() : typeOf(error);
        report.put("type", truncate(type, MAX_ERROR_TYPE));
        if (options.details() != null) {
            report.set("details", tree(options.details(), lease.message().queue()));
        }
        Boolean permanent = options.permanent();
        ObjectNode payload = receipt(lease);
        payload.set("error", report);
        payload.put("permanent", permanent != null ? permanent : PermanentFailure.isPermanent(error));
        JsonNode answer = send(new Call("POST", messagePath(lease, "fail"), payload, true, Duration.ZERO)).body();
        return new FailResult(Json.text(answer, "outcome"), Json.time(answer, "available_at"),
                Json.integer(answer, "attempt"), Json.integer(answer, "max_attempts"));
    }

    /**
     * Gives the message back at once without an outcome; the attempt does not count.
     *
     * @throws LeaseLostException when the lease already ended
     */
    public void release(Lease lease) {
        release(lease, 0);
    }

    /**
     * Gives the message back without an outcome, available again after delaySeconds; the
     * attempt does not count.
     *
     * @throws LeaseLostException when the lease already ended
     */
    public void release(Lease lease, int delaySeconds) {
        ObjectNode payload = receipt(lease);
        if (delaySeconds > 0) {
            payload.put("delay_seconds", delaySeconds);
        }
        send(new Call("POST", messagePath(lease, "release"), payload, true, Duration.ZERO));
    }

    /** A queue's counts. Needs the consume or admin scope. Not part of the protocol; for monitoring. */
    public Stats stats(String queue) {
        JsonNode stats = send(new Call("GET", "/v1/queues/" + segment(queue), null, true, Duration.ZERO)).body()
                .path("stats");
        return new Stats(Json.integer(stats, "ready"), Json.integer(stats, "delayed"),
                Json.integer(stats, "processing"), Json.integer(stats, "completed"), Json.integer(stats, "failed"),
                Json.integer(stats, "oldest_ready_seconds"), Json.integer(stats, "oldest_processing_seconds"),
                Json.integer(stats, "workers"));
    }

    /** Never shows the key. */
    @Override
    public String toString() {
        return "LessorMQClient(" + baseUrl + ")";
    }

    private ObjectNode receipt(Lease lease) {
        return mapper.createObjectNode().put("receipt", lease.receipt());
    }

    private JsonNode tree(@Nullable Object value, String queue) {
        if (value == null) {
            return mapper.nullNode();
        }
        if (value instanceof JsonNode node) {
            return node;
        }
        try {
            return mapper.valueToTree(value);
        } catch (JacksonException e) {
            throw new LessorMQException("Could not write a " + value.getClass().getName() + " as JSON for queue "
                    + queue + ": " + e.getOriginalMessage(), e);
        }
    }

    /** One request. Repeatable: it may be sent again after a failure that leaves its effect unknown. */
    private record Call(String method, String path, @Nullable JsonNode payload, boolean repeatable, Duration extra) {
    }

    private record Answer(int status, JsonNode body) {
    }

    private Answer send(Call call) {
        byte[] body = call.payload() == null ? null : mapper.writeValueAsBytes(call.payload());
        for (int attempt = 1; ; attempt++) {
            HttpResponse<byte[]> response;
            try {
                response = http.send(request(call, body), HttpResponse.BodyHandlers.ofByteArray());
            } catch (IOException e) {
                if (call.repeatable() && attempt <= retries) {
                    pause(call, attempt, null, e.toString());
                    continue;
                }
                throw new TransportException(call.method(), call.path(), e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TransportException(call.method(), call.path(), e);
            }

            int status = response.statusCode();
            if (status < 400) {
                return new Answer(status, status == 204 ? mapper.missingNode() : parse(response.body(), call));
            }

            ApiException error = problem(status, response.body(),
                    response.headers().firstValue("Retry-After").orElse(""));
            // A 429 was not processed at all, so it is always safe to send again.
            boolean retry = error.code().equals("rate_limited") || (call.repeatable() && status >= 500);
            if (retry && attempt <= retries) {
                pause(call, attempt, error.retryAfterSeconds(), error.getMessage());
                continue;
            }
            throw error;
        }
    }

    private HttpRequest request(Call call, byte @Nullable [] body) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + call.path()))
                .timeout(timeout.plus(call.extra()))
                .header("Authorization", "Bearer " + apiKey)
                .header("Accept", "application/json")
                .header("User-Agent", userAgent);
        if (body == null) {
            return request.method(call.method(), HttpRequest.BodyPublishers.noBody()).build();
        }
        return request.header("Content-Type", "application/json")
                .method(call.method(), HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
    }

    private JsonNode parse(byte[] raw, Call call) {
        if (raw.length == 0) {
            return mapper.missingNode();
        }
        try {
            return mapper.readTree(raw);
        } catch (JacksonException e) {
            throw new LessorMQException("Could not read the answer to " + call.method() + " " + call.path() + ": "
                    + e.getOriginalMessage(), e);
        }
    }

    private ApiException problem(int status, byte[] raw, String retryAfter) {
        JsonNode body = mapper.missingNode();
        try {
            body = mapper.readTree(raw);
        } catch (JacksonException e) {
            // A proxy's error page, say, does not decode: the status says enough.
        }
        String code = Json.text(body, "code");
        List<FieldError> errors = new ArrayList<>();
        for (JsonNode error : body.path("errors")) {
            errors.add(new FieldError(Json.text(error, "field"), Json.text(error, "message")));
        }
        Integer seconds = null;
        try {
            int value = Integer.parseInt(retryAfter.trim());
            if (value >= 0) {
                seconds = value;
            }
        } catch (NumberFormatException e) {
            // Absent, or an HTTP date: wait the usual delay instead.
        }
        return ApiException.of(status, code.isEmpty() ? ApiException.codeFor(status) : code,
                Json.text(body, "detail"), errors, seconds);
    }

    private void pause(Call call, int attempt, @Nullable Integer retryAfterSeconds, String reason) {
        Duration delay = retryAfterSeconds != null
                ? Duration.ofSeconds(retryAfterSeconds)
                : retryDelay.multipliedBy(attempt);
        logger.log(System.Logger.Level.WARNING, () -> "LessorMQ request failed, retrying: " + call.path()
                + ", attempt " + attempt + ", in " + delay.toMillis() + " ms: " + reason);
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransportException(call.method(), call.path(), e);
        }
    }

    private static String messagePath(Lease lease, String action) {
        return "/v1/messages/" + segment(lease.message().id()) + "/" + action;
    }

    /** A path segment, percent-encoded but for the unreserved characters. */
    static String segment(String value) {
        StringBuilder out = new StringBuilder();
        for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
            char c = (char) (b & 0xff);
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.'
                    || c == '_' || c == '~') {
                out.append(c);
            } else {
                out.append('%').append(HexFormat.of().withUpperCase().toHexDigits(b));
            }
        }
        return out.toString();
    }

    private static String randomHex(int bytes) {
        byte[] random = new byte[bytes];
        RANDOM.nextBytes(random);
        return HexFormat.of().formatHex(random);
    }

    private static String truncate(String text, int codePoints) {
        if (text.codePointCount(0, text.length()) <= codePoints) {
            return text;
        }
        return text.substring(0, text.offsetByCodePoints(0, codePoints));
    }

    /** The class that names an error in the report, looking through a wrapping PermanentFailure. */
    private static String typeOf(Throwable error) {
        if (error.getClass() == PermanentFailure.class && error.getCause() != null) {
            return error.getCause().getClass().getName();
        }
        return error.getClass().getName();
    }

    /** Builds a {@link LessorMQClient}. */
    public static final class Builder {
        private final String baseUrl;
        private final String apiKey;
        private @Nullable HttpClient http;
        private int retries = 3;
        private Duration retryDelay = Duration.ofMillis(100);
        private boolean autoDedupKey = true;
        private Duration timeout = Duration.ofSeconds(10);
        private String userAgent = USER_AGENT;
        private @Nullable ObjectMapper mapper;
        private System.@Nullable Logger logger;

        private Builder(String baseUrl, String apiKey) {
            this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl");
            this.apiKey = Objects.requireNonNull(apiKey, "apiKey");
        }

        /** Your own HttpClient (proxies, TLS, an executor); the client sets a timeout per request. */
        public Builder httpClient(HttpClient http) {
            this.http = http;
            return this;
        }

        /**
         * Retries after a network error, a 5xx or a 429, for calls that are safe to repeat
         * (all but a publish without a dedup key). Default 3.
         */
        public Builder retries(int retries) {
            this.retries = retries;
            return this;
        }

        /** The base delay between retries, times the attempt number. Default 100 ms. */
        public Builder retryDelay(Duration retryDelay) {
            this.retryDelay = retryDelay;
            return this;
        }

        /**
         * Whether a publish without a dedup key gets one of its own (default true). Without
         * one a publish is never retried, since a repeat could create a second message.
         */
        public Builder autoDedupKey(boolean autoDedupKey) {
            this.autoDedupKey = autoDedupKey;
            return this;
        }

        /** The deadline per request; a claim gets its wait on top. Default 10 s. */
        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder userAgent(String userAgent) {
            this.userAgent = userAgent;
            return this;
        }

        /**
         * The JSON mapper for bodies, such as your application's. Default: a JsonMapper with
         * the Jackson modules found on the classpath (the Kotlin module, say).
         */
        public Builder jsonMapper(ObjectMapper mapper) {
            this.mapper = mapper;
            return this;
        }

        /** Where retries and a worker's troubles go. Default: System.getLogger("dev.lessormq"). */
        public Builder logger(System.Logger logger) {
            this.logger = logger;
            return this;
        }

        public LessorMQClient build() {
            return new LessorMQClient(this);
        }
    }
}
