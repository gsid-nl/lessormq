package dev.lessormq;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Map;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** A delivered message: what a handler runs. */
public final class Message {
    private final String id;
    private final String queue;
    private final JsonNode body;
    private final Map<String, String> headers;
    private final int attempt;
    private final int maxAttempts;
    private final Instant createdAt;
    private final ObjectMapper mapper;

    /** A message from its parts, for adapters and tests; claims return real ones. */
    public Message(String id, String queue, JsonNode body, Map<String, String> headers, int attempt,
            int maxAttempts, Instant createdAt) {
        this(id, queue, body, headers, attempt, maxAttempts, createdAt, Json.DEFAULT);
    }

    Message(String id, String queue, JsonNode body, Map<String, String> headers, int attempt, int maxAttempts,
            Instant createdAt, ObjectMapper mapper) {
        this.id = id;
        this.queue = queue;
        this.body = body;
        this.headers = Map.copyOf(headers);
        this.attempt = attempt;
        this.maxAttempts = maxAttempts;
        this.createdAt = createdAt;
        this.mapper = mapper;
    }

    public String id() {
        return id;
    }

    public String queue() {
        return queue;
    }

    /** The JSON value as published; {@link #decode(Class)} turns it into your type. */
    public JsonNode body() {
        return body;
    }

    public Map<String, String> headers() {
        return headers;
    }

    /** Which attempt this is, counting this one. */
    public int attempt() {
        return attempt;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public Instant createdAt() {
        return createdAt;
    }

    /** Whether this attempt is the last one the queue allows. */
    public boolean isLastAttempt() {
        return attempt >= maxAttempts;
    }

    /**
     * The body as a {@code type}, with the client's JSON mapper.
     *
     * @throws PermanentFailure when the body is null or does not fit the type: another
     *     attempt would only fail again, so a handler that lets it pass reports the message
     *     as permanent
     */
    public <T> T decode(Class<T> type) {
        return decode(mapper.constructType(type));
    }

    /** {@link #decode(Class)} for generic types: {@code decode(new TypeReference<List<Mail>>() {})}. */
    public <T> T decode(TypeReference<T> type) {
        return decode(mapper.getTypeFactory().constructType(type));
    }

    /** {@link #decode(Class)} for a reflected type, such as a method's generic parameter. */
    public <T> T decode(Type type) {
        return decode(mapper.constructType(type));
    }

    private <T> T decode(JavaType type) {
        if (body.isNull() || body.isMissingNode()) {
            throw new PermanentFailure("The body of message " + id + " is null, not a " + type.toCanonical());
        }
        try {
            T value = mapper.readerFor(type).readValue(body);
            if (value == null) {
                throw new PermanentFailure("The body of message " + id + " reads as null, not a "
                        + type.toCanonical());
            }
            return value;
        } catch (JacksonException e) {
            throw new PermanentFailure("The body of message " + id + " is not a " + type.toCanonical() + ": "
                    + e.getOriginalMessage(), e);
        }
    }

    @Override
    public String toString() {
        return "Message(" + id + ", queue " + queue + ", attempt " + attempt + " of " + maxAttempts + ")";
    }
}
