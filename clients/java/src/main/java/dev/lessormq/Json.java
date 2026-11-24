package dev.lessormq;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** Reading the server's answers. They are read field by field, ignoring what is unknown. */
final class Json {
    /** The mapper of a client that was given none: with the modules found on the classpath. */
    static final ObjectMapper DEFAULT = JsonMapper.builder().findAndAddModules().build();

    private Json() {
    }

    static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isValueNode() && !value.isNull() ? value.asString() : "";
    }

    static int integer(JsonNode node, String field) {
        return node.path(field).asInt(0);
    }

    static @Nullable Instant time(JsonNode node, String field) {
        String value = text(node, field);
        return value.isEmpty() ? null : OffsetDateTime.parse(value).toInstant();
    }

    static Instant requiredTime(JsonNode node, String field) {
        Instant value = time(node, field);
        return value != null ? value : Instant.EPOCH;
    }

    static Map<String, String> strings(JsonNode node) {
        Map<String, String> out = new LinkedHashMap<>();
        if (node.isObject()) {
            for (Map.Entry<String, JsonNode> entry : node.properties()) {
                out.put(entry.getKey(), entry.getValue().asString());
            }
        }
        return out;
    }

    /** RFC 3339 in UTC, to the second: the server takes no fractions. */
    static String format(Instant time) {
        return DateTimeFormatter.ISO_INSTANT.format(time.truncatedTo(ChronoUnit.SECONDS));
    }

    static Message message(JsonNode node, ObjectMapper mapper) {
        return new Message(text(node, "id"), text(node, "queue"), node.path("body").isMissingNode()
                ? mapper.nullNode() : node.path("body"), strings(node.path("headers")), integer(node, "attempt"),
                integer(node, "max_attempts"), requiredTime(node, "created_at"), mapper);
    }

    static Lease lease(JsonNode node, ObjectMapper mapper) {
        return new Lease(text(node, "receipt"), requiredTime(node, "lease_until"), message(node.path("message"),
                mapper));
    }
}
