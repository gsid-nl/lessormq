package dev.lessormq.spring;

import dev.lessormq.LessorMQClient;

/**
 * Changes the client's builder before the bean is built, after the lessormq.* properties:
 * your own HttpClient (proxies, TLS), a logger, a user agent.
 */
@FunctionalInterface
public interface LessorMQClientCustomizer {
    void customize(LessorMQClient.Builder builder);
}
