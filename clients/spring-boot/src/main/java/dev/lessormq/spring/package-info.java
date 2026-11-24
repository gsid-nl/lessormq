/**
 * Spring Boot auto-configuration for LessorMQ: a {@link dev.lessormq.LessorMQClient} bean
 * and {@link dev.lessormq.spring.LessorMQListener} methods run by managed workers. Retries,
 * backoff and the dead-letter set are the queue's; a handler only succeeds or fails.
 */
@NullMarked
package dev.lessormq.spring;

import org.jspecify.annotations.NullMarked;
