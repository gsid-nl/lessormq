package dev.lessormq.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Runs a bean method on every message of a queue, in a worker that starts and stops with
 * the application context.
 *
 * <pre>{@code
 * @Component
 * class Mailer {
 *     @LessorMQListener(queue = "mail")
 *     void send(Mail mail) { ... }
 * }
 * }</pre>
 *
 * <p>Parameters: one for the body, decoded with the application's JSON mapper into the
 * parameter's type (a body that does not decode is a permanent failure), and any of
 * {@link dev.lessormq.Message}, {@link dev.lessormq.Job} and {@link dev.lessormq.Lease}.
 *
 * <p>Returning acks the message. Throwing fails it: as permanent for a
 * {@link dev.lessormq.PermanentFailure} or an exception implementing
 * {@link dev.lessormq.Permanent} (also as a cause), as retryable for anything else. The
 * queue decides what follows, so the method must not be {@code @Retryable} or
 * {@code @Async}; the application refuses to start when it is.
 *
 * <p>String attributes take placeholders: {@code queue = "${app.queues.mail}"}.
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface LessorMQListener {
    /** The queue. */
    String queue();

    /** Messages handled at the same time; lessormq.worker.concurrency when empty. */
    String concurrency() default "";

    /** The path this worker runs jobs through; lessormq.worker.route when empty. */
    String route() default "";

    /** Shown to administrators; lessormq.worker.consumer (or host:pid) when empty. */
    String consumer() default "";

    /** The lease per claim and per extend; lessormq.worker.lease-seconds (or the queue's) when empty. */
    String leaseSeconds() default "";
}
