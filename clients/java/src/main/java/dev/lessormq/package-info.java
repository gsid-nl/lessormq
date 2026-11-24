/**
 * Java client and worker for LessorMQ, the message queue over HTTP where the server decides
 * and clients only claim, run, ack or fail.
 *
 * <pre>{@code
 * LessorMQClient client = new LessorMQClient("https://mq.example.com", System.getenv("LESSORMQ_KEY"));
 * client.publish("mail", Map.of("to", "ada@example.com"));
 *
 * Worker worker = new Worker(client, "mail", job -> send(job.message().decode(Mail.class)));
 * worker.run();
 * }</pre>
 *
 * <p>The client never decides what happens to a message: no retry of a failed job, no
 * delay, no dead-lettering. It reports, and the queue decides. It does retry its own HTTP
 * calls when that is safe: network errors, 5xx and 429, for every call that can be
 * repeated (all of them, given the automatic dedup key on publish).
 *
 * <p>Everything here is non-null unless marked {@code @Nullable}, and every exception the
 * library throws is unchecked: a {@link dev.lessormq.LessorMQException}.
 */
@NullMarked
package dev.lessormq;

import org.jspecify.annotations.NullMarked;
