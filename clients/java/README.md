# LessorMQ for Java

Client and worker for [LessorMQ](https://lessormq.dev), the message queue over HTTP where
the server decides and clients only claim, run, ack or fail. Java 17 or newer, and Kotlin
just as well: every type is annotated for nullness (JSpecify) and every exception is
unchecked. It speaks HTTP with the JDK's `java.net.http` and JSON with Jackson 3; nothing
else.

The package is `dev.lessormq:lessormq` on Maven Central. For Spring Boot there is a
starter on top of it, [lessormq-spring-boot-starter](https://github.com/gsid-nl/lessormq/tree/main/clients/spring-boot).
Both are published from `clients/` in [gsid-nl/lessormq](https://github.com/gsid-nl/lessormq);
issues and pull requests go there.

## Install

```text
dev.lessormq:lessormq
```

From Maven Central, with Maven:

```xml
<dependency>
    <groupId>dev.lessormq</groupId>
    <artifactId>lessormq</artifactId>
    <version>0.1.0</version>
</dependency>
```

or Gradle:

```kotlin
implementation("dev.lessormq:lessormq:0.1.0")
```

The newest version is on [Maven Central](https://central.sonatype.com/artifact/dev.lessormq/lessormq).

## Connect

```java
LessorMQClient client = new LessorMQClient("https://mq.example.com", System.getenv("LESSORMQ_KEY"));
```

One client per server and key, shared by every thread. The builder takes the rest:

```java
LessorMQClient client = LessorMQClient.builder("https://mq.example.com", key)
        .retries(3)                         // after network errors, 5xx and 429
        .retryDelay(Duration.ofMillis(100)) // times the attempt, or the server's Retry-After
        .timeout(Duration.ofSeconds(10))    // per request; a claim gets its wait on top
        .jsonMapper(myJsonMapper)           // bodies are written and read with this one
        .httpClient(myHttpClient)           // proxies, TLS
        .build();
```

Without a `jsonMapper`, the client makes a Jackson 3 `JsonMapper` with the modules found on
the classpath (the Kotlin module, say). Retries and a worker's troubles are logged to
`System.getLogger("dev.lessormq")`, which ends up in java.util.logging, or in SLF4J or
Log4j when their bridge is there.

## Publish

```java
client.publish("mail", Map.of("to", "ada@example.com"));

client.publish("invoices", new Invoice(1042), PublishOptions.builder()
        .dedupKey("invoice-1042") // one open message per key
        .delaySeconds(600)
        .header("type", "InvoiceDue")
        .build());
```

The body is anything Jackson writes, or a `JsonNode` as it is. Every publish carries a
dedup key (your own, or one the client makes up), so the client can safely retry it after
a network error without creating a second message. The answer, `Published`, says whether
an open message with the same key was returned instead (`duplicate()`).

## Work

```java
Worker worker = new Worker(client, "mail", WorkerOptions.builder().concurrency(4).build(), job -> {
    Mail mail = job.message().decode(Mail.class);
    if (mail.to().isEmpty()) {
        throw new PermanentFailure("no recipient");
    }
    send(mail);
});
worker.run();
```

- The handler returns: the message is acked.
- It throws (anything, also an `Error`): the message is failed with the exception's class
  and text. A `PermanentFailure`, or an exception of your own that implements `Permanent`,
  anywhere in the chain of causes, is reported as permanent: the input itself is unusable.
  A body that does not decode (`decode` throws `PermanentFailure`) is too.
- What happens after a failure is not up to the worker. The queue's settings decide
  whether and when the message comes back, and when it goes to the dead-letter set.
- While the handler runs, the lease is extended every third of it. When the lease is lost
  anyway, `job.isLeaseLost()` turns true and the outcome is not reported: the message is
  someone else's by then. A long handler can check it and stop early.
- `concurrency` runs that many handlers side by side, each on its own thread with its own
  claims.
- `run()` blocks until the worker stops: on SIGTERM or Ctrl+C (the JVM's shutdown waits
  for it), `stop()` or `close()` from another thread, or an interrupt. `start()` runs it
  in the background instead, and leaves stopping to you. Either way it stops after the
  current messages: handlers are not interrupted, and a claim already waiting is not cut
  off: it ends within `waitSeconds`, and a message it brings is given back.

`WorkerOptions`: `consumer` (host:pid by default), `route`, `waitSeconds` (20),
`leaseSeconds`, `extendEvery`, `autoExtend` (true), `concurrency` (1), `maxMessages`,
`errorPause` (1 s), `stopOnShutdown` (true) and `threadFactory`
(`Thread.ofVirtual().factory()` on Java 21 runs the handlers on virtual threads).

### Kotlin

```kotlin
data class Mail(val to: String)

val client = LessorMQClient("https://mq.example.com", System.getenv("LESSORMQ_KEY"))
client.publish("mail", Mail("ada@example.com"))

val worker = Worker(client, "mail", WorkerOptions.builder().concurrency(4).build()) { job ->
    val mail = job.message().decode(Mail::class.java)
    if (mail.to.isBlank()) throw PermanentFailure("no recipient")
    send(mail)
}
worker.run()
```

Data classes need `tools.jackson.module:jackson-module-kotlin` on the classpath; the
default mapper finds it. Nullness comes through: `client.claim(...)` is a `Lease?`,
`decode` a non-null value.

## Without the worker

```java
Lease lease = client.claim("mail", ClaimOptions.builder().waitSeconds(20).build());
if (lease != null) {
    try {
        send(lease.message().decode(Mail.class));
        client.ack(lease);
    } catch (Exception e) {
        FailResult result = client.fail(lease, e); // result.outcome(): retrying, dead_lettered or discarded
    }
}
```

`client.extend(lease)` keeps a lease while a long job runs, and `client.release(lease)`
hands a message back without counting the attempt. All of them throw
`LeaseLostException` once the lease has ended: drop the message then, it is someone else's
now. `client.fail(lease, e, FailOptions.builder()...)` sets the error type, details, or
permanence yourself, and `Lease.of(messageId, receipt)` reports on a lease claimed
elsewhere.

Many short jobs go faster in batches: `client.claimBatch("mail", 50, options)` leases up
to 50 ready messages at once (each with its own lease), and `client.ackBatch(leases)` acks
them in one request, with an outcome per lease (`ACKED`, `LEASE_LOST`, ...). A message
that fails is reported on its own with `fail`.

## Errors

Everything the library throws is a `LessorMQException`, unchecked. The server's errors are
an `ApiException` (with `status()`, `code()`, `detail()` and `errors()`), as one of
`LeaseLostException`, `ValidationException`, `UnauthorizedException`,
`ForbiddenException`, `NotFoundException`, `ConflictException`,
`PayloadTooLargeException`, `RateLimitedException` and `ServerException`; any other code
(such as `quota_exceeded`) is an `ApiException` itself. A server that cannot be reached
gives a `TransportException`.

Network errors, 5xx and 429 are retried (`retries(3)`, `retryDelay(100 ms)` times the
attempt, or the server's `Retry-After`). That is the only retrying the library does. See
the [protocol](https://lessormq.dev/docs/protocol) for what the server decides.

## Development

Java 17 or newer and Maven:

```sh
cd clients/java
mvn verify
```

The library passes the LessorMQ conformance suite through
`src/test/java/dev/lessormq/conformance/ConformanceAdapter.java`, run on the test
classpath (`mvn test-compile` copies the dependencies to `target/dependency`):

```sh
(cd clients/java && mvn -q test-compile)
LESSORMQ_ADAPTER_CMD="java -cp 'clients/java/target/classes:clients/java/target/test-classes:clients/java/target/dependency/*' dev.lessormq.conformance.ConformanceAdapter" conformance/run.sh
```

See [writing a client](https://lessormq.dev/docs/writing-a-client) for what the suite
checks.
