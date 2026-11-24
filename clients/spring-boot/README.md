# LessorMQ for Spring Boot

A Spring Boot starter for [LessorMQ](https://lessormq.dev), the message queue over HTTP
where the server decides and clients only claim, run, ack or fail. It configures a
`LessorMQClient` bean from `lessormq.*` properties and runs `@LessorMQListener` methods in
workers that start and stop with the application. The queue decides what happens to a
failed message (retries, backoff, the dead-letter set); the starter only reports.

Spring Boot 4.0, 4.1 or 4.2 (3.5 works as well), Java 17 or newer, Kotlin included. The
package is `dev.lessormq:lessormq-spring-boot-starter` on Maven Central, built on the Java
client [dev.lessormq:lessormq](https://github.com/gsid-nl/lessormq/tree/main/clients/java).

## Install

```text
dev.lessormq:lessormq-spring-boot-starter
```

From Maven Central, with Maven:

```xml
<dependency>
    <groupId>dev.lessormq</groupId>
    <artifactId>lessormq-spring-boot-starter</artifactId>
    <version>0.1.0</version>
</dependency>
```

or Gradle:

```kotlin
implementation("dev.lessormq:lessormq-spring-boot-starter:0.1.0")
```

The newest version is on [Maven Central](https://central.sonatype.com/artifact/dev.lessormq/lessormq-spring-boot-starter).

## Configure

```yaml
# application.yaml
lessormq:
  url: https://mq.example.com
  key: ${LESSORMQ_KEY}
  worker:
    concurrency: 4
```

`LESSORMQ_URL` and `LESSORMQ_KEY` in the environment do the same, by Spring's relaxed
binding. The application does not start without a URL.

| Property | Default | |
|---|---|---|
| `lessormq.url` | (required) | the server |
| `lessormq.key` | | a key with the publish and/or consume scope |
| `lessormq.http-retries` | 3 | retries after network errors, 5xx and 429 |
| `lessormq.retry-delay` | 100ms | between those retries, times the attempt |
| `lessormq.timeout` | 10s | per request; a claim gets its wait on top |
| `lessormq.auto-dedup-key` | true | a dedup key on every publish, so it can be retried safely |
| `lessormq.worker.auto-startup` | true | start the workers with the application |
| `lessormq.worker.concurrency` | 1 | messages handled at the same time, per listener |
| `lessormq.worker.consumer` | host:pid | shown to administrators, in the server's list of workers |
| `lessormq.worker.route` | | the path the workers run jobs through (VPN, proxy, account) |
| `lessormq.worker.wait-seconds` | 5 | how long a claim waits for a message (at most 20) |
| `lessormq.worker.lease-seconds` | the queue's | lease per claim, and per extend |
| `lessormq.worker.auto-extend` | true | extend the lease while a handler runs |
| `lessormq.worker.extend-every` | a third of the lease | |
| `lessormq.worker.error-pause` | 1s | the wait after a claim failed |

There is no property for retries, delays or dead-lettering: set those on the queue
(`PATCH /v1/queues/{queue}`, see [concepts](https://lessormq.dev/docs/concepts)).

Bodies are written and read with the application's `JsonMapper` (Jackson 3, Spring Boot
4), so its modules and settings apply; without one (Spring Boot 3.5 configures Jackson 2),
the client makes its own, with the Jackson 3 modules on the classpath. A
`LessorMQClientCustomizer` bean changes the client's builder (your own `HttpClient`, a
logger), and a `LessorMQClient` bean of your own replaces the starter's.

## Dispatch

Inject the client and publish:

```java
@Service
class Signup {
    private final LessorMQClient lessorMQ;

    Signup(LessorMQClient lessorMQ) {
        this.lessorMQ = lessorMQ;
    }

    void welcome(User user) {
        lessorMQ.publish("mail", new Mail(user.email()));
        lessorMQ.publish("invoices", new Invoice(1042), PublishOptions.builder()
                .dedupKey("invoice-1042")
                .delaySeconds(600)
                .build());
    }
}
```

A publish goes out at once, not when a surrounding transaction commits. To publish only
after the commit, publish from a `@TransactionalEventListener`.

## Run workers

```java
@Component
class Mailer {
    @LessorMQListener(queue = "mail")
    void send(Mail mail) {
        // ...
    }

    @LessorMQListener(queue = "${app.queues.invoices}", concurrency = "8")
    void remind(Invoice invoice, Message message, Job job) {
        if (message.isLastAttempt()) {
            // ...
        }
    }
}
```

```kotlin
@Component
class Mailer {
    @LessorMQListener(queue = "mail")
    fun send(mail: Mail) {
        // ...
    }
}
```

Every listener gets a worker of its own; the workers start once the application context
is refreshed, and a plain (non-web) application keeps running while they do. A parameter of type
`Message`, `Job` or `Lease` gets the message, the job or its lease; the one other
parameter gets the body, decoded into its type (generics included). The annotation's
`queue`, `concurrency`, `route`, `consumer` and `leaseSeconds` take placeholders and
override the `lessormq.worker.*` properties. `LessorMQListenerContainer` (a bean) has the
running workers.

Workers only in some deployments: `lessormq.worker.auto-startup=false` where the
application should publish but not consume, and in tests that should not claim.

## How it behaves

- A listener that returns: the message is acked.
- A listener that throws: the message is failed with the exception's class and message,
  and the queue decides what follows (another attempt after its backoff, or the
  dead-letter set after its `max_attempts`).
- `PermanentFailure`, and any exception of yours implementing `Permanent` (also as the
  cause of another), is reported as permanent: the input itself is unusable. A body that
  does not decode into the parameter's type is too. The queue's `on_permanent` decides
  what that means.
- Error handlers: there is none to configure. The exception is the report.
- Retries: none in the application. A `@Retryable` listener (Spring Retry, or Spring
  Framework 7's own) is refused at startup, as is `@Async`: both would take the outcome
  away from the queue. The attempt count, `max_attempts` and the backoff are the queue's;
  `message.attempt()` tells the listener which attempt it runs.
- Backoff and delays: the queue's `backoff`. A delay of your own is `delaySeconds` on
  publish.
- Release: a message claimed while the application stops is given back unhandled, without
  counting the attempt.
- Failed jobs: there is no failed-jobs table. Messages that ran out of attempts, or failed
  permanently, stay in the queue's dead-letter set, where an administrator can look at and
  replay them.
- Timeouts: there is none on the listener. While it runs, the lease is extended every
  third of it; a lease lost anyway (`job.isLeaseLost()`) means the message is someone
  else's, and the outcome is not reported. A worker that dies mid-job leaves the lease to
  run out, which the server counts as a failed attempt.
- `@Transactional` listeners: the transaction commits before the ack; a failed commit
  fails the message.
- Shutdown: the workers stop before the rest of the application. A running listener
  finishes and reports; a waiting claim ends within `wait-seconds`. Spring waits up to
  `spring.lifecycle.timeout-per-shutdown-phase` (30s), so raise that above your longest
  listener.
- Delivery is at least once: a message can run twice when a lease runs out under a slow
  listener or an ack is lost. Make listeners tolerate that.

## Moving from another queue

- Retries move to the queue: set its `max_attempts` and `backoff` to what the old retry
  policy did, and remove `@Retryable`, retry templates and `RetryTopic` setups from the
  listeners.
- A dead-letter queue or failed-jobs table is not needed: the queue's dead-letter set
  keeps them, filterable by error type, and replays them.
- `@RabbitListener`, `@KafkaListener`, `@JmsListener` and `@SqsListener` methods become
  `@LessorMQListener` methods; the body parameter works the same way. Listener container
  factories become `lessormq.worker.*` properties, or the annotation's attributes.
- `RabbitTemplate.convertAndSend`, `KafkaTemplate.send` and friends become
  `lessorMQ.publish(queue, body)`. Message keys used to avoid duplicates become
  `dedupKey`.
- Workers on different network paths (VPN exits, proxies) set `route`; see
  [routes](https://lessormq.dev/docs/concepts#routes).
- Run the old and the new listeners side by side and move one queue at a time.

## Development

The client first, then the starter, with Java 17 or newer and Maven:

```sh
(cd clients/java && mvn install)
cd clients/spring-boot
mvn verify
mvn verify -Dspring-boot.version=4.2.0   # another Spring Boot line
```

The tests use Spring Boot's `ApplicationContextRunner` against a scripted HTTP server.
With `LESSORMQ_URL` and `LESSORMQ_OPERATOR_KEY` set, `EndToEndTest` also runs against
that server, in a tenant of its own: a published job is run by its listener and acked, a
failing one comes back after the queue's backoff as attempt 2, and a permanent failure
lands in the dead-letter set. The client itself passes the conformance suite (see its
README); see [the protocol](https://lessormq.dev/docs/protocol) for the rules.
