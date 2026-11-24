package dev.lessormq.spring;

import static org.assertj.core.api.Assertions.assertThat;

import dev.lessormq.Job;
import dev.lessormq.LessorMQClient;
import dev.lessormq.Message;
import dev.lessormq.PermanentFailure;
import dev.lessormq.Worker;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Async;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class LessorMQAutoConfigurationTest {
    private FakeServer fake;
    private ApplicationContextRunner runner;

    @BeforeEach
    void start() throws Exception {
        fake = new FakeServer();
        runner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(LessorMQAutoConfiguration.class))
                .withPropertyValues("lessormq.url=" + fake.url(), "lessormq.key=lmq_key",
                        "lessormq.retry-delay=0ms", "lessormq.worker.wait-seconds=1",
                        "lessormq.worker.consumer=test-worker");
    }

    @AfterEach
    void stop() {
        fake.close();
    }

    record Mail(String to) {
    }

    /** Records what the listeners saw; the latch counts handled messages. */
    static final class Seen {
        final List<Object> values = new CopyOnWriteArrayList<>();
        final CountDownLatch handled;

        Seen(int messages) {
            handled = new CountDownLatch(messages);
        }

        void await() throws InterruptedException {
            assertThat(handled.await(5, TimeUnit.SECONDS)).as("handled in time").isTrue();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class MailListener {
        @Bean
        Seen seen() {
            return new Seen(1);
        }

        @Bean
        Object mailer(Seen seen) {
            return new Object() {
                @LessorMQListener(queue = "${app.queue:mail}", concurrency = "2")
                void send(Mail mail, Message message, Job job) {
                    seen.values.add(mail);
                    seen.values.add(message.headers());
                    seen.values.add(job.lease().receipt());
                    seen.handled.countDown();
                }
            };
        }
    }

    @Test
    void theClientComesFromTheProperties() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(LessorMQClient.class);
            assertThat(context.getBean(LessorMQClient.class).toString()).isEqualTo("LessorMQClient(" + fake.url() + ")");
            assertThat(context.getBean(LessorMQListenerContainer.class).getWorkers()).isEmpty();
        });
    }

    @Test
    void withoutAUrlTheApplicationDoesNotStart() {
        // Empty rather than absent: LESSORMQ_URL in the environment would set it.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(LessorMQAutoConfiguration.class))
                .withPropertyValues("lessormq.url=")
                .run(context -> assertThat(context).getFailure().rootCause().hasMessageContaining("lessormq.url"));
    }

    @Test
    void theApplicationsJsonMapperAndCustomizersAreUsed() {
        JsonMapper mapper = JsonMapper.builder().build();
        runner.withBean(JsonMapper.class, () -> mapper)
                .withBean(LessorMQClientCustomizer.class, () -> builder -> builder.userAgent("my-app"))
                .run(context -> {
                    LessorMQClient client = context.getBean(LessorMQClient.class);
                    assertThat(client.jsonMapper()).isSameAs(mapper);
                    fake.on("/v1/queues/mail/messages", 201, "{\"id\":\"m1\",\"duplicate\":false}");
                    client.publish("mail", new Mail("ada@example.com"));
                    FakeServer.Request request = fake.requests().get(0);
                    assertThat(request.headers().get("User-agent")).containsExactly("my-app");
                    assertThat(request.headers().get("Authorization")).containsExactly("Bearer lmq_key");
                    assertThat(request.body().path("body").path("to").asString()).isEqualTo("ada@example.com");
                });
    }

    @Test
    void anOwnClientBeanWins() {
        LessorMQClient own = new LessorMQClient("https://own.example.com", "lmq_own");
        runner.withBean(LessorMQClient.class, () -> own)
                .run(context -> assertThat(context.getBean(LessorMQClient.class)).isSameAs(own));
    }

    @Test
    void aListenerRunsTheMessageAndAcks() {
        fake.once("mail", FakeServer.LEASE);
        runner.withUserConfiguration(MailListener.class).run(context -> {
            Seen seen = context.getBean(Seen.class);
            seen.await();
            assertThat(seen.values).containsExactly(new Mail("ada@example.com"), Map.of("type", "Welcome"), "r1");
            List<Worker> workers = context.getBean(LessorMQListenerContainer.class).getWorkers();
            assertThat(workers).hasSize(1);
            assertThat(workers.get(0).queue()).isEqualTo("mail");
            context.close();
            assertThat(fake.calls("/v1/messages/m1/ack")).hasSize(1);
            JsonNode claim = fake.calls("/v1/queues/mail/claims").get(0);
            assertThat(claim.path("consumer").asString()).isEqualTo("test-worker");
            assertThat(claim.path("wait_seconds").asInt()).isEqualTo(1);
        });
    }

    @Test
    void placeholdersNameTheQueue() {
        fake.once("orders", FakeServer.LEASE);
        runner.withUserConfiguration(MailListener.class).withPropertyValues("app.queue=orders").run(context -> {
            context.getBean(Seen.class).await();
            assertThat(context.getBean(LessorMQListenerContainer.class).getWorkers().get(0).queue())
                    .isEqualTo("orders");
        });
    }

    @Test
    void workersCanStayOff() {
        fake.once("mail", FakeServer.LEASE);
        runner.withUserConfiguration(MailListener.class).withPropertyValues("lessormq.worker.auto-startup=false")
                .run(context -> {
                    assertThat(context.getBean(LessorMQListenerContainer.class).isRunning()).isFalse();
                    assertThat(fake.requests()).isEmpty();
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class FailingListeners {
        @Bean
        Seen seen() {
            return new Seen(3);
        }

        @Bean
        Object failing(Seen seen) {
            return new Object() {
                @LessorMQListener(queue = "flaky")
                void flaky(JsonNode body) {
                    seen.handled.countDown();
                    throw new IllegalStateException("upstream slow");
                }

                @LessorMQListener(queue = "broken")
                void broken(Mail mail) {
                    seen.handled.countDown();
                    throw new PermanentFailure("no recipient for " + mail.to());
                }

                @LessorMQListener(queue = "undecodable")
                void undecodable(List<Integer> numbers) {
                    seen.values.add(numbers);
                }
            };
        }
    }

    @Test
    void exceptionsAreRetryableFailuresUnlessPermanent() {
        String lease = FakeServer.LEASE;
        fake.once("flaky", lease.replace("m1", "f1"))
                .once("broken", lease.replace("m1", "b1"))
                .once("undecodable", lease.replace("m1", "u1"))
                .on("/v1/messages/f1/fail", 200, FakeServer.FAILED)
                .on("/v1/messages/b1/fail", 200, FakeServer.FAILED)
                .on("/v1/messages/u1/fail", 200, FakeServer.FAILED);
        runner.withUserConfiguration(FailingListeners.class).run(context -> {
            Seen seen = context.getBean(Seen.class);
            seen.handled.countDown();
            seen.await();
            fake.await("/v1/messages/u1/fail");
            context.close();

            JsonNode flaky = fake.calls("/v1/messages/f1/fail").get(0);
            assertThat(flaky.path("permanent").asBoolean()).isFalse();
            assertThat(flaky.path("error").path("type").asString()).isEqualTo("java.lang.IllegalStateException");
            assertThat(flaky.path("error").path("message").asString()).isEqualTo("upstream slow");

            JsonNode broken = fake.calls("/v1/messages/b1/fail").get(0);
            assertThat(broken.path("permanent").asBoolean()).isTrue();
            assertThat(broken.path("error").path("message").asString()).isEqualTo("no recipient for ada@example.com");

            JsonNode undecodable = fake.calls("/v1/messages/u1/fail").get(0);
            assertThat(undecodable.path("permanent").asBoolean()).isTrue();
            assertThat(seen.values).isEmpty();
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class SlowListener {
        @Bean
        Seen seen() {
            return new Seen(1);
        }

        @Bean
        Object slow(Seen seen) {
            return new Object() {
                @LessorMQListener(queue = "mail")
                void handle(Mail mail) throws InterruptedException {
                    seen.handled.countDown();
                    Thread.sleep(500);
                    seen.values.add("done");
                }
            };
        }
    }

    @Test
    void closingTheContextLetsTheHandlerFinishAndReport() {
        fake.once("mail", FakeServer.LEASE);
        runner.withUserConfiguration(SlowListener.class).run(context -> {
            Seen seen = context.getBean(Seen.class);
            seen.await();
            context.close();
            assertThat(seen.values).containsExactly("done");
            assertThat(fake.calls("/v1/messages/m1/ack")).hasSize(1);
        });
    }

    static class Parallel {
        @Async
        @LessorMQListener(queue = "mail")
        void handle(Mail mail) {
        }
    }

    static class TwoBodies {
        @LessorMQListener(queue = "mail")
        void handle(Mail mail, String other) {
        }
    }

    @Test
    void methodsThatWouldTakeTheOutcomeAwayAreRefused() {
        runner.withBean(Parallel.class).run(context -> assertThat(context).getFailure()
                .rootCause().hasMessageContaining("must not be @Async"));
        runner.withBean(TwoBodies.class).run(context -> assertThat(context).getFailure()
                .rootCause().hasMessageContaining("more than one parameter for the body"));
    }

    @Test
    void theWorkerOptionsComeFromThePropertiesAndTheAnnotation() {
        fake.once("mail", FakeServer.LEASE);
        runner.withUserConfiguration(MailListener.class)
                .withPropertyValues("lessormq.worker.route=vpn-2", "lessormq.worker.lease-seconds=60")
                .run(context -> {
                    context.getBean(Seen.class).await();
                    context.close();
                    JsonNode claim = fake.calls("/v1/queues/mail/claims").get(0);
                    assertThat(claim.path("route").asString()).isEqualTo("vpn-2");
                    assertThat(claim.path("lease_seconds").asInt()).isEqualTo(60);
                    // concurrency = "2": two threads claim.
                    assertThat(fake.calls("/v1/queues/mail/claims").size()).isGreaterThanOrEqualTo(2);
                });
    }
}
