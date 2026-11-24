package dev.lessormq.spring;

import dev.lessormq.LessorMQClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.json.JsonMapper;

/**
 * A {@link LessorMQClient} from the lessormq.* properties, and workers for the
 * {@link LessorMQListener} methods. Ordered after Jackson's auto-configuration, so bodies
 * are written and read with the application's JsonMapper (Spring Boot 4), modules and all.
 */
@AutoConfiguration(afterName = "org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration")
@EnableConfigurationProperties(LessorMQProperties.class)
public class LessorMQAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public LessorMQClient lessorMQClient(LessorMQProperties properties, ObjectProvider<JsonMapper> jsonMapper,
            ObjectProvider<LessorMQClientCustomizer> customizers) {
        String url = properties.getUrl();
        if (url == null || url.isBlank()) {
            throw new IllegalStateException("Set lessormq.url (and lessormq.key) to use LessorMQ");
        }
        String key = properties.getKey();
        LessorMQClient.Builder builder = LessorMQClient.builder(url, key == null ? "" : key)
                .retries(properties.getHttpRetries())
                .retryDelay(properties.getRetryDelay())
                .timeout(properties.getTimeout())
                .autoDedupKey(properties.isAutoDedupKey())
                .userAgent(LessorMQClient.USER_AGENT + " spring-boot");
        jsonMapper.ifUnique(builder::jsonMapper);
        customizers.orderedStream().forEach(customizer -> customizer.customize(builder));
        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean
    public LessorMQListenerContainer lessorMQListenerContainer(ObjectProvider<LessorMQClient> client,
            LessorMQProperties properties) {
        return new LessorMQListenerContainer(client, properties);
    }

    // Static: a post-processor made by an instance method would make this class early.
    @Bean
    static LessorMQListenerAnnotationBeanPostProcessor lessorMQListenerAnnotationBeanPostProcessor() {
        return new LessorMQListenerAnnotationBeanPostProcessor();
    }
}
