package dev.lessormq.spring;

import dev.lessormq.WorkerOptions;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

/**
 * Finds @LessorMQListener methods on beans and hands them to the
 * {@link LessorMQListenerContainer} once every singleton exists, so the container (and the
 * client it needs) is not made early.
 */
final class LessorMQListenerAnnotationBeanPostProcessor
        implements BeanPostProcessor, BeanFactoryAware, SmartInitializingSingleton {
    private final List<Found> found = Collections.synchronizedList(new ArrayList<>());
    private final Set<Class<?>> without = ConcurrentHashMap.newKeySet();
    private @Nullable ConfigurableBeanFactory beanFactory;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        if (beanFactory instanceof ConfigurableBeanFactory configurable) {
            this.beanFactory = configurable;
        }
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        if (without.contains(targetClass)) {
            return bean;
        }
        Map<Method, LessorMQListener> methods = MethodIntrospector.selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<LessorMQListener>) method ->
                        AnnotatedElementUtils.findMergedAnnotation(method, LessorMQListener.class));
        if (methods.isEmpty()) {
            without.add(targetClass);
            return bean;
        }
        methods.forEach((method, annotation) -> {
            // Through the proxy, if there is one, so @Transactional and the like apply.
            Method invocable = AopUtils.selectInvocableMethod(method, bean.getClass());
            try {
                found.add(new Found(annotation, new ListenerMethod(bean, invocable, method, targetClass)));
            } catch (IllegalStateException e) {
                throw new BeanInitializationException(e.getMessage(), e);
            }
        });
        return bean;
    }

    /** A listener method and its annotation, until the properties can be read. */
    private record Found(LessorMQListener annotation, ListenerMethod handler) {
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (beanFactory == null || found.isEmpty()) {
            return;
        }
        LessorMQListenerContainer container = beanFactory.getBean(LessorMQListenerContainer.class);
        LessorMQProperties.Worker defaults = beanFactory.getBean(LessorMQProperties.class).getWorker();
        synchronized (found) {
            for (Found listener : found) {
                String queue = resolve(listener.annotation().queue());
                if (queue.isEmpty()) {
                    throw new BeanInitializationException("@LessorMQListener " + listener.handler() + " names no queue");
                }
                container.register(new LessorMQListenerContainer.Listener(queue,
                        options(listener.annotation(), defaults, listener.handler()), listener.handler()));
            }
            found.clear();
        }
    }

    private WorkerOptions options(LessorMQListener annotation, LessorMQProperties.Worker defaults,
            ListenerMethod handler) {
        String consumer = resolve(annotation.consumer());
        String route = resolve(annotation.route());
        int concurrency = number(annotation.concurrency(), defaults.getConcurrency(), "concurrency", handler);
        if (concurrency < 1) {
            throw new BeanInitializationException("@LessorMQListener " + handler + ": concurrency must be 1 or more");
        }
        return WorkerOptions.builder()
                .consumer(!consumer.isEmpty() ? consumer : blankToNull(defaults.getConsumer()))
                .route(!route.isEmpty() ? route : blankToNull(defaults.getRoute()))
                .concurrency(concurrency)
                .leaseSeconds(number(annotation.leaseSeconds(), defaults.getLeaseSeconds(), "leaseSeconds", handler))
                .waitSeconds(defaults.getWaitSeconds())
                .autoExtend(defaults.isAutoExtend())
                .extendEvery(defaults.getExtendEvery())
                .errorPause(defaults.getErrorPause())
                .build();
    }

    private int number(String value, int fallback, String attribute, ListenerMethod handler) {
        String resolved = resolve(value);
        if (resolved.isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(resolved);
        } catch (NumberFormatException e) {
            throw new BeanInitializationException("@LessorMQListener " + handler + ": " + attribute
                    + " is not a number: " + resolved, e);
        }
    }

    private String resolve(String value) {
        if (value.isEmpty() || beanFactory == null) {
            return value;
        }
        String resolved = beanFactory.resolveEmbeddedValue(value);
        return resolved == null ? "" : resolved.trim();
    }

    private static @Nullable String blankToNull(@Nullable String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
