package dev.lessormq.spring;

import dev.lessormq.Handler;
import dev.lessormq.Job;
import dev.lessormq.Lease;
import dev.lessormq.Message;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.springframework.util.ReflectionUtils;

/**
 * A @LessorMQListener method as a worker's handler: its parameters filled from the job,
 * its exceptions passed on as they are, so the worker reports them to the queue.
 */
final class ListenerMethod implements Handler {
    /** Retry and async proxies that would take the outcome away from the queue. */
    private static final Set<String> REFUSED = Set.of(
            "org.springframework.retry.annotation.Retryable",
            "org.springframework.resilience.annotation.Retryable",
            "org.springframework.scheduling.annotation.Async");

    private final Object bean;
    private final Method method;
    private final Argument[] arguments;
    private final String description;

    /** Fills one parameter from the job. */
    @FunctionalInterface
    private interface Argument {
        @Nullable Object from(Job job);
    }

    ListenerMethod(Object bean, Method method, Method declared, Class<?> targetClass) {
        this.bean = bean;
        this.method = method;
        this.description = targetClass.getName() + "." + declared.getName();
        refuse(declared, targetClass);
        Type[] types = declared.getGenericParameterTypes();
        Class<?>[] classes = declared.getParameterTypes();
        this.arguments = new Argument[classes.length];
        boolean body = false;
        for (int i = 0; i < classes.length; i++) {
            Class<?> type = classes[i];
            if (type == Message.class) {
                arguments[i] = Job::message;
            } else if (type == Job.class) {
                arguments[i] = job -> job;
            } else if (type == Lease.class) {
                arguments[i] = Job::lease;
            } else if (type.getName().equals("kotlin.coroutines.Continuation")) {
                throw new IllegalStateException("@LessorMQListener " + description
                        + " is a suspend function; make it a plain one: the worker runs it on a thread of its own");
            } else if (body) {
                throw new IllegalStateException("@LessorMQListener " + description + " has more than one parameter"
                        + " for the body; the others can be Message, Job or Lease");
            } else {
                body = true;
                Type bodyType = types[i];
                arguments[i] = job -> job.message().decode(bodyType);
            }
        }
        ReflectionUtils.makeAccessible(method);
    }

    private void refuse(Method declared, Class<?> targetClass) {
        for (Annotation[] annotations : new Annotation[][] {declared.getAnnotations(), targetClass.getAnnotations()}) {
            for (Annotation annotation : annotations) {
                String name = annotation.annotationType().getName();
                if (REFUSED.contains(name)) {
                    throw new IllegalStateException("@LessorMQListener " + description + " must not be @"
                            + annotation.annotationType().getSimpleName() + ": the queue decides about retries,"
                            + " and the outcome is reported when the method returns or throws");
                }
            }
        }
    }

    @Override
    public void handle(Job job) throws Exception {
        Object[] values = new Object[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            values[i] = arguments[i].from(job);
        }
        try {
            method.invoke(bean, values);
        } catch (InvocationTargetException e) {
            // The method's own exception, so the report names its type and text.
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

    @Override
    public String toString() {
        return description;
    }
}
