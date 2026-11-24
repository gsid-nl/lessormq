package dev.lessormq;

/**
 * The server could not be reached, after the configured retries, or the thread was
 * interrupted while it waited for an answer (then the interrupt flag is set again).
 */
public final class TransportException extends LessorMQException {
    private static final long serialVersionUID = 1L;

    private final String method;
    private final String path;

    public TransportException(String method, String path, Throwable cause) {
        super("LessorMQ " + method + " " + path + " failed: " + cause, cause);
        this.method = method;
        this.path = path;
    }

    public String method() {
        return method;
    }

    public String path() {
        return path;
    }
}
