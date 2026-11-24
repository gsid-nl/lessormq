namespace LessorMQ.Extensions.Hosting;

/// <summary>
/// Handles the messages of a queue in a hosted worker. A new one is resolved for every
/// message, in a scope of its own, so scoped services (a DbContext) are fresh per
/// message. Returning acks the message; throwing fails it, as permanent when the
/// exception is (or wraps) a <see cref="PermanentFailureException"/>. Whether and when a
/// failed message comes back is the queue's decision.
/// </summary>
public interface IMessageHandler
{
    /// <summary>
    /// Runs one message. <paramref name="cancellationToken"/> is cancelled when the lease
    /// is lost and when the host stops; a handler that gives up on a stop by throwing an
    /// <see cref="OperationCanceledException"/> has its message released, so the attempt
    /// does not count.
    /// </summary>
    Task HandleAsync(Job job, CancellationToken cancellationToken);
}
