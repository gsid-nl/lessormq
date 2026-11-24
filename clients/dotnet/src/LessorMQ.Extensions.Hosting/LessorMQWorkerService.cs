using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace LessorMQ.Extensions.Hosting;

/// <summary>
/// A <see cref="LessorMQWorker"/> as a hosted service: it starts with the host, and on
/// stop finishes the messages in hand (see <see cref="LessorMQWorker.RunAsync"/>). Keep
/// the worker's <c>WaitSeconds</c> plus your longest handler below the host's
/// <c>ShutdownTimeout</c>.
/// </summary>
internal sealed class LessorMQWorkerService : BackgroundService
{
    private readonly LessorMQWorker worker;

    public LessorMQWorkerService(
        LessorMQClient client,
        IServiceScopeFactory scopes,
        string queue,
        Func<Job, IServiceProvider, CancellationToken, Task> handler,
        WorkerOptions options)
    {
        worker = new LessorMQWorker(
            client,
            queue,
            async (job, cancellationToken) =>
            {
                // A scope per message, as ASP.NET Core makes one per request.
                await using var scope = scopes.CreateAsyncScope();
                await handler(job, scope.ServiceProvider, cancellationToken).ConfigureAwait(false);
            },
            options);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => worker.RunAsync(stoppingToken);
}
