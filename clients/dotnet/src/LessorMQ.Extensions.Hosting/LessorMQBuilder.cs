using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace LessorMQ.Extensions.Hosting;

/// <summary>Adds workers to the services that <c>AddLessorMQ</c> registered.</summary>
public sealed class LessorMQBuilder
{
    private readonly IConfiguration? configuration;

    internal LessorMQBuilder(IServiceCollection services, IConfiguration? configuration)
    {
        Services = services;
        this.configuration = configuration;
    }

    /// <summary>The service collection.</summary>
    public IServiceCollection Services { get; }

    internal Func<IServiceProvider, HttpClient>? HttpClientFactory { get; private set; }

    /// <summary>
    /// Runs <typeparamref name="THandler"/> on every message of <paramref name="queue"/>,
    /// as a hosted service that starts and stops with the host. The handler is registered
    /// as scoped unless it already is registered. Worker options come from the
    /// configuration section <c>Workers:{queue}</c> under the one given to
    /// <c>AddLessorMQ</c>, then from <paramref name="configure"/>.
    /// </summary>
    public LessorMQBuilder AddWorker<THandler>(string queue, Action<WorkerOptions>? configure = null)
        where THandler : class, IMessageHandler
    {
        Services.TryAddScoped<THandler>();
        return AddWorker(
            queue,
            (job, services, cancellationToken) => services.GetRequiredService<THandler>().HandleAsync(job, cancellationToken),
            configure);
    }

    /// <summary>
    /// Runs <paramref name="handler"/> on every message of <paramref name="queue"/>, as a
    /// hosted service. The handler gets the service provider of a scope made for that
    /// message. Worker options as for <see cref="AddWorker{THandler}"/>.
    /// </summary>
    public LessorMQBuilder AddWorker(
        string queue,
        Func<Job, IServiceProvider, CancellationToken, Task> handler,
        Action<WorkerOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);
        ArgumentNullException.ThrowIfNull(handler);
        var section = configuration?.GetSection("Workers").GetSection(queue);

        // Not AddHostedService: it registers a type once, and every queue needs its own.
        Services.AddSingleton<IHostedService>(provider =>
        {
            var options = new WorkerOptions();
            section?.Bind(options);
            configure?.Invoke(options);
            return new LessorMQWorkerService(
                provider.GetRequiredService<LessorMQClient>(),
                provider.GetRequiredService<IServiceScopeFactory>(),
                queue,
                handler,
                options);
        });
        return this;
    }

    /// <summary>
    /// Gives the client an HttpClient of your own (a proxy, client certificates, a test
    /// handler). It is not disposed by the client; its timeout should be infinite or above
    /// 30 seconds. Leave out retrying handlers (Polly, resilience pipelines): the client
    /// already retries what is safe to repeat, and only that.
    /// </summary>
    public LessorMQBuilder UseHttpClient(Func<IServiceProvider, HttpClient> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        HttpClientFactory = factory;
        return this;
    }
}
