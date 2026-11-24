using LessorMQ;
using LessorMQ.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

// In this namespace so that AddLessorMQ shows up next to AddControllers and the like.
namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers LessorMQ with a service collection.</summary>
public static class LessorMQServiceCollectionExtensions
{
    /// <summary>
    /// Registers a <see cref="LessorMQClient"/> singleton, with its options bound from
    /// <paramref name="configuration"/> (<c>Url</c>, <c>ApiKey</c>, <c>Retries</c>, ...),
    /// and logging through <c>ILogger</c> under the category <c>LessorMQ</c>.
    /// <code>
    /// builder.Services.AddLessorMQ(builder.Configuration.GetSection("LessorMQ"))
    ///     .AddWorker&lt;SendMail&gt;("mail");
    /// </code>
    /// </summary>
    public static LessorMQBuilder AddLessorMQ(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddOptions<LessorMQClientOptions>().Bind(configuration);
        return Add(services, configuration);
    }

    /// <summary>
    /// Registers a <see cref="LessorMQClient"/> singleton with options set in code. Worker
    /// options then come only from the <c>configure</c> of each <c>AddWorker</c>.
    /// </summary>
    public static LessorMQBuilder AddLessorMQ(this IServiceCollection services, Action<LessorMQClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        services.AddOptions<LessorMQClientOptions>().Configure(configure);
        return Add(services, null);
    }

    private static LessorMQBuilder Add(IServiceCollection services, IConfiguration? configuration)
    {
        var builder = new LessorMQBuilder(services, configuration);
        if (services.Any(service => service.ServiceType == typeof(LessorMQClient)))
        {
            return builder;
        }

        // A missing URL or key stops the host at start, not at the first message.
        services.AddOptions<LessorMQClientOptions>()
            .Validate(o => Uri.TryCreate(o.Url, UriKind.Absolute, out _), "LessorMQ: Url must be an absolute URL, such as https://mq.example.com.")
            .Validate(o => !string.IsNullOrWhiteSpace(o.ApiKey), "LessorMQ: ApiKey is required.")
            .ValidateOnStart();
        services.AddSingleton<IPostConfigureOptions<LessorMQClientOptions>>(provider =>
            new PostConfigureOptions<LessorMQClientOptions>(Options.Options.DefaultName, options =>
            {
                if (options.Log is null && provider.GetService<ILoggerFactory>() is { } loggers)
                {
                    options.Log = Bridge(loggers.CreateLogger("LessorMQ"));
                }
            }));
        services.AddSingleton(provider => new LessorMQClient(
            provider.GetRequiredService<IOptions<LessorMQClientOptions>>().Value,
            builder.HttpClientFactory?.Invoke(provider)));
        return builder;
    }

    private static Action<LogEvent> Bridge(ILogger logger) => e =>
    {
        var level = e.Severity switch
        {
            LogSeverity.Debug => LogLevel.Debug,
            LogSeverity.Information => LogLevel.Information,
            LogSeverity.Warning => LogLevel.Warning,
            _ => LogLevel.Error,
        };
        if (logger.IsEnabled(level))
        {
#pragma warning disable CA2254 // The template comes from LessorMQ, with its arguments.
            logger.Log(level, e.Exception, e.Template, [.. e.Arguments]);
#pragma warning restore CA2254
        }
    };
}
