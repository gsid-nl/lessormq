# LessorMQ for .NET

Client and worker for [LessorMQ](https://lessormq.dev), the message queue over HTTP where
the server decides and clients only claim, run, ack or fail. Async throughout, with
`HttpClient` and `System.Text.Json` only, and nullable annotations. Targets .NET 10, the
current LTS, and runs on any later runtime.

Two NuGet packages: `LessorMQ` is the client and the worker, with no dependencies of its
own. `LessorMQ.Extensions.Hosting` adds `services.AddLessorMQ(...)`, options from
configuration and hosted workers, for worker services and ASP.NET Core.

Both are published from `clients/dotnet` in [gsid-nl/lessormq](https://github.com/gsid-nl/lessormq);
issues and pull requests go there.

## Install

```sh
dotnet add package LessorMQ
```

In a worker service or an ASP.NET Core application:

```sh
dotnet add package LessorMQ.Extensions.Hosting
```

## Connect

```csharp
using LessorMQ;

using var client = new LessorMQClient("https://mq.example.com", Environment.GetEnvironmentVariable("LESSORMQ_KEY")!);
```

Make one client per server and key and keep it: it holds the HTTP connections, and it is
safe to use from many threads at once. With every option:

```csharp
var client = new LessorMQClient(new LessorMQClientOptions
{
    Url = "https://mq.example.com",
    ApiKey = apiKey,
    Retries = 3,                                  // after a network error, a 5xx or a 429
    RetryDelay = TimeSpan.FromMilliseconds(100),  // times the attempt; a Retry-After wins
    Timeout = TimeSpan.FromSeconds(10),           // per request; a claim gets its wait on top
    SerializerOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web), // message bodies
    Log = e => Console.Error.WriteLine($"{e.Severity}: {e}"),
});
```

A second argument takes an `HttpClient` of your own (a proxy, client certificates). It
stays yours to dispose, and its `Timeout` should be infinite: the client sets a deadline
per request itself.

In a host, the client comes from configuration:

```json
{
  "LessorMQ": {
    "Url": "https://mq.example.com",
    "ApiKey": "lmq_...",
    "Workers": {
      "mail": { "Concurrency": 4, "Route": "vpn-3" }
    }
  }
}
```

```csharp
builder.Services.AddLessorMQ(builder.Configuration.GetSection("LessorMQ"));
```

That registers `LessorMQClient` as a singleton to inject anywhere, logging through
`ILogger` under the category `LessorMQ`. A missing `Url` or `ApiKey` stops the host when
it starts. `AddLessorMQ(options => ...)` sets the options in code instead, and
`.UseHttpClient(services => ...)` gives the client your own `HttpClient`.

## Publish

```csharp
await client.PublishAsync("mail", new { to = "ada@example.com", template = "welcome" });

await client.PublishAsync("invoices", new Invoice(1042), new PublishOptions
{
    DedupKey = "invoice-1042", // one open message per key
    DelaySeconds = 600,
    Headers = new Dictionary<string, string> { ["type"] = "InvoiceDue" },
});
```

The body is written with `System.Text.Json` and the client's `SerializerOptions` (the web
defaults: camelCase names); a `JsonElement` or `JsonNode` goes out as it is. Every
publish carries a dedup key (your own, or one the client makes up), so the client can
safely retry it after a network error without creating a second message. The answer says
whether an open message with the same key was returned instead (`Published.Duplicate`).

## Work

```csharp
var worker = new LessorMQWorker(client, "mail", async (job, cancellationToken) =>
{
    var mail = job.Message.Decode<Mail>();
    if (mail?.To is null)
    {
        throw new PermanentFailureException("no recipient");
    }

    await mailer.SendAsync(mail, cancellationToken);
}, new WorkerOptions { Route = Environment.GetEnvironmentVariable("VPN_ID"), Concurrency = 4 });

await worker.RunAsync(stoppingToken);
```

- The handler returns: the message is acked.
- It throws: the message is failed with the exception's message and type (`FullName`,
  such as `System.Net.Http.HttpRequestException`), with `exception.ToString()` in the
  details. A `PermanentFailureException`, or an exception that wraps one, is reported as
  permanent: throw it when the input itself is unusable. `new PermanentFailureException(e)`
  reports the type and message of `e`.
- What happens after a failure is not up to the worker. The queue's settings decide
  whether and when the message comes back, and when it goes to the dead-letter set.
- While the handler runs, the lease is extended every third of it. When the lease is lost
  anyway, the handler's `CancellationToken` is cancelled and the result is not reported:
  the message is someone else's by then.
- `Concurrency` runs that many handlers side by side, each with its own claims.
- Cancelling the token passed to `RunAsync` stops the worker after the current messages.
  A claim already waiting is not cut off (that could leave a leased message to run out):
  it ends within `WaitSeconds`, and a message it brings is given back. The handler's token
  is cancelled too: a handler that gives up by throwing an `OperationCanceledException`
  has its message released, so the attempt does not count; one that finishes is acked or
  failed as usual. `RunAsync` returns once every message in hand is reported.

`WorkerOptions`: `Consumer` (host:pid by default), `Route`, `WaitSeconds` (20),
`LeaseSeconds` (the queue's), `ExtendEvery`, `AutoExtend` (true), `Concurrency` (1),
`MaxMessages`, `ErrorPause` (1 s). `RunOnceAsync` makes a single claim.

### In a host

With `LessorMQ.Extensions.Hosting`, a handler is a class, and each queue runs as a hosted
service (a `BackgroundService`) that starts and stops with the host:

```csharp
builder.Services.AddLessorMQ(builder.Configuration.GetSection("LessorMQ"))
    .AddWorker<SendMail>("mail")
    .AddWorker("thumbnails", async (job, services, cancellationToken) =>
    {
        var images = services.GetRequiredService<Thumbnailer>();
        await images.RunAsync(job.Message.Decode<Upload>()!, cancellationToken);
    }, worker => worker.Concurrency = 8);

public sealed class SendMail(Mailer mailer) : IMessageHandler
{
    public async Task HandleAsync(Job job, CancellationToken cancellationToken)
    {
        var mail = job.Message.Decode<Mail>() ?? throw new PermanentFailureException("empty message");
        await mailer.SendAsync(mail, cancellationToken);
    }
}
```

- The handler is resolved for every message in a scope of its own, so scoped services
  (a `DbContext`) are fresh per message. `AddWorker<T>` registers it as scoped unless it
  is registered already.
- Worker options come from the section `Workers:<queue>` under the one given to
  `AddLessorMQ`, then from the `configure` argument.
- A handler that returns acks, one that throws fails, and a `PermanentFailureException`
  fails permanently, as above. The queue decides what follows, so leave retry policies
  (Polly, `Microsoft.Extensions.Http.Resilience`) out of handlers and out of the client's
  `HttpClient`: a retry inside a handler hides attempts from the server, and one around
  the client could repeat a publish. The client already retries what is safe to repeat.
- Stopping the host stops the workers as described above. Keep `WaitSeconds` plus your
  longest handler below the host's `ShutdownTimeout` (30 seconds by default); a process
  that ends mid-message leaves the lease to run out, and that counts as an attempt.

## Without the worker

```csharp
var lease = await client.ClaimAsync("mail", new ClaimOptions { WaitSeconds = 20 });
if (lease is not null)
{
    try
    {
        await SendAsync(lease.Message.Decode<Mail>()!);
    }
    catch (Exception e)
    {
        var result = await client.FailAsync(lease, e); // result.Outcome: retrying, dead_lettered or discarded
        return;
    }

    await client.AckAsync(lease);
}
```

`client.ExtendAsync(lease)` keeps a lease while a long job runs, and
`client.ReleaseAsync(lease)` hands a message back without counting the attempt. All of
them throw a `LeaseLostException` once the lease has ended: drop the message then, it is
someone else's now. `FailAsync` takes an exception, or a message of your own, and
`FailOptions` (`Permanent`, `Type`, `Details`) overrides what the exception says.

Many short jobs go faster in batches: `client.ClaimBatchAsync("mail", 50)` leases up to
50 ready messages at once (each with its own lease), and `client.AckBatchAsync(leases)`
acks them in one request, with an outcome per lease (`AckOutcome.Acked`,
`AckOutcome.LeaseLost`, ...). A message that fails is reported on its own with
`FailAsync`. `client.GetStatsAsync("mail")` returns the queue's counts, for monitoring.

## Errors

Everything the library throws is a `LessorMQException`. The server's errors are an
`ApiException` (with `Status`, `Code`, `Detail` and the invalid fields in `Errors`), or
one of its subclasses: `LeaseLostException`, `ValidationFailedException`,
`UnauthorizedException`, `ForbiddenException` (also for `tenant_suspended`),
`NotFoundException`, `InvalidStateException`, `PayloadTooLargeException`,
`RateLimitedException` (with `RetryAfter`) and `ServerException`. A server that cannot be
reached, or does not answer in time, gives a `TransportException`. A cancelled
`CancellationToken` gives an `OperationCanceledException`, as anywhere in .NET.

Network errors, timeouts, 5xx and 429 are retried (`Retries` 3, `RetryDelay` 100 ms times
the attempt, or the server's `Retry-After`), for every call that is safe to repeat. That
is the only retrying the library does. The `Log` option (or `ILogger`, in a host) shows
the retries.

`PermanentFailureException` is never thrown by the library: handlers throw it.

The rules behind all this are in the [protocol](https://lessormq.dev/docs/protocol), and
every request and answer in the [API reference](https://lessormq.dev/docs/api).

## Development

With the .NET 10 SDK:

```sh
cd clients/dotnet
dotnet test
```

The tests use a fake `HttpMessageHandler`, apart from one end-to-end test of a hosted
worker (an ack, a retry after the queue's backoff, a permanent failure) that runs against
a real server when `LESSORMQ_URL` and `LESSORMQ_OPERATOR_KEY` are set, in a tenant of its
own.

The library passes the LessorMQ [conformance suite](https://lessormq.dev/docs/writing-a-client)
through the console adapter in `conformance/LessorMQ.Conformance`, from the root of the
repository:

```sh
dotnet build -c Release clients/dotnet/conformance/LessorMQ.Conformance
LESSORMQ_ADAPTER_CMD="dotnet clients/dotnet/conformance/LessorMQ.Conformance/bin/Release/net10.0/LessorMQ.Conformance.dll" conformance/run.sh
```
