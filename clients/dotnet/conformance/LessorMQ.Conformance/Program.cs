// Runs the LessorMQ conformance scenarios through this library
// (conformance/php/src/ProcessAdapter.php in the server repository).
//
// One JSON request per line on stdin, one JSON result per line on stdout. Every call goes
// through LessorMQClient, and its results and exceptions are turned back into the
// {status, body} the scenarios check:
//
//   dotnet build -c Release clients/dotnet/conformance/LessorMQ.Conformance
//   LESSORMQ_ADAPTER_CMD="dotnet clients/dotnet/conformance/LessorMQ.Conformance/bin/Release/net10.0/LessorMQ.Conformance.dll" \
//       conformance/run.sh
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using LessorMQ;

namespace LessorMQ.Conformance;

internal static class Program
{
    private static readonly string Url = Environment.GetEnvironmentVariable("LESSORMQ_URL") is { Length: > 0 } url
        ? url
        : "http://127.0.0.1:18080";

    private static readonly Dictionary<string, LessorMQClient> Clients = [];

    public static async Task<int> Main()
    {
        using var input = new StreamReader(Console.OpenStandardInput(), new UTF8Encoding(false));
        using var output = new StreamWriter(Console.OpenStandardOutput(), new UTF8Encoding(false)) { AutoFlush = true };
        while (await input.ReadLineAsync() is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var request = JsonNode.Parse(line)!.AsObject();
            var call = (string)request["call"]!;
            var apiKey = (string)request["api_key"]!;
            // The runner sends [] for a step without arguments.
            var args = request["args"] as JsonObject ?? [];
            JsonObject result;
            try
            {
                result = await CallAsync(ClientFor(apiKey), call, args);
            }
            catch (ApiException e)
            {
                var errors = new JsonArray();
                foreach (var error in e.Errors)
                {
                    errors.Add(new JsonObject { ["field"] = error.Field, ["message"] = error.Message });
                }

                result = Result(e.Status, new JsonObject { ["code"] = e.Code, ["detail"] = e.Detail, ["errors"] = errors });
            }
            catch (Exception e)
            {
                await Console.Error.WriteLineAsync($"{call}: {e}");
                return 1;
            }

            await output.WriteLineAsync(result.ToJsonString());
        }

        return 0;
    }

    private static LessorMQClient ClientFor(string apiKey)
    {
        if (!Clients.TryGetValue(apiKey, out var client))
        {
            client = new LessorMQClient(new LessorMQClientOptions
            {
                Url = Url,
                ApiKey = apiKey,
                RetryDelay = TimeSpan.FromMilliseconds(50),
            });
            Clients[apiKey] = client;
        }

        return client;
    }

    private static async Task<JsonObject> CallAsync(LessorMQClient client, string call, JsonObject args)
    {
        switch (call)
        {
            case "publish":
                var published = await client.PublishAsync(Text(args, "queue")!, args["body"]?.DeepClone(), new PublishOptions
                {
                    Headers = Headers(args),
                    DedupKey = Text(args, "dedup_key"),
                    DelaySeconds = Number(args, "delay_seconds"),
                    AvailableAt = Text(args, "available_at") is { } at ? DateTimeOffset.Parse(at, CultureInfo.InvariantCulture) : null,
                    MaxAttempts = Number(args, "max_attempts"),
                });
                return Result(published.Duplicate ? 200 : 201, new JsonObject
                {
                    ["id"] = published.Id,
                    ["queue"] = published.Queue,
                    ["status"] = published.Status,
                    ["created_at"] = Format(published.CreatedAt),
                    ["available_at"] = Format(published.AvailableAt),
                    ["duplicate"] = published.Duplicate,
                });

            case "claim":
                var lease = await client.ClaimAsync(Text(args, "queue")!, ClaimOptionsOf(args));
                return lease is null ? Result(204, null) : Result(200, LeaseBody(lease));

            case "claim_batch":
                var leases = await client.ClaimBatchAsync(Text(args, "queue")!, Number(args, "max_messages"), ClaimOptionsOf(args));
                if (leases.Count == 0)
                {
                    return Result(204, null);
                }

                return Result(200, new JsonObject { ["items"] = new JsonArray([.. leases.Select(LeaseBody)]) });

            case "ack_batch":
                var given = (args["leases"] as JsonArray ?? []).Select(item => item!.AsObject()).ToList();
                var outcomes = await client.AckBatchAsync(given.Select(LeaseOf).ToList());
                var items = new JsonArray();
                for (var i = 0; i < given.Count; i++)
                {
                    items.Add(new JsonObject { ["message"] = Text(given[i], "message"), ["outcome"] = outcomes[i] });
                }

                return Result(200, new JsonObject { ["items"] = items });

            case "extend":
                var extended = LeaseOf(args);
                await client.ExtendAsync(extended, Number(args, "lease_seconds"));
                return Result(200, new JsonObject { ["lease_until"] = Format(extended.Until) });

            case "ack":
                await client.AckAsync(LeaseOf(args));
                return Result(204, null);

            case "release":
                await client.ReleaseAsync(LeaseOf(args), Number(args, "delay_seconds"));
                return Result(204, null);

            case "fail":
                var error = args["error"] as JsonObject ?? [];
                var failed = await client.FailAsync(LeaseOf(args), Text(error, "message") ?? "", new FailOptions
                {
                    Permanent = args["permanent"]?.GetValue<bool>() ?? false,
                    Type = Text(error, "type"),
                    Details = error["details"]?.DeepClone(),
                });
                return Result(200, new JsonObject
                {
                    ["outcome"] = failed.Outcome,
                    ["available_at"] = failed.AvailableAt is { } availableAt ? Format(availableAt) : null,
                    ["attempt"] = failed.Attempt,
                    ["max_attempts"] = failed.MaxAttempts,
                });
        }

        throw new InvalidOperationException("unknown call " + call);
    }

    private static ClaimOptions ClaimOptionsOf(JsonObject args) => new()
    {
        Consumer = Text(args, "consumer"),
        Route = Text(args, "route"),
        WaitSeconds = Number(args, "wait_seconds"),
        LeaseSeconds = Number(args, "lease_seconds"),
    };

    // A lease as the scenario names it: the message id and the receipt, which may be stale
    // or someone else's on purpose.
    private static Lease LeaseOf(JsonObject args) =>
        new(Text(args, "receipt") ?? "", DateTimeOffset.UtcNow, new Message { Id = Text(args, "message") ?? "" });

    private static JsonObject LeaseBody(Lease lease)
    {
        var message = lease.Message;
        var headers = new JsonObject();
        foreach (var (name, value) in message.Headers)
        {
            headers[name] = value;
        }

        return new JsonObject
        {
            ["receipt"] = lease.Receipt,
            ["lease_until"] = Format(lease.Until),
            ["message"] = new JsonObject
            {
                ["id"] = message.Id,
                ["queue"] = message.Queue,
                ["body"] = JsonSerializer.SerializeToNode(message.Body),
                ["headers"] = headers,
                ["attempt"] = message.Attempt,
                ["max_attempts"] = message.MaxAttempts,
                ["created_at"] = Format(message.CreatedAt),
            },
        };
    }

    private static Dictionary<string, string>? Headers(JsonObject args)
    {
        if (args["headers"] is not JsonObject headers)
        {
            return null;
        }

        return headers.ToDictionary(header => header.Key, header => header.Value?.ToString() ?? "");
    }

    private static string? Text(JsonObject args, string name) =>
        args[name] is JsonValue value && value.TryGetValue<string>(out var text) ? text : null;

    private static int Number(JsonObject args, string name) =>
        args[name] is JsonValue value && value.TryGetValue<int>(out var number) ? number : 0;

    private static JsonObject Result(int status, JsonNode? body) => new() { ["status"] = status, ["body"] = body };

    private static string Format(DateTimeOffset time) =>
        time.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
}
