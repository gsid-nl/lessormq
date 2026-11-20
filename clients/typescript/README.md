# LessorMQ for TypeScript and JavaScript

Client and worker for [LessorMQ](https://lessormq.dev), the message queue over HTTP where
the server decides and clients only claim, run, ack or fail. Built on `fetch` with no
dependencies: Node 20.3+, Bun, Deno and browsers. Types come from the OpenAPI
description of the API.

```sh
npm install lessormq
```

## Publish

```ts
import { Client } from "lessormq";

const client = new Client("https://mq.example.com", process.env.LESSORMQ_KEY!);

await client.publish("mail", { to: "ada@example.com", template: "welcome" });

await client.publish("invoices", { invoice: 1042 }, {
  dedupKey: "invoice-1042", // one open message per key
  delaySeconds: 600,
  headers: { type: "InvoiceDue" },
});
```

Every publish carries a dedup key (your own, or one the client makes up), so the client
can safely retry it after a network error without creating a second message.

## Work

```ts
import { PermanentFailure, Worker } from "lessormq";

const controller = new AbortController();
process.on("SIGTERM", () => controller.abort());

await new Worker<{ to?: string }>(client, "mail", async (message, job) => {
  if (!message.body.to) {
    throw new PermanentFailure("no recipient");
  }
  await sendMail(message.body, { signal: job.signal });
}, { route: process.env.VPN_ID, signal: controller.signal }).run();
```

- The handler resolves: the message is acked.
- It throws or rejects: the message is failed with the error's name and text. Throw
  `PermanentFailure`, or any error with `permanent: true`, when the input itself is
  unusable.
- What happens after a failure is not up to the worker. The queue's settings decide
  whether and when the message comes back, and when it goes to the dead-letter set.
- While the handler runs, a timer extends the lease every third of it. When the lease
  is lost anyway, `job.signal` aborts and the result is not reported: the message is
  someone else's by then. A handler that blocks the event loop also blocks the timer.
- The `signal` option stops the worker after the current message. A claim that is
  already waiting is not cut off; it ends within `waitSeconds`, and a message it brings
  is given back.

`Worker` options: `consumer` (`$HOSTNAME` by default), `route`, `waitSeconds` (20),
`leaseSeconds`, `extendEvery`, `autoExtend`, `maxMessages`, `maxSeconds`,
`errorPauseMs`, `signal`, `logger`. Several workers can run side by side on one client.

## Without the worker

```ts
const lease = await client.claim("mail", { waitSeconds: 20 });
if (lease) {
  try {
    await sendMail(lease.message.body);
    await client.ack(lease);
  } catch (error) {
    const result = await client.fail(lease, error); // result.outcome: retrying, dead_lettered or discarded
  }
}
```

`client.extend(lease)` keeps a lease while a long job runs, and `client.release(lease)`
hands a message back without counting the attempt. All of them throw `LeaseLostError`
once the lease has ended: drop the message then, it is someone else's now. Every call
takes a `signal` to cancel it.

## Errors

Everything the library throws is a `LessorMQError`: `TransportError` when the server
cannot be reached, `ApiError` (with `status`, `code` and `errors`) when it answers with
a problem, in subclasses for the codes a client acts on: `LeaseLostError`,
`ValidationError`, `AuthenticationError`, `ForbiddenError`, `NotFoundError`,
`ConflictError`, `PayloadTooLargeError`, `RateLimitedError` and `ServerError`.

Network errors, 5xx and 429 are retried (`httpRetries: 3`, `retryDelayMs: 100` times the
attempt, or the server's `Retry-After`). That is the only retrying the library does.
Pass `logger: console` to see the retries.

## Development

```sh
npm install
npm test            # node --test, straight on the TypeScript sources
npm run typecheck
npm run generate    # src/openapi.ts from docs/openapi.yaml
npm run build       # dist/
```

The library passes the LessorMQ conformance suite through `test/conformance-adapter.ts`:

```sh
LESSORMQ_ADAPTER_CMD="node clients/typescript/test/conformance-adapter.ts" conformance/run.sh
```
