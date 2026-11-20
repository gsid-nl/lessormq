import assert from "node:assert/strict";
import { test } from "node:test";

import {
  Client,
  LeaseLostError,
  PermanentFailure,
  RateLimitedError,
  ServerError,
  TransportError,
  ValidationError,
  type ClientOptions,
} from "../src/index.ts";
import { FAIL_RESULT, Fake, LEASE, PUBLISHED } from "./fake.ts";

function client(fake: Fake, options: ClientOptions = {}): Client {
  return new Client("https://mq.test/", "lmq_key", { retryDelayMs: 0, fetch: fake.fetch, ...options });
}

test("publish sends the body and an automatic dedup key", async () => {
  const fake = new Fake().on("/v1/queues/mail/messages", [201, PUBLISHED]);

  const published = await client(fake).publish("mail", { to: "ada@example.com" }, { headers: { type: "Welcome" }, maxAttempts: 3 });

  const sent = fake.calls("/v1/queues/mail/messages")[0]!;
  assert.deepEqual(sent.body, { to: "ada@example.com" });
  assert.deepEqual(sent.headers, { type: "Welcome" });
  assert.equal(sent.max_attempts, 3);
  assert.match(String(sent.dedup_key), /^lmq-auto-[0-9a-f]{24}$/);
  assert.equal("delay_seconds" in sent, false);
  assert.equal(published.duplicate, false);
  assert.deepEqual(published.availableAt, new Date("2030-01-01T00:00:00Z"));
  const headers = fake.requests[0]!.init.headers as Record<string, string>;
  assert.equal(headers.Authorization, "Bearer lmq_key");
  assert.match(headers["User-Agent"]!, /^lessormq-js\//);
});

test("publish keeps null bodies, encodes queue names and writes times in UTC", async () => {
  const fake = new Fake().on("/v1/queues/q%2F1/messages", [201, PUBLISHED]);

  await client(fake).publish("q/1", null, { dedupKey: "k1", availableAt: new Date("2030-01-01T02:00:00.500Z") });

  assert.deepEqual(fake.calls("/v1/queues/q%2F1/messages")[0], {
    body: null,
    dedup_key: "k1",
    available_at: "2030-01-01T02:00:00Z",
  });
});

test("a publish without a dedup key is not retried", async () => {
  const fake = new Fake().on("/v1/queues/mail/messages", [503, { code: "internal" }], [201, PUBLISHED]);

  await assert.rejects(client(fake, { autoDedupKey: false }).publish("mail", {}), ServerError);
  assert.equal(fake.requests.length, 1);
});

test("claim returns null when nothing arrived", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [204, null]);

  assert.equal(await client(fake).claim("mail", { waitSeconds: 5, route: "vpn-1", consumer: "w1" }), null);
  assert.deepEqual(fake.calls("/v1/queues/mail/claims")[0], { consumer: "w1", route: "vpn-1", wait_seconds: 5 });
});

test("claim returns the lease", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [200, LEASE]);

  const lease = await client(fake).claim<{ to: string }>("mail");

  assert.ok(lease);
  assert.equal(lease.receipt, "r1");
  assert.equal(lease.message.body.to, "ada@example.com");
  assert.deepEqual(lease.message.headers, { type: "Welcome" });
  assert.equal(lease.message.maxAttempts, 7);
  assert.equal(lease.message.isLastAttempt, false);
  assert.deepEqual(lease.leaseUntil, new Date("2030-01-01T00:00:30Z"));
});

test("extend moves the lease", async () => {
  const fake = new Fake()
    .on("/v1/queues/mail/claims", [200, LEASE])
    .on("/v1/messages/m1/extend", [200, { lease_until: "2030-01-01T00:05:00Z" }]);
  const c = client(fake);
  const lease = (await c.claim("mail"))!;

  await c.extend(lease, 300);

  assert.deepEqual(fake.calls("/v1/messages/m1/extend")[0], { receipt: "r1", lease_seconds: 300 });
  assert.deepEqual(lease.leaseUntil, new Date("2030-01-01T00:05:00Z"));
});

test("fail reports the error, its name and where it was thrown", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/fail", [200, FAIL_RESULT]);
  const c = client(fake);
  const lease = (await c.claim("mail"))!;

  const result = await c.fail(lease, new TypeError("mail server slow"));

  const sent = fake.calls("/v1/messages/m1/fail")[0] as { error: Record<string, unknown>; permanent: boolean };
  assert.equal(sent.permanent, false);
  assert.equal(sent.error.type, "TypeError");
  assert.equal(sent.error.message, "mail server slow");
  assert.match(String((sent.error.details as { file: string }).file), /client\.test\.ts$/);
  assert.equal(result.outcome, "retrying");
  assert.deepEqual(result.availableAt, new Date("2030-01-01T00:00:10Z"));
});

test("fail marks permanent failures, from the class or a flag", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/fail", [200, FAIL_RESULT]);
  const c = client(fake);
  const lease = (await c.claim("mail"))!;

  await c.fail(lease, new PermanentFailure("no recipient"));
  await c.fail(lease, Object.assign(new Error("bad json"), { permanent: true }));
  await c.fail(lease, "plain text", { details: { step: 2 }, type: "Custom" });

  const [first, second, third] = fake.calls("/v1/messages/m1/fail") as { error: Record<string, unknown>; permanent: boolean }[];
  assert.equal(first!.permanent, true);
  assert.equal(first!.error.type, "PermanentFailure");
  assert.equal(second!.permanent, true);
  assert.deepEqual(third, { receipt: "r1", error: { message: "plain text", type: "Custom", details: { step: 2 } }, permanent: false });
});

test("a lost lease is thrown", async () => {
  const fake = new Fake()
    .on("/v1/queues/mail/claims", [200, LEASE])
    .on("/v1/messages/m1/ack", [409, { code: "lease_lost", detail: "The lease ended." }]);
  const c = client(fake);
  const lease = (await c.claim("mail"))!;

  await assert.rejects(c.ack(lease), (error: unknown) => {
    assert.ok(error instanceof LeaseLostError);
    assert.equal(error.status, 409);
    assert.equal(error.message, "The lease ended.");
    return true;
  });
});

test("server and network errors are retried", async () => {
  const fake = new Fake().on(
    "/v1/queues/mail/claims",
    new TypeError("fetch failed"),
    new Response("<html>Bad gateway</html>", { status: 502 }),
    [200, LEASE],
  );

  assert.ok(await client(fake).claim("mail"));
  assert.equal(fake.requests.length, 3);
});

test("retries run out", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", new TypeError("fetch failed"));

  await assert.rejects(client(fake, { httpRetries: 2 }).claim("mail"), TransportError);
  assert.equal(fake.requests.length, 3);
});

test("a rate limit waits for Retry-After", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [429, { code: "rate_limited" }, { "Retry-After": "0" }]);

  await assert.rejects(client(fake, { httpRetries: 1 }).claim("mail"), (error: unknown) => {
    assert.ok(error instanceof RateLimitedError);
    assert.equal(error.retryAfter, 0);
    return true;
  });
  assert.equal(fake.requests.length, 2);
});

test("validation errors carry the fields and are not retried", async () => {
  const errors = [{ field: "lease_seconds", message: "too long" }];
  const fake = new Fake().on("/v1/queues/mail/claims", [400, { code: "validation_failed", errors }]);

  await assert.rejects(client(fake).claim("mail", { leaseSeconds: 99999 }), (error: unknown) => {
    assert.ok(error instanceof ValidationError);
    assert.deepEqual(error.errors, errors);
    return true;
  });
  assert.equal(fake.requests.length, 1);
});

test("an aborted call is not retried", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", new DOMException("aborted", "AbortError"));
  const controller = new AbortController();
  controller.abort();

  await assert.rejects(client(fake).claim("mail", { signal: controller.signal }));
  assert.equal(fake.requests.length, 1);
});

test("stats", async () => {
  const stats = { ready: 3, delayed: 1, processing: 2, completed: 9, failed: 1, oldest_ready_seconds: 12, oldest_processing_seconds: 4 };
  const fake = new Fake().on("/v1/queues/mail", [200, { name: "mail", stats }]);

  const result = await client(fake).stats("mail");

  assert.equal(result.ready, 3);
  assert.equal(result.oldestReadySeconds, 12);
});

test("the key stays out of the client's string", () => {
  assert.equal(String(client(new Fake())).includes("lmq_key"), false);
});
