import assert from "node:assert/strict";
import { test } from "node:test";

import { Client, PermanentFailure, Worker, type Message, type WorkerOptions } from "../src/index.ts";
import { FAIL_RESULT, Fake, LEASE } from "./fake.ts";

const SLEEP = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function worker(fake: Fake, handler: ConstructorParameters<typeof Worker>[2], options: WorkerOptions = {}): Worker {
  const client = new Client("https://mq.test", "k", { retryDelayMs: 0, fetch: fake.fetch });
  return new Worker(client, "mail", handler, { consumer: "w1", waitSeconds: 1, maxMessages: 1, ...options });
}

test("a handler that resolves acks", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/ack", [204, null]);
  const seen: Message[] = [];

  const handled = await worker(fake, async (message) => {
    seen.push(message);
  }).run();

  assert.equal(handled, 1);
  assert.deepEqual(seen[0]!.body, { to: "ada@example.com" });
  assert.deepEqual(fake.calls("/v1/messages/m1/ack"), [{ receipt: "r1" }]);
  assert.deepEqual(fake.calls("/v1/queues/mail/claims")[0], { consumer: "w1", wait_seconds: 1 });
});

test("a handler that throws fails, and the worker goes on", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/fail", [200, FAIL_RESULT]);

  const handled = await worker(fake, () => {
    throw new PermanentFailure("no recipient");
  }, { maxMessages: 2 }).run();

  assert.equal(handled, 2);
  const sent = fake.calls("/v1/messages/m1/fail") as { permanent: boolean; error: { type: string } }[];
  assert.equal(sent.length, 2);
  assert.equal(sent[0]!.permanent, true);
  assert.equal(sent[0]!.error.type, "PermanentFailure");
});

test("the lease is extended while the handler runs", async () => {
  const fake = new Fake()
    .on("/v1/queues/mail/claims", [200, LEASE])
    .on("/v1/messages/m1/extend", [200, { lease_until: "2030-01-01T00:01:00Z" }])
    .on("/v1/messages/m1/ack", [204, null]);

  await worker(fake, () => SLEEP(350), { extendEvery: 0.1, leaseSeconds: 60 }).run();

  const extends_ = fake.calls("/v1/messages/m1/extend");
  assert.ok(extends_.length >= 2 && extends_.length <= 4, `extended ${extends_.length} times`);
  assert.deepEqual(extends_[0], { receipt: "r1", lease_seconds: 60 });
  assert.equal(fake.calls("/v1/messages/m1/ack").length, 1);
});

test("a lost lease aborts the job's signal and is not reported", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/extend", [409, { code: "lease_lost" }]);
  let aborted = false;

  await worker(fake, async (message, job) => {
    await new Promise<void>((resolve) => job.signal.addEventListener("abort", () => resolve(), { once: true }));
    aborted = job.signal.aborted;
  }, { extendEvery: 0.05 }).run();

  assert.equal(aborted, true);
  assert.deepEqual(fake.calls("/v1/messages/m1/ack"), []);
  assert.deepEqual(fake.calls("/v1/messages/m1/fail"), []);
});

test("a lost ack does not stop the worker", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/ack", [409, { code: "lease_lost" }]);

  assert.equal(await worker(fake, () => {}, { maxMessages: 2 }).run(), 2);
});

test("a claim failure pauses and goes on", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [503, { code: "internal" }], [200, LEASE]).on("/v1/messages/m1/ack", [204, null]);
  const client = new Client("https://mq.test", "k", { retryDelayMs: 0, httpRetries: 0, fetch: fake.fetch });

  const started = Date.now();
  const handled = await new Worker(client, "mail", () => {}, { errorPauseMs: 200, maxMessages: 1 }).run();

  assert.equal(handled, 1);
  assert.ok(Date.now() - started >= 190);
});

test("aborting during a claim gives the message back", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/release", [204, null]);
  const controller = new AbortController();
  fake.onRequest = (path) => {
    if (path.endsWith("/claims")) controller.abort();
  };
  const ran: Message[] = [];

  const handled = await worker(fake, (message) => {
    ran.push(message);
  }, { signal: controller.signal, maxMessages: undefined }).run();

  assert.equal(handled, 0);
  assert.deepEqual(ran, []);
  assert.deepEqual(fake.calls("/v1/messages/m1/release"), [{ receipt: "r1" }]);
});

test("an aborted signal wakes a pausing worker", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [503, { code: "internal" }]);
  const client = new Client("https://mq.test", "k", { httpRetries: 0, fetch: fake.fetch });
  const controller = new AbortController();
  setTimeout(() => controller.abort(), 50);

  const started = Date.now();
  await new Worker(client, "mail", () => {}, { errorPauseMs: 10_000, signal: controller.signal }).run();

  assert.ok(Date.now() - started < 1000);
});

test("maxSeconds ends the run", async () => {
  const fake = new Fake().on("/v1/queues/mail/claims", [204, null]);

  const started = Date.now();
  await worker(fake, () => {}, { maxMessages: undefined, maxSeconds: 0.2 }).run();

  assert.ok(Date.now() - started < 2000);
});
