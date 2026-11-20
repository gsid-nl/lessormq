// Publish three messages and work them off with the LessorMQ worker.
// LESSORMQ_URL and LESSORMQ_KEY (publish and consume scopes); `npm install lessormq`.
// Runs on Node 22.6+ (`node worker.ts`), Bun and Deno.

import { Client, Worker } from "lessormq";

const client = new Client(process.env.LESSORMQ_URL ?? "http://127.0.0.1:18080", process.env.LESSORMQ_KEY!);
const queue = process.env.LESSORMQ_QUEUE ?? "examples";

for (const body of [{ n: 1 }, { n: 2, fail_once: true }, { n: 3 }]) {
  console.log("published", (await client.publish(queue, body)).id);
}

const controller = new AbortController();
process.on("SIGTERM", () => controller.abort());

let handled = 0;
const worker = new Worker<{ n: number; fail_once?: boolean }>(client, queue, async (message) => {
  const { n, fail_once } = message.body;
  if (fail_once && message.attempt === 1) {
    console.log(`n=${n} attempt=${message.attempt} failed`);
    throw new Error("temporarily unavailable");
  }

  console.log(`n=${n} attempt=${message.attempt} handled`);
  if (++handled === 3) controller.abort();
}, { consumer: "ts-example", waitSeconds: 5, maxSeconds: 60, signal: controller.signal });

await worker.run();
console.log(`done: ${handled} handled`);
process.exit(handled === 3 ? 0 : 1);
