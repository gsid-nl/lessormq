/**
 * Runs the conformance scenarios through this library (conformance/php/src/ProcessAdapter.php).
 *
 * One JSON request per line on stdin, one JSON result per line on stdout. Every call goes
 * through the Client, and its results and errors are turned back into the {status, body}
 * the scenarios check:
 *
 *   LESSORMQ_ADAPTER_CMD="node clients/typescript/test/conformance-adapter.ts" conformance/run.sh
 */

import { createInterface } from "node:readline";

import { ApiError, Client, type Lease } from "../src/index.ts";
import { formatTime } from "../src/models.ts";

type Args = Record<string, any>;
type Result = { status: number; body: unknown };

const url = process.env.LESSORMQ_URL ?? "http://127.0.0.1:18080";
const clients = new Map<string, Client>();

async function call(name: string, apiKey: string, args: Args): Promise<Result> {
  let client = clients.get(apiKey);
  if (!client) {
    client = new Client(url, apiKey, { retryDelayMs: 50 });
    clients.set(apiKey, client);
  }
  try {
    switch (name) {
      case "publish": {
        const published = await client.publish(args.queue, args.body ?? null, {
          ...(args.headers ? { headers: args.headers } : {}),
          ...(args.dedup_key !== undefined ? { dedupKey: args.dedup_key } : {}),
          ...(args.delay_seconds !== undefined ? { delaySeconds: args.delay_seconds } : {}),
          ...(args.available_at !== undefined ? { availableAt: new Date(args.available_at) } : {}),
          ...(args.max_attempts !== undefined ? { maxAttempts: args.max_attempts } : {}),
        });
        return {
          status: published.duplicate ? 200 : 201,
          body: {
            id: published.id,
            queue: published.queue,
            status: published.status,
            created_at: formatTime(published.createdAt),
            available_at: formatTime(published.availableAt),
            duplicate: published.duplicate,
          },
        };
      }
      case "claim": {
        const lease = await client.claim(args.queue, claimOptions(args));
        if (lease === null) return { status: 204, body: null };
        return { status: 200, body: leaseBody(lease) };
      }
      case "claim_batch": {
        const leases = await client.claimBatch(args.queue, Number(args.max_messages), claimOptions(args));
        if (leases.length === 0) return { status: 204, body: null };
        return { status: 200, body: { items: leases.map(leaseBody) } };
      }
      case "ack_batch": {
        const items: Args[] = args.leases ?? [];
        const outcomes = await client.ackBatch(items.map(leaseOf));
        return { status: 200, body: { items: items.map((item, i) => ({ message: item.message, outcome: outcomes[i] })) } };
      }
      case "extend": {
        const lease = await client.extend(leaseOf(args), args.lease_seconds);
        return { status: 200, body: { lease_until: formatTime(lease.leaseUntil) } };
      }
      case "ack":
        await client.ack(leaseOf(args));
        return { status: 204, body: null };
      case "release":
        await client.release(leaseOf(args), args.delay_seconds ?? 0);
        return { status: 204, body: null };
      case "fail": {
        const error = args.error ?? {};
        const result = await client.fail(leaseOf(args), String(error.message ?? ""), {
          permanent: Boolean(args.permanent),
          ...(error.details !== undefined ? { details: error.details } : {}),
          ...(error.type !== undefined ? { type: error.type } : {}),
        });
        return {
          status: 200,
          body: {
            outcome: result.outcome,
            available_at: result.availableAt ? formatTime(result.availableAt) : null,
            attempt: result.attempt,
            max_attempts: result.maxAttempts,
          },
        };
      }
      default:
        throw new Error(`unknown call ${name}`);
    }
  } catch (error) {
    if (error instanceof ApiError) {
      return { status: error.status, body: { code: error.code, detail: error.detail, errors: error.errors } };
    }
    throw error;
  }
}

/** A lease as the scenario names it: the message id and the receipt, which may be stale or someone else's on purpose. */
function claimOptions(args: Args) {
  return {
    ...(args.consumer !== undefined ? { consumer: args.consumer } : {}),
    ...(args.route !== undefined ? { route: args.route } : {}),
    ...(args.wait_seconds !== undefined ? { waitSeconds: args.wait_seconds } : {}),
    ...(args.lease_seconds !== undefined ? { leaseSeconds: args.lease_seconds } : {}),
  };
}

function leaseBody(lease: Lease) {
  const { message } = lease;
  return {
    receipt: lease.receipt,
    lease_until: formatTime(lease.leaseUntil),
    message: {
      id: message.id,
      queue: message.queue,
      body: message.body,
      headers: message.headers,
      attempt: message.attempt,
      max_attempts: message.maxAttempts,
      created_at: formatTime(message.createdAt),
    },
  };
}

function leaseOf(args: Args): Lease {
  const now = new Date();
  return {
    receipt: String(args.receipt),
    leaseUntil: now,
    message: { id: String(args.message), queue: "", body: null, headers: {}, attempt: 0, maxAttempts: 0, createdAt: now, isLastAttempt: false },
  };
}

for await (const line of createInterface({ input: process.stdin })) {
  if (line.trim() === "") continue;
  const request = JSON.parse(line) as { call: string; api_key: string; args: Args | unknown[] };
  const args = Array.isArray(request.args) ? {} : request.args;
  process.stdout.write(JSON.stringify(await call(request.call, request.api_key, args)) + "\n");
}
