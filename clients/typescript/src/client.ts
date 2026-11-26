/**
 * The six protocol calls of LessorMQ (docs/protocol.md), nothing more.
 *
 * The client never decides what happens to a message: no retry of a failed job, no
 * delay, no dead-lettering. It reports, and the queue decides. It does retry its own HTTP
 * calls when that is safe: network errors, 5xx and 429, for every call that can be
 * repeated (all of them, given the automatic dedup key on publish).
 *
 * Built on fetch, so it runs on Node 20+, Bun, Deno and in browsers.
 */

import { ApiError, isPermanent, RateLimitedError, ServerError, TransportError } from "./errors.ts";
import {
  failResultFrom,
  formatTime,
  leaseFrom,
  publishedFrom,
  statsFrom,
  type FailResult,
  type Lease,
  type Published,
  type Stats,
} from "./models.ts";
import type { components } from "./openapi.ts";
import { VERSION } from "./version.ts";

type Schemas = components["schemas"];

/** Sent with every request unless `userAgent` says otherwise: lessormq-js/0.4.10. */
export const USER_AGENT = `lessormq-js/${VERSION}`;

/** Where the library logs its own retries and a worker's troubles. `console` fits. */
export interface Logger {
  warn(message: string, context?: Record<string, unknown>): void;
  error(message: string, context?: Record<string, unknown>): void;
  info?(message: string, context?: Record<string, unknown>): void;
}

export interface ClientOptions {
  /** Retries after a network error, a 5xx or a 429, for calls that are safe to repeat. Default 3. */
  httpRetries?: number;
  /** Base delay between retries in milliseconds, times the attempt number. Default 100. */
  retryDelayMs?: number;
  /** Give every publish without a dedup key one of its own, so a retried publish can never create a second message. Default true. */
  autoDedupKey?: boolean;
  /** Milliseconds per request; a claim gets its wait on top. Default 10000. */
  timeoutMs?: number;
  userAgent?: string;
  /** Your own fetch (a proxy agent, a test double). Default the global one. */
  fetch?: typeof fetch;
  logger?: Logger;
}

export interface CallOptions {
  /** Cancels the request. An aborted claim may still have leased a message on the server; that lease runs out and counts as an attempt. */
  signal?: AbortSignal;
}

export interface PublishOptions extends CallOptions {
  headers?: Record<string, string>;
  /** One open message per key: a publish with the key of an open message returns that one. */
  dedupKey?: string;
  delaySeconds?: number;
  availableAt?: Date;
  /** Attempts for this message, within the queue's max_attempts_limit. */
  maxAttempts?: number;
}

export interface ClaimOptions extends CallOptions {
  /** Shown to administrators. */
  consumer?: string;
  /** The path this worker runs jobs through (a VPN exit, a proxy, an API account). */
  route?: string;
  /** Wait up to this long for a message (at most 20). */
  waitSeconds?: number;
  /** Lease for this claim; the queue's when left out. */
  leaseSeconds?: number;
}

/** What became of one lease of ackBatch(): acked (also when acked before), lease_lost, not_found or forbidden. */
export type AckOutcome = Schemas["AckBatchResponse"]["items"][number]["outcome"];

export interface FailOptions extends CallOptions {
  /** The input itself cannot be processed; by default, whether the error is permanent (see isPermanent). */
  permanent?: boolean;
  /** Any JSON value with more context; by default where the error was thrown. */
  details?: unknown;
  /** The error type; by default the error's name. */
  type?: string;
}

interface Call {
  method: "GET" | "POST";
  path: string;
  payload?: unknown;
  /** Whether it may be sent again after a failure that leaves its effect unknown. */
  repeatable: boolean;
  extraTimeoutMs?: number;
  signal?: AbortSignal | undefined;
}

const MAX_ERROR_MESSAGE = 65535;
const MAX_ERROR_TYPE = 255;

export class Client {
  readonly #baseUrl: string;
  readonly #apiKey: string;
  readonly #httpRetries: number;
  readonly #retryDelayMs: number;
  readonly #autoDedupKey: boolean;
  readonly #timeoutMs: number;
  readonly #userAgent: string;
  readonly #fetch: typeof fetch;
  readonly logger: Logger | undefined;

  constructor(baseUrl: string, apiKey: string, options: ClientOptions = {}) {
    this.#baseUrl = baseUrl.replace(/\/+$/, "");
    this.#apiKey = apiKey;
    this.#httpRetries = options.httpRetries ?? 3;
    this.#retryDelayMs = options.retryDelayMs ?? 100;
    this.#autoDedupKey = options.autoDedupKey ?? true;
    this.#timeoutMs = options.timeoutMs ?? 10_000;
    this.#userAgent = options.userAgent ?? USER_AGENT;
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.logger = options.logger;
  }

  /**
   * Put a message on a queue. With a dedup key (yours, or the automatic one), an open
   * message with that key is returned instead of a new one: `published.duplicate`.
   */
  async publish(queue: string, body: unknown, options: PublishOptions = {}): Promise<Published> {
    const dedupKey = options.dedupKey ?? (this.#autoDedupKey ? `lmq-auto-${randomHex(12)}` : undefined);
    const payload: Schemas["PublishRequest"] = { body: body ?? null };
    if (options.headers && Object.keys(options.headers).length > 0) payload.headers = options.headers;
    if (dedupKey !== undefined) payload.dedup_key = dedupKey;
    if (options.delaySeconds !== undefined) payload.delay_seconds = options.delaySeconds;
    if (options.availableAt !== undefined) payload.available_at = formatTime(options.availableAt);
    if (options.maxAttempts !== undefined) payload.max_attempts = options.maxAttempts;

    // Without a dedup key a repeated publish could create a second message.
    const response = await this.#send({
      method: "POST",
      path: `/v1/queues/${segment(queue)}/messages`,
      payload,
      repeatable: dedupKey !== undefined,
      signal: options.signal,
    });
    return publishedFrom(response.body as Schemas["PublishResponse"]);
  }

  /**
   * Lease the next message of a queue, waiting up to `waitSeconds` for one. Null when
   * nothing became available, or the queue is paused.
   */
  async claim<Body = unknown>(queue: string, options: ClaimOptions = {}): Promise<Lease<Body> | null> {
    const response = await this.#claim(queue, options);
    return response.status === 204 ? null : leaseFrom<Body>(response.body as Schemas["Lease"]);
  }

  /**
   * Lease up to `maxMessages` (1 to 100) ready messages at once, oldest first, each with a
   * receipt of its own. Waits up to `waitSeconds` only while there is none; an empty list
   * when nothing became available. Report each lease as usual, or ack them together with
   * ackBatch().
   */
  async claimBatch<Body = unknown>(queue: string, maxMessages: number, options: ClaimOptions = {}): Promise<Lease<Body>[]> {
    const response = await this.#claim(queue, options, maxMessages);
    if (response.status === 204) return [];
    return (response.body as Schemas["LeaseBatch"]).items.map((item) => leaseFrom<Body>(item));
  }

  /**
   * Report success for up to 100 leases in one request. The outcomes are in the order of
   * `leases`; one that cannot be acked does not hold up the others.
   */
  async ackBatch(leases: Lease[], options: CallOptions = {}): Promise<AckOutcome[]> {
    const payload: Schemas["AckBatchRequest"] = {
      leases: leases.map((lease) => ({ message: lease.message.id, receipt: lease.receipt })),
    };
    const response = await this.#send({ method: "POST", path: "/v1/acks", payload, repeatable: true, signal: options.signal });
    const items = (response.body as Schemas["AckBatchResponse"]).items;
    return leases.map((_, i) => items[i]?.outcome ?? "not_found");
  }

  async #claim(queue: string, options: ClaimOptions, maxMessages?: number) {
    const waitSeconds = Math.max(0, options.waitSeconds ?? 0);
    const payload: Schemas["ClaimRequest"] = {};
    if (options.consumer !== undefined) payload.consumer = options.consumer;
    if (options.route !== undefined) payload.route = options.route;
    if (waitSeconds > 0) payload.wait_seconds = waitSeconds;
    if (options.leaseSeconds !== undefined) payload.lease_seconds = options.leaseSeconds;
    if (maxMessages !== undefined) payload.max_messages = maxMessages;

    // A claim whose answer got lost leaves a lease that simply expires and counts as an
    // attempt, so claims are retried like the rest.
    return this.#send({
      method: "POST",
      path: `/v1/queues/${segment(queue)}/claims`,
      payload,
      repeatable: true,
      extraTimeoutMs: waitSeconds * 1000,
      signal: options.signal,
    });
  }

  /**
   * Move the end of the lease to now plus `leaseSeconds` (or the queue's lease).
   *
   * @throws LeaseLostError when the lease already ended
   */
  async extend<Body>(lease: Lease<Body>, leaseSeconds?: number, options: CallOptions = {}): Promise<Lease<Body>> {
    const payload: Schemas["ExtendRequest"] = { receipt: lease.receipt };
    if (leaseSeconds !== undefined) payload.lease_seconds = leaseSeconds;
    const response = await this.#send({
      method: "POST",
      path: messagePath(lease, "extend"),
      payload,
      repeatable: true,
      signal: options.signal,
    });
    lease.leaseUntil = new Date((response.body as Schemas["ExtendResponse"]).lease_until);
    return lease;
  }

  /**
   * Report success.
   *
   * @throws LeaseLostError when the lease already ended
   */
  async ack(lease: Lease, options: CallOptions = {}): Promise<void> {
    await this.#send({
      method: "POST",
      path: messagePath(lease, "ack"),
      payload: { receipt: lease.receipt },
      repeatable: true,
      signal: options.signal,
    });
  }

  /**
   * Report failure. The server decides what follows and says so in the result.
   *
   * @param error the error, or a message
   * @throws LeaseLostError when the lease already ended
   */
  async fail(lease: Lease, error: unknown, options: FailOptions = {}): Promise<FailResult> {
    const thrown = error instanceof Error ? error : null;
    const text = thrown ? thrown.message : typeof error === "string" ? error : String(error);
    const report: Schemas["ErrorReport"] = { message: text.slice(0, MAX_ERROR_MESSAGE) || "(no message)" };
    const type = options.type ?? (thrown ? thrown.name || thrown.constructor.name : undefined);
    if (type !== undefined) report.type = type.slice(0, MAX_ERROR_TYPE);
    const details = options.details !== undefined ? options.details : thrown ? origin(thrown) : undefined;
    if (details !== undefined) report.details = details;

    const payload: Schemas["FailRequest"] = {
      receipt: lease.receipt,
      error: report,
      permanent: options.permanent ?? isPermanent(error),
    };
    const response = await this.#send({
      method: "POST",
      path: messagePath(lease, "fail"),
      payload,
      repeatable: true,
      signal: options.signal,
    });
    return failResultFrom(response.body as Schemas["FailResponse"]);
  }

  /**
   * Give the message back without an outcome; the attempt does not count.
   *
   * @throws LeaseLostError when the lease already ended
   */
  async release(lease: Lease, delaySeconds = 0, options: CallOptions = {}): Promise<void> {
    const payload: Schemas["ReleaseRequest"] = { receipt: lease.receipt };
    if (delaySeconds > 0) payload.delay_seconds = delaySeconds;
    await this.#send({
      method: "POST",
      path: messagePath(lease, "release"),
      payload,
      repeatable: true,
      signal: options.signal,
    });
  }

  /** A queue's counts. Needs the consume or admin scope. Not part of the protocol; for monitoring. */
  async stats(queue: string, options: CallOptions = {}): Promise<Stats> {
    const response = await this.#send({
      method: "GET",
      path: `/v1/queues/${segment(queue)}`,
      repeatable: true,
      signal: options.signal,
    });
    return statsFrom((response.body as Schemas["Queue"]).stats);
  }

  /** Never the key. */
  toString(): string {
    return `Client(${this.#baseUrl})`;
  }

  async #send(call: Call): Promise<{ status: number; body: unknown }> {
    for (let attempt = 1; ; attempt++) {
      let response: Response;
      try {
        response = await this.#fetch(this.#baseUrl + call.path, this.#request(call));
      } catch (error) {
        if (call.signal?.aborted) {
          throw error;
        }
        const reason = error instanceof Error ? error.message : String(error);
        if (call.repeatable && attempt <= this.#httpRetries) {
          await this.#pause(attempt, null, call, reason);
          continue;
        }
        throw new TransportError(`${call.method} ${call.path} failed: ${reason}`, { cause: error });
      }

      const raw = await response.text();
      let body: unknown = null;
      if (raw !== "") {
        try {
          body = JSON.parse(raw);
        } catch {
          // A proxy's error page, say: the status says enough.
        }
      }
      if (response.status < 400) {
        return { status: response.status, body };
      }

      const retryAfter = parseRetryAfter(response.headers.get("Retry-After"));
      const error = ApiError.fromResponse(response.status, body, retryAfter);
      // A 429 was not processed at all, so it is always safe to send again.
      const retry = error instanceof RateLimitedError || (call.repeatable && error instanceof ServerError);
      if (retry && attempt <= this.#httpRetries) {
        await this.#pause(attempt, retryAfter, call, error.message);
        continue;
      }
      throw error;
    }
  }

  #request(call: Call): RequestInit {
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.#apiKey}`,
      Accept: "application/json",
      "User-Agent": this.#userAgent,
    };
    const timeout = AbortSignal.timeout(this.#timeoutMs + (call.extraTimeoutMs ?? 0));
    const init: RequestInit = {
      method: call.method,
      headers,
      signal: call.signal ? AbortSignal.any([call.signal, timeout]) : timeout,
    };
    if (call.payload !== undefined) {
      headers["Content-Type"] = "application/json";
      init.body = JSON.stringify(call.payload);
    }
    return init;
  }

  async #pause(attempt: number, retryAfter: number | null, call: Call, reason: string): Promise<void> {
    const delayMs = retryAfter !== null ? retryAfter * 1000 : this.#retryDelayMs * attempt;
    this.logger?.warn("LessorMQ request failed, retrying", { path: call.path, attempt, delayMs, reason });
    await sleep(delayMs, call.signal);
  }
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const timer = setTimeout(done, ms);
    function done(): void {
      signal?.removeEventListener("abort", aborted);
      resolve();
    }
    function aborted(): void {
      clearTimeout(timer);
      reject(signal?.reason);
    }
    signal?.addEventListener("abort", aborted, { once: true });
  });
}

function segment(value: string): string {
  return encodeURIComponent(value);
}

function messagePath(lease: Lease, action: string): string {
  return `/v1/messages/${segment(lease.message.id)}/${action}`;
}

function randomHex(bytes: number): string {
  const values = crypto.getRandomValues(new Uint8Array(bytes));
  return Array.from(values, (value) => value.toString(16).padStart(2, "0")).join("");
}

function parseRetryAfter(value: string | null): number | null {
  if (value === null || !/^\d+$/.test(value.trim())) return null;
  return Number(value.trim());
}

/** Where the error was thrown: the first frame of its stack, when it can be read. */
function origin(error: Error): { file: string; line: number } | undefined {
  for (const line of (error.stack ?? "").split("\n").slice(1)) {
    const match = /\(?((?:file|https?|node|data):[^()]*?|\/[^():]*?|[A-Za-z]:\\[^():]*?):(\d+):\d+\)?\s*$/.exec(line.trim());
    if (match?.[1] && match[2]) {
      return { file: match[1], line: Number(match[2]) };
    }
  }
  return undefined;
}

