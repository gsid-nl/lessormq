/**
 * A worker: claims messages of one queue and runs a handler on each.
 *
 * The handler resolves: the message is acked. It throws (or rejects): the message is
 * failed with the error's name and text, as permanent when isPermanent(error). What
 * happens next (another attempt, when, or the dead-letter set) is decided by the queue.
 * While the handler runs, the lease is extended on a timer.
 */

import { sleep, type Client, type Logger } from "./client.ts";
import { LeaseLostError, LessorMQError } from "./errors.ts";
import { secondsLeft, type Lease, type Message } from "./models.ts";

/** What a handler gets besides the message: the lease, and a way to keep it. */
export interface Job<Body = unknown> {
  readonly lease: Lease<Body>;
  /**
   * Extend the lease now. The worker already does this on a timer; call it yourself only
   * with `autoExtend: false`, or to ask for a longer lease at once.
   *
   * @throws LeaseLostError when the lease already ended
   */
  extend(leaseSeconds?: number): Promise<void>;
  /** Aborted when the lease is lost while the handler runs: stop, the message is someone else's now. */
  readonly signal: AbortSignal;
}

export type Handler<Body = unknown> = (message: Message<Body>, job: Job<Body>) => unknown;

export interface WorkerOptions {
  /** Shown to administrators; $HOSTNAME by default (the pod name in Kubernetes). */
  consumer?: string;
  /** The path this worker runs jobs through (a VPN exit, a proxy, an API account). */
  route?: string;
  /** Long-poll wait per claim, at most 20. Default 20. */
  waitSeconds?: number;
  /** Lease per claim; the queue's when left out. */
  leaseSeconds?: number;
  /** Seconds between automatic extends; a third of the lease by default. */
  extendEvery?: number;
  /** Extend the lease while a handler runs. Default true. */
  autoExtend?: boolean;
  /** Stop after this many messages. */
  maxMessages?: number;
  /** Stop after running this long. */
  maxSeconds?: number;
  /** Milliseconds to wait after the server could not be reached. Default 1000. */
  errorPauseMs?: number;
  /**
   * Stops the worker after the current message. A claim already waiting is not cut off
   * (that could leave a leased message to run out); it ends within waitSeconds, and a
   * message it brings is given back with release.
   */
  signal?: AbortSignal;
  logger?: Logger;
}

/**
 * ```ts
 * const controller = new AbortController();
 * process.on("SIGTERM", () => controller.abort());
 *
 * await new Worker(client, "mail", async (message) => {
 *   await sendMail(message.body);
 * }, { signal: controller.signal }).run();
 * ```
 *
 * Several workers can run side by side in one process, on one client.
 */
export class Worker<Body = unknown> {
  readonly #client: Client;
  readonly #queue: string;
  readonly #handler: Handler<Body>;
  readonly #options: WorkerOptions;
  readonly #logger: Logger | undefined;
  #stopping = false;
  #wakeUp: (() => void) | null = null;

  constructor(client: Client, queue: string, handler: Handler<Body>, options: WorkerOptions = {}) {
    this.#client = client;
    this.#queue = queue;
    this.#handler = handler;
    this.#options = options;
    this.#logger = options.logger ?? client.logger;
    options.signal?.addEventListener("abort", () => this.stop(), { once: true });
  }

  /**
   * Claim and handle messages until stop(), the signal, or a configured limit.
   *
   * @returns messages handled
   */
  async run(): Promise<number> {
    this.#stopping = this.#options.signal?.aborted ?? false;
    let handled = 0;
    const started = Date.now();
    const { maxMessages, maxSeconds } = this.#options;

    while (!this.#stopping) {
      if (maxMessages !== undefined && handled >= maxMessages) break;
      if (maxSeconds !== undefined && Date.now() - started >= maxSeconds * 1000) break;
      if (await this.runOnce()) handled++;
    }
    return handled;
  }

  /**
   * One claim, and the handler on what it got.
   *
   * @returns whether a message was handled
   */
  async runOnce(): Promise<boolean> {
    let lease: Lease<Body> | null;
    try {
      const claim: Parameters<Client["claim"]>[1] = { waitSeconds: this.#options.waitSeconds ?? 20 };
      const consumer = this.#options.consumer ?? defaultConsumer();
      if (consumer !== undefined) claim.consumer = consumer;
      if (this.#options.route !== undefined) claim.route = this.#options.route;
      if (this.#options.leaseSeconds !== undefined) claim.leaseSeconds = this.#options.leaseSeconds;
      lease = await this.#client.claim<Body>(this.#queue, claim);
    } catch (error) {
      if (!(error instanceof LessorMQError)) throw error;
      this.#logger?.error("Claim failed", { queue: this.#queue, error: error.message });
      await this.#pause(this.#options.errorPauseMs ?? 1000);
      return false;
    }

    if (lease === null) return false;
    if (this.#stopping) {
      // Asked to stop while the claim waited: give the message back untouched.
      await this.#report(() => this.#client.release(lease), lease, "release");
      return false;
    }

    await this.#handle(lease);
    return true;
  }

  /** Stop after the current message. */
  stop(): void {
    this.#stopping = true;
    this.#wakeUp?.();
  }

  async #handle(lease: Lease<Body>): Promise<void> {
    const lost = new AbortController();
    const stopKeeping = this.#options.autoExtend === false ? () => {} : this.#keep(lease, lost);
    const job: Job<Body> = {
      lease,
      signal: lost.signal,
      extend: async (leaseSeconds?: number) => {
        await this.#client.extend(lease, leaseSeconds);
      },
    };

    let failure: { error: unknown } | null = null;
    try {
      await this.#handler(lease.message, job);
    } catch (error) {
      failure = { error };
    } finally {
      stopKeeping();
    }

    if (lost.signal.aborted) {
      this.#logger?.warn("Lease lost while the handler ran; not reporting", { messageId: lease.message.id });
      return;
    }
    if (failure === null) {
      await this.#report(() => this.#client.ack(lease), lease, "ack");
      return;
    }
    const { error } = failure;
    await this.#report(
      async () => {
        const result = await this.#client.fail(lease, error);
        this.#logger?.info?.("Message failed", {
          messageId: lease.message.id,
          error: error instanceof Error ? error.message : String(error),
          outcome: result.outcome,
          attempt: result.attempt,
          maxAttempts: result.maxAttempts,
        });
      },
      lease,
      "fail",
    );
  }

  /** Extend the lease every interval until stopped or the lease is lost. Returns the stop. */
  #keep(lease: Lease<Body>, lost: AbortController): () => void {
    let timer: ReturnType<typeof setTimeout> | undefined;
    let stopped = false;
    const schedule = (): void => {
      const seconds = this.#options.extendEvery ?? Math.max(1, secondsLeft(lease) / 3);
      timer = setTimeout(tick, seconds * 1000);
    };
    const tick = async (): Promise<void> => {
      if (stopped) return;
      try {
        await this.#client.extend(lease, this.#options.leaseSeconds);
      } catch (error) {
        if (error instanceof LeaseLostError) {
          lost.abort(error);
          return;
        }
        this.#logger?.warn("Extend failed; trying again", {
          error: error instanceof Error ? error.message : String(error),
        });
      }
      if (!stopped) schedule();
    };
    schedule();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }

  /**
   * Ack, fail or release, keeping the worker alive whatever happens: a lost lease means
   * the message is someone else's now; any other failure means the lease will expire
   * and the server will make the message available again.
   */
  async #report(call: () => Promise<unknown>, lease: Lease<Body>, what: string): Promise<void> {
    try {
      await call();
    } catch (error) {
      if (error instanceof LeaseLostError) {
        this.#logger?.warn(`Lease lost before ${what}; the message is no longer ours`, { messageId: lease.message.id });
        return;
      }
      if (!(error instanceof LessorMQError)) throw error;
      this.#logger?.error(`${what} failed; the lease will expire and the message come back`, {
        messageId: lease.message.id,
        error: error.message,
      });
    }
  }

  /** Wait, unless stop() comes first. */
  async #pause(ms: number): Promise<void> {
    const wake = new AbortController();
    this.#wakeUp = () => wake.abort();
    try {
      await sleep(ms, wake.signal);
    } catch {
      // Stopped.
    } finally {
      this.#wakeUp = null;
    }
  }
}

function defaultConsumer(): string | undefined {
  const env = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env;
  return env?.HOSTNAME || undefined;
}
