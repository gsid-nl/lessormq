/** What the server hands back, as camelCase objects with Dates, built from the wire types. */

import type { components } from "./openapi.ts";

type Schemas = components["schemas"];

/** A delivered message: what a handler runs. */
export interface Message<Body = unknown> {
  readonly id: string;
  readonly queue: string;
  /** The JSON value as published. */
  readonly body: Body;
  readonly headers: Record<string, string>;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly createdAt: Date;
  /** Whether this attempt is the last one the queue allows. */
  readonly isLastAttempt: boolean;
}

/**
 * A claimed message and the proof of holding it. Extend, ack, fail and release take the
 * lease; only its receipt is accepted, and only until `leaseUntil`, which extend moves.
 */
export interface Lease<Body = unknown> {
  readonly receipt: string;
  leaseUntil: Date;
  readonly message: Message<Body>;
}

/** The answer to a publish: the message, or the open one with the same dedup key. */
export interface Published {
  readonly id: string;
  readonly queue: string;
  readonly status: Schemas["MessageStatus"];
  readonly createdAt: Date;
  readonly availableAt: Date;
  readonly duplicate: boolean;
}

/** What the server did with a failed attempt. For logging: a client never acts on it. */
export interface FailResult {
  readonly outcome: Schemas["FailResponse"]["outcome"];
  readonly availableAt: Date | null;
  readonly attempt: number;
  readonly maxAttempts: number;
}

/** A queue's counts, for monitoring. Not part of the protocol. */
export interface Stats {
  readonly ready: number;
  readonly delayed: number;
  readonly processing: number;
  readonly completed: number;
  readonly failed: number;
  readonly oldestReadySeconds: number;
  readonly oldestProcessingSeconds: number;
}

/** Seconds until the lease ends; negative once it has. */
export function secondsLeft(lease: Lease): number {
  return (lease.leaseUntil.getTime() - Date.now()) / 1000;
}

/** An RFC 3339 time as the API writes them: UTC, whole seconds, `Z`. */
export function formatTime(time: Date): string {
  return time.toISOString().replace(/\.\d{3}Z$/, "Z");
}

export function messageFrom<Body>(data: Schemas["DeliveredMessage"]): Message<Body> {
  return {
    id: String(data.id),
    queue: String(data.queue),
    body: data.body as Body,
    headers: typeof data.headers === "object" && data.headers !== null ? { ...data.headers } : {},
    attempt: data.attempt,
    maxAttempts: data.max_attempts,
    createdAt: new Date(data.created_at),
    isLastAttempt: data.attempt >= data.max_attempts,
  };
}

export function leaseFrom<Body>(data: Schemas["Lease"]): Lease<Body> {
  return { receipt: data.receipt, leaseUntil: new Date(data.lease_until), message: messageFrom<Body>(data.message) };
}

export function publishedFrom(data: Schemas["PublishResponse"]): Published {
  return {
    id: data.id,
    queue: data.queue,
    status: data.status,
    createdAt: new Date(data.created_at),
    availableAt: new Date(data.available_at),
    duplicate: data.duplicate,
  };
}

export function failResultFrom(data: Schemas["FailResponse"]): FailResult {
  return {
    outcome: data.outcome,
    availableAt: data.available_at ? new Date(data.available_at) : null,
    attempt: data.attempt,
    maxAttempts: data.max_attempts,
  };
}

export function statsFrom(data: Schemas["QueueStats"]): Stats {
  return {
    ready: data.ready,
    delayed: data.delayed,
    processing: data.processing,
    completed: data.completed,
    failed: data.failed,
    oldestReadySeconds: data.oldest_ready_seconds,
    oldestProcessingSeconds: data.oldest_processing_seconds,
  };
}
