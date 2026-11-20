/**
 * LessorMQ for TypeScript and JavaScript: the message queue over HTTP where the server
 * decides and clients only claim, run, ack or fail.
 *
 * ```ts
 * import { Client, Worker } from "lessormq";
 *
 * const client = new Client("https://mq.example.com", apiKey);
 * await client.publish("mail", { to: "ada@example.com" });
 * await new Worker(client, "mail", async (message) => send(message.body)).run();
 * ```
 */

export { Client, USER_AGENT } from "./client.ts";
export type { CallOptions, ClaimOptions, ClientOptions, FailOptions, Logger, PublishOptions } from "./client.ts";
export {
  ApiError,
  AuthenticationError,
  ConflictError,
  ForbiddenError,
  isPermanent,
  LeaseLostError,
  LessorMQError,
  NotFoundError,
  PayloadTooLargeError,
  PermanentFailure,
  RateLimitedError,
  ServerError,
  TransportError,
  ValidationError,
} from "./errors.ts";
export type { FieldError } from "./errors.ts";
export { secondsLeft } from "./models.ts";
export type { FailResult, Lease, Message, Published, Stats } from "./models.ts";
export { Worker } from "./worker.ts";
export type { Handler, Job, WorkerOptions } from "./worker.ts";
