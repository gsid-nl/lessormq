/** Everything the library throws, and what a handler throws to report a permanent failure. */

export interface FieldError {
  field: string;
  message: string;
}

/** Everything this library throws. */
export class LessorMQError extends Error {
  override name = "LessorMQError";
}

/** The server could not be reached, after the configured retries. */
export class TransportError extends LessorMQError {
  override name = "TransportError";
}

/**
 * The server answered with an error (RFC 9457 problem details). `code` is the stable
 * problem code to branch on; the subclasses cover the ones a client acts on.
 */
export class ApiError extends LessorMQError {
  override name = "ApiError";
  readonly status: number;
  readonly code: string;
  readonly detail: string;
  readonly errors: FieldError[];

  constructor(status: number, code: string, detail = "", errors: FieldError[] = []) {
    super(detail || `LessorMQ error ${status} (${code})`);
    this.status = status;
    this.code = code;
    this.detail = detail;
    this.errors = errors;
  }

  /** @param retryAfter seconds, from the Retry-After header of a 429 */
  static fromResponse(status: number, body: unknown, retryAfter: number | null = null): ApiError {
    const problem = typeof body === "object" && body !== null ? (body as Record<string, unknown>) : {};
    const code = typeof problem.code === "string" ? problem.code : codeFor(status);
    const detail = typeof problem.detail === "string" ? problem.detail : "";
    const errors = Array.isArray(problem.errors) ? (problem.errors as FieldError[]) : [];

    if (code === "rate_limited") {
      return new RateLimitedError(status, code, detail, errors, retryAfter);
    }
    const ErrorClass = BY_CODE[code] ?? (status >= 500 ? ServerError : ApiError);
    return new ErrorClass(status, code, detail, errors);
  }
}

/**
 * The receipt is not the current lease of the message: it expired, was released, or the
 * message was claimed again. Stop reporting on it; the server has already made it
 * available again.
 */
export class LeaseLostError extends ApiError {
  override name = "LeaseLostError";
}

/** The request was rejected as invalid; `errors` names each field. */
export class ValidationError extends ApiError {
  override name = "ValidationError";
}

/** The API key is missing, unknown or revoked. */
export class AuthenticationError extends ApiError {
  override name = "AuthenticationError";
}

/** The key lacks the scope or the queue, or its tenant is suspended (code `tenant_suspended`). */
export class ForbiddenError extends ApiError {
  override name = "ForbiddenError";
}

/** No such queue or message for this tenant. */
export class NotFoundError extends ApiError {
  override name = "NotFoundError";
}

/** The message is in a state that does not allow this (`invalid_state`). */
export class ConflictError extends ApiError {
  override name = "ConflictError";
}

/** Body and headers exceed the queue's `max_message_bytes`. */
export class PayloadTooLargeError extends ApiError {
  override name = "PayloadTooLargeError";
}

/** The tenant exceeded a quota or rate limit, after the configured retries. */
export class RateLimitedError extends ApiError {
  override name = "RateLimitedError";
  readonly retryAfter: number | null;

  constructor(status: number, code: string, detail = "", errors: FieldError[] = [], retryAfter: number | null = null) {
    super(status, code, detail, errors);
    this.retryAfter = retryAfter;
  }
}

/** The server failed (5xx), after the configured retries. */
export class ServerError extends ApiError {
  override name = "ServerError";
}

/**
 * Throw from a handler when the message itself cannot be processed: it cannot be
 * decoded, it fails validation. Retrying it would only fail again. Any error with a
 * `permanent: true` property counts too. A fact for the server; the queue's
 * `on_permanent` decides what it means.
 */
export class PermanentFailure extends Error {
  override name = "PermanentFailure";
  readonly permanent = true;
}

/** Whether an error says the input itself cannot be processed. */
export function isPermanent(error: unknown): boolean {
  return (
    error instanceof PermanentFailure ||
    (typeof error === "object" && error !== null && (error as { permanent?: unknown }).permanent === true)
  );
}

type ApiErrorClass = new (status: number, code: string, detail?: string, errors?: FieldError[]) => ApiError;

const BY_CODE: Record<string, ApiErrorClass> = {
  lease_lost: LeaseLostError,
  validation_failed: ValidationError,
  unauthorized: AuthenticationError,
  forbidden: ForbiddenError,
  tenant_suspended: ForbiddenError,
  not_found: NotFoundError,
  invalid_state: ConflictError,
  payload_too_large: PayloadTooLargeError,
};

function codeFor(status: number): string {
  if (status === 401) return "unauthorized";
  if (status === 403) return "forbidden";
  if (status === 404) return "not_found";
  if (status === 413) return "payload_too_large";
  if (status === 429) return "rate_limited";
  if (status >= 500) return "internal";
  return "validation_failed";
}
