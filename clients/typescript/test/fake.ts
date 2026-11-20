/** A scripted LessorMQ server for the unit tests: answers queued per path, every request recorded. */

export const LEASE = {
  receipt: "r1",
  lease_until: "2030-01-01T00:00:30Z",
  message: {
    id: "m1",
    queue: "mail",
    body: { to: "ada@example.com" },
    headers: { type: "Welcome" },
    attempt: 1,
    max_attempts: 7,
    created_at: "2030-01-01T00:00:00Z",
  },
};

export const FAIL_RESULT = { outcome: "retrying", available_at: "2030-01-01T00:00:10Z", attempt: 1, max_attempts: 7 };

export const PUBLISHED = {
  id: "m1",
  queue: "mail",
  status: "pending",
  created_at: "2030-01-01T00:00:00Z",
  available_at: "2030-01-01T00:00:00Z",
  duplicate: false,
};

/** An answer: [status, body, headers?], a Response, or an Error to throw. */
type Answer = [number, unknown, Record<string, string>?] | Response | Error;

export interface Recorded {
  path: string;
  init: RequestInit;
  body: Record<string, unknown>;
}

export class Fake {
  readonly answers = new Map<string, Answer[]>();
  readonly requests: Recorded[] = [];
  /** Runs before each answer; lets a test act while a request is in flight. */
  onRequest: ((path: string) => void) | null = null;

  /** Answers for a path, in order; the last one repeats. */
  on(path: string, ...answers: Answer[]): this {
    this.answers.set(path, [...(this.answers.get(path) ?? []), ...answers]);
    return this;
  }

  calls(path: string): Record<string, unknown>[] {
    return this.requests.filter((r) => r.path === path).map((r) => r.body);
  }

  readonly fetch = async (input: string | URL | Request, init: RequestInit = {}): Promise<Response> => {
    const path = new URL(String(input)).pathname;
    this.requests.push({ path, init, body: init.body ? JSON.parse(String(init.body)) : {} });
    this.onRequest?.(path);
    const queue = this.answers.get(path);
    if (!queue || queue.length === 0) {
      return Response.json({ code: "not_found", detail: "no answer scripted" }, { status: 404 });
    }
    const answer = queue.length > 1 ? queue.shift()! : queue[0]!;
    if (answer instanceof Error) throw answer;
    if (answer instanceof Response) return answer.clone();
    const [status, body, headers] = answer;
    if (body === null) return new Response(null, { status, headers: headers ?? {} });
    return Response.json(body, { status, headers: headers ?? {} });
  };
}
