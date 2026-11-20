---
title: The protocol
description: "How LessorMQ behaves and the rules a conforming client follows: leases, receipts, attempts, routes, idempotency, errors and versioning."
---

# The protocol

LessorMQ is a message queue over HTTP. This document explains how it behaves and what a
client must do. The exact requests and responses are in [openapi.yaml](https://github.com/gsid-nl/lessormq/blob/main/docs/openapi.yaml).

## The rule: the server decides

A lessor grants a lease and sets its terms. LessorMQ leases each message to one worker
at a time, and everything after that is decided by the server, from the settings of the
message's queue:

- when a failed message is tried again, and how often;
- when a message is given up on, and whether it is kept (dead-lettered) or dropped;
- when a lease has expired and the message goes to another worker;
- which route a message may not take again;
- how long finished messages are kept.

A client gets a message, runs it, and reports that it worked or that it failed. It may
pass settings (a lease length, a maximum number of attempts, a delay) and the server
applies them within the limits of the queue. A client never decides whether a message is
retried. That keeps every client small, and makes the behaviour of a queue the same
whichever language its workers are written in.

## Tenants and keys

Every request carries `Authorization: Bearer <api key>`. The key identifies the tenant,
so no path contains a tenant. Queue names are unique within a tenant; two tenants can
both have a queue called `mail` without seeing each other's messages.

A key has scopes:

| Scope     | Allows                                                                |
|-----------|-----------------------------------------------------------------------|
| `publish` | publishing messages                                                   |
| `consume` | claiming, extending, acking, failing, releasing; reading queue stats  |
| `admin`   | everything, including queue settings, the dead-letter set and keys    |

A key can also be limited to queue name patterns, such as `import-*`.

### Operators

Tenants are created and managed by whoever runs the installation, usually through a web
interface, with an **operator key**. An operator key belongs to no tenant and is the
only key accepted on `/v1/tenants`. There it can:

- create a tenant, with quotas and default queue settings, and receive its first key;
- change a tenant's name, quotas and defaults;
- suspend a tenant (its keys get `403 tenant_suspended`, its messages stay) and resume it;
- delete a suspended tenant with all its data;
- create and revoke keys for any tenant.

With the header `LessorMQ-Tenant: <tenant id>`, an operator key can also call every
other endpoint as an `admin` key of that tenant, so the same interface can show a
tenant's queues and dead-letter set without holding that tenant's keys.

The first operator key is created on the command line when the installation is set up.
No endpoint creates operator keys, so a leaked tenant key can never be turned into one.

A tenant can read its own profile, quotas and usage at `/v1/tenant`, and change the
defaults for its new queues there.

## Message lifecycle

| From | To | When |
|---|---|---|
| (new) | `pending` | publish |
| `pending` | `processing` | a worker claims it; an attempt starts |
| `processing` | `completed` | ack |
| `processing` | `pending` | fail with attempts left, after the backoff |
| `processing` | `pending` | the lease expired; the attempt counts |
| `processing` | `pending` | release; the attempt does not count |
| `processing` | `failed` | fail with no attempts left, or a permanent failure |
| `processing` | (deleted) | as above, when the queue's `on_exhausted` is `discard` |
| `failed`, `completed` | `pending` | replay |
| `failed`, `completed` | (deleted) | retention |

`failed` is the dead-letter set: messages the server gave up on. They stay there for the
queue's `retention.failed_seconds`, so they can be inspected, replayed or deleted.

## Leases and receipts

A claim returns the message, a `receipt` and `lease_until`. The receipt is the only
proof that the worker holds the message. Extend, ack, fail and release all require it,
and are only accepted while the lease is current.

When the lease runs out, the server makes the message available again and counts the
attempt, whether or not the worker is still running. A late ack or fail from that worker
gets `409 lease_lost`: the message is no longer its to report on. This is what keeps a
slow worker from completing or resetting a message that another worker now holds.

A job that may outlast its lease must extend it. Client libraries do this on a timer
while the handler runs, at about a third of the lease length. `max_lease_seconds` bounds
one lease or one extension, not the total run time.

## Attempts, backoff and permanent failures

Every claim starts an attempt. After a failed attempt the server either schedules the
next one or stops:

- With attempts left, the message becomes pending again after the backoff delay:
  `min(initial_seconds * multiplier^(n-1), max_seconds)`, randomised by `jitter`, for the
  n-th failed attempt. With the defaults: 10 s, 20 s, 40 s, up to an hour.
- With no attempts left, the queue's `on_exhausted` decides: `dead_letter` (status
  `failed`) or `discard` (deleted).

An expired lease counts as a failed attempt. A worker that crashes on a message (out of
memory, killed) therefore cannot get it redelivered forever: `max_attempts` still ends it.

A worker that can tell that the input itself is unusable (it cannot be decoded, it fails
validation) reports `permanent: true`. That is a fact, not an instruction. The queue's
`on_permanent` decides what it means: `dead_letter` skips the remaining attempts,
`retry` treats it like any other failure.

The response to `fail` says what the server did (`retrying`, `dead_lettered`,
`discarded`). It is for logging; a client must not act on it.

## Routes

Some failures depend on the path a job took rather than on the job. A website that
blocks one VPN exit will keep refusing that exit, while another exit works. The same
holds for an API account that has used up its quota, or one proxy among many.

A worker that knows its path passes it as `route` when it claims. The server records the
route of every attempt. With the queue's `route_policy.mode` set to `avoid_failed`, a
message is not handed to a worker on a route where it failed within
`route_policy.cooldown_seconds`. Workers on other routes still get it.

The cooldown keeps this from getting stuck. A message that has failed on every route
becomes available on those routes again once the cooldown runs out; attempts keep
counting, so `max_attempts` still ends it. A worker without a route is not restricted.

## Delivery guarantees

- **At least once.** A message can be delivered more than once: when a lease expires
  while the worker is still running, or when an ack is lost on the network. Handlers must
  tolerate seeing a message again.
- **No strict ordering.** Messages are handed out roughly in order of availability.
  Retries, delays and concurrent workers reorder them.
- **Deduplication while open.** A publish with a `dedup_key` returns the existing message
  if one with that key is still pending or processing in the queue.

## Idempotency

Clients retry requests that failed on the network, so the server makes the calls that
end a lease safe to repeat:

- `ack` with the same receipt returns 204 again.
- `fail` with the same receipt returns the same outcome, as long as the message has not
  been claimed again.
- `publish` is only safe to repeat with a `dedup_key`. Client libraries may generate one
  per publish call for exactly that reason.

## Long polling

A claim with `wait_seconds` (at most 20) waits for a message instead of returning 204 at
once. A worker loop that claims with a wait needs no sleep of its own. For now the server
checks for work once a second while it waits, so a new message is picked up within a
second; a wake-up on publish will bring that down to milliseconds. A paused queue behaves
as an empty one, so workers simply wait out the pause.

## Errors

Every error is `application/problem+json` with a stable `code`:

| Code                | Status | Meaning                                                        |
|---------------------|--------|----------------------------------------------------------------|
| `validation_failed` | 400    | malformed request or a value out of range                      |
| `unauthorized`      | 401    | missing, unknown or revoked key                                |
| `forbidden`         | 403    | missing scope, or queue outside the key's patterns             |
| `tenant_suspended`  | 403    | the key's tenant is suspended by the operator                  |
| `not_found`         | 404    | no such queue or message for this tenant                       |
| `lease_lost`        | 409    | the receipt is not the current lease; stop reporting           |
| `invalid_state`     | 409    | the message is not in a state that allows this                 |
| `payload_too_large` | 413    | body and headers exceed the queue's `max_message_bytes`        |
| `rate_limited`      | 429    | tenant quota or rate limit; wait `Retry-After` seconds         |
| `unavailable`       | 503    | database or Redis unreachable; retry later                     |
| `internal`          | 500    | a server fault                                                 |

## Writing a client

A conforming client:

1. sends the API key as a bearer token on every request;
2. claims with a `wait_seconds` and loops; it does not sleep between empty claims;
3. runs the handler with the delivered `body` and `headers`;
4. extends the lease on a timer while the handler runs, when the job can outlast it;
5. acks on success and fails on an exception, with the error's type and message;
6. sets `permanent: true` only when the input itself is unusable;
7. on `lease_lost`, drops the message without retrying the call;
8. retries network failures and 5xx responses of extend, ack and fail with the same
   receipt, and of publish only with a `dedup_key`;
9. waits `Retry-After` on 429;
10. never decides on its own to retry, delay or drop a message.

Options a client library may expose: `consumer`, `route`, `wait_seconds`,
`lease_seconds`, the extend interval, HTTP timeouts and the number of HTTP retries, and
on publish `delay_seconds`, `available_at`, `max_attempts` and `dedup_key`.

The conformance suite checks these rules against a running server; every official client
passes it.

## Versioning

All endpoints live under `/v1`. Within v1, fields and endpoints are only added, never
removed or changed in meaning, so clients must ignore fields they do not know. A breaking
change gets a new path prefix, and the old one keeps working alongside it for a
deprecation period.
