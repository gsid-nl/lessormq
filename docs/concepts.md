---
title: Concepts
description: "Tenants and keys, queue settings, the life of a message, leases and receipts, attempts, the dead-letter set, deduplication and routes."
---

# Concepts

## Tenants and keys

An installation serves one or more tenants. Every request carries an API key, and the key
says which tenant it belongs to: no path contains a tenant, and tenants never see each
other's queues or messages.

A key has scopes. `publish` may put messages on queues and read back what became of
them, `consume` may claim and report on them, `admin` may do everything inside the
tenant: change queue settings, inspect and replay messages, manage keys. A key can also
be limited to queues matching a pattern, such as `import-*`.

Tenants themselves are managed with an operator key, which belongs to no tenant. It is
created on the command line when the installation is set up, typically for a web
interface that signs up and manages tenants.

## Queues and their settings

A queue exists from its first message on. Its behaviour comes from three layers:
built-in defaults, the tenant's defaults, and the queue's own settings, each overriding
the one before.

| Setting | Default | What it decides |
|---|---|---|
| `lease_seconds` | 300 | how long a claim holds a message |
| `max_lease_seconds` | 3600 | the longest lease a worker may ask for at once |
| `max_attempts` | 7 | attempts before the message is given up on |
| `max_attempts_limit` | 25 | the most attempts a publisher may ask for |
| `backoff` | 10 s, ×2, up to 1 h, ±10 % | the wait before the next attempt |
| `on_exhausted` | `dead_letter` | after the last attempt: keep it, or `discard` it |
| `on_permanent` | `dead_letter` | what a permanent failure does: dead-letter at once, or `retry` |
| `route_policy` | none | `avoid_failed`: keep messages off routes they failed on, for `cooldown_seconds` |
| `retention` | 1 day completed, 7 days failed | how long finished messages are kept |
| `max_message_bytes` | 1 MiB | the largest body plus headers |
| `paused` | false | a paused queue accepts messages but hands none out |

Settings change with `PATCH /v1/queues/{queue}` and take effect within a second.

## The life of a message

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

A message is published to a queue, optionally with a delay, a dedup key, headers, or a
different number of attempts. It waits as `pending` until a worker claims it.

## Leases and receipts

A claim returns the message, a receipt and the time the lease ends. Only the holder of
the current receipt can extend the lease, ack, fail, or release the message.

When a lease runs out, the server makes the message available again and counts the
attempt. The worker that held it may still be busy; when it finally reports, it gets
`409 lease_lost` and must drop the message. This is what keeps a slow worker from
completing or resetting a message that another worker now holds.

A worker with a long job extends its lease while it works. The official libraries do this
on a timer.

## Attempts and failures

Every claim starts an attempt. A worker that fails reports the error's type and message;
the queue decides what follows:

- with attempts left, the message becomes pending again after the backoff;
- after the last attempt, it goes to the dead-letter set (or is discarded);
- an expired lease counts as a failed attempt, so a message that crashes its worker still
  runs out of attempts instead of coming back forever.

A worker that can tell the input itself is unusable reports the failure as permanent. The
queue's `on_permanent` setting decides whether that skips the remaining attempts.

Delivery is at least once: a message can run more than once, when a lease expires under a
slow worker or an ack is lost on the network. Handlers should tolerate that.

## The dead-letter set

Messages the server gave up on keep status `failed`, with their last error, for the
queue's retention. Administrators list them (filtered by error type), look at them,
replay them (back to pending, attempts reset) or delete them, one at a time or in bulk.

## Deduplication

A publish with a `dedup_key` returns the existing message if one with that key is still
open (pending or processing) in the queue. The database enforces it, so two publishes at
the same moment still create one message. Once a message completes or fails, its key is
free again.

## Routes

Some failures depend on the path a job took. A website that blocks one VPN exit will keep
refusing it; an API account that used up its quota fails until tomorrow. A worker that
knows its path passes it as `route` when it claims. The server records the route of every
failed attempt, and on a queue with `route_policy.mode: avoid_failed` it does not hand
the message to a worker on such a route until `cooldown_seconds` have passed.

## Long polling

A claim may wait up to 20 seconds for a message instead of returning empty at once. A
worker loop needs no sleep of its own, and the server is not flooded with empty polls.
