---
title: LessorMQ
description: "A message queue over HTTP where the server decides what happens to every message, and clients only claim, run, ack or fail."
---

# LessorMQ

LessorMQ is a message queue over HTTP for teams that run many small workers in many
languages. You publish a message, a worker claims it, runs the job, and reports that it
worked or that it failed. Everything else is decided by the server.

## The server decides

In most queues the client library carries the policy: it counts retries, sleeps
between them, decides when to give up, and moves dead messages somewhere. Every library
does that a little differently, and changing the policy means redeploying every worker.

LessorMQ keeps all of that on the server, as settings of the queue:

- how long a worker may hold a message before it is handed to someone else;
- how often a failed message is tried again, and how long it waits in between;
- what happens after the last attempt: kept in a dead-letter set, or dropped;
- which route (VPN, proxy, API account) a message should not take again;
- how long finished messages are kept.

A client only claims, runs, and reports. A worker in a new language is a loop of a few
dozen lines, and it behaves exactly like every other worker on the queue.

## The name

A lessor is the party that grants a lease and sets its terms. LessorMQ leases each
message to one worker at a time: the worker gets a receipt and a deadline, and only the
holder of the current receipt can report on the message. When the lease runs out, the
server takes the message back and counts the attempt, whatever the worker is still doing.

## What you get

- Leases with receipts: a slow worker can never complete or reset a message another
  worker now holds.
- Retries, backoff, attempts and dead-lettering per queue, changed at runtime.
- Deduplication by key, enforced by the database.
- Routes: keep a message away from the VPN exit or proxy it was refused on.
- Long polling: workers wait for work instead of hammering the server.
- Tenants with API keys and scopes, for running one installation for many teams.
- Prometheus metrics for stuck queues.
- An OpenAPI description, a protocol you can implement in an afternoon, and a
  conformance suite that tells you when you got it right.

## Start here

- [Quickstart](quickstart.md): a server on your machine and a first message with curl.
- [Concepts](concepts.md): queues, leases, attempts, the dead-letter set, routes, tenants.

## Client libraries

- [Client libraries](clients/index.md): every library, for each language and framework,
  and how to install it.

## Guides

- [Examples](examples.md): complete workers to copy, each one tested.
- [Writing a client](writing-a-client.md): what a conforming client does, and how to prove it.
- [Self-hosting](self-hosting.md): configuration, scaling, metrics and alerts.

## Reference

- [Protocol](protocol.md): every rule, and the reasons for it.
- [API reference](api.md): every endpoint, request and answer.
