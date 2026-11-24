---
title: Writing a client
description: "What a LessorMQ client does, the rules it follows, and how the conformance suite proves it."
---

# Writing a client

A LessorMQ client is small on purpose: it claims, runs a handler, and reports. All
policy lives on the server. The [client libraries](clients/index.md) cover the common
languages and frameworks; this page is for a language that has none yet. The [protocol](protocol.md) and the curl [quickstart](quickstart.md) show every
call on the wire.

## The loop

1. `POST /v1/queues/{queue}/claims` with `wait_seconds` (up to 20). A 204 means nothing
   arrived in time: claim again, without sleeping.
2. Run the handler with the message's `body` and `headers`.
3. While it runs, call `POST /v1/messages/{id}/extend` with the receipt every third of
   the lease, if the job can take longer than the lease.
4. On success, `POST /v1/messages/{id}/ack` with the receipt.
5. On an exception, `POST /v1/messages/{id}/fail` with the receipt and
   `{"error": {"type": ..., "message": ...}}`. Add `"permanent": true` only when the
   input itself is unusable. The answer says what the server did; log it, do not act
   on it.
6. On `409 lease_lost`, drop the message: it is someone else's now.

## The rules

A conforming client:

- sends the API key as `Authorization: Bearer <key>` on every request;
- never decides on its own to retry, delay or drop a message;
- retries network errors and 5xx answers of claim, extend, ack, fail and release with
  the same receipt, and of publish only with a `dedup_key` (generate one per publish
  to make that always safe);
- waits `Retry-After` seconds on 429;
- ignores fields it does not know: the API only ever adds them.

The full set, with the reasons, is in the [protocol](protocol.md); every request and
answer is in the [API reference](api.md).

## Proving it

The server repository has a conformance suite: scenarios written as data (YAML) that
run against a real server in docker. A library passes them through a small adapter that
maps the six protocol calls onto its own API, and turns its results and errors back into
status codes.

The adapter is a program in your library's language that reads one JSON request per line
on stdin and answers one JSON result per line on stdout:

```json
{"call": "claim", "api_key": "lmq_...", "args": {"queue": "c1a2b3-0-main", "wait_seconds": 1}}
{"status": 200, "body": {"receipt": "...", "lease_until": "...", "message": {...}}}
```

An empty claim answers status 204, a lost lease status 409 with `"code": "lease_lost"`,
as the HTTP API would. The Python library's adapter is the model:
`clients/python/tests/conformance_adapter.py`.

```sh
LESSORMQ_ADAPTER_CMD="python3 path/to/adapter.py" conformance/run.sh
```

The runner, the scenarios and the setup of tenants and queues stay as they are. Every
official library passes the suite before it is released.
