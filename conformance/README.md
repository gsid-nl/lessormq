# Conformance suite

Scenarios that pin down how a LessorMQ server behaves and how a client library talks to
it. They are data (`scenarios/*.yaml`), so any language can run them: a runner reads
the steps, performs each protocol call through an adapter, and checks the result.

## Running it

```sh
conformance/run.sh            # all scenarios
conformance/run.sh lease      # scenarios whose file name contains "lease"
```

This starts a server with MariaDB and Redis from `compose.yaml` (port 18080, override
with `LESSORMQ_PORT`), creates an operator key, runs every scenario through the
reference HTTP adapter, and removes the containers. Output is TAP; the exit code is 0
only when every scenario passed.

Against a server you run yourself:

```sh
LESSORMQ_URL=http://127.0.0.1:8080 LESSORMQ_OPERATOR_KEY=lmq_... php conformance/php/run.php
```

The server needs `LEASE_SWEEP_SECONDS=1`, or the lease scenarios wait up to a minute for
an expired lease to be ended and fail.

## What a run does

- Creates two tenants with the operator key, `a` and `b`, and removes them afterwards.
- Gives every scenario its own queues, so runs and scenarios never see each other's
  messages. Queue names in a scenario (`main` when none is given) are short names.
- Applies the scenario's `queues:` settings with `PATCH /v1/queues/{queue}`.
- Runs the steps in order and stops a scenario at its first failed expectation.

## Scenario format

```yaml
name: A failed attempt comes back after the queue's backoff
queues:
  main: {max_attempts: 3, backoff: {initial_seconds: 2, jitter: 0}}
steps:
  - publish: {body: {}}
    save: m                         # keep the result as $m
  - claim: {}
    save: l1
  - fail: {lease: $l1, error: {message: upstream slow}}
    expect: {status: 200, body.outcome: retrying, body.available_at: {seconds_from_now: [0, 3]}}
  - sleep: 2.5
  - claim: {}
    expect: {body.message.id: $m.body.id, body.message.attempt: 2}
```

Every step has one action, an optional `save` and an optional `expect`. Any step takes
`tenant: b` to act as the second tenant.

| Action | Through | Arguments |
|---|---|---|
| `publish` | adapter | `queue`, `body`, `headers`, `dedup_key`, `delay_seconds`, `available_at`, `max_attempts` |
| `claim` | adapter | `queue`, `consumer`, `route`, `lease_seconds`, `wait_seconds`; `publish_after: {seconds, body}` publishes from another process while the claim waits |
| `extend`, `ack`, `fail`, `release` | adapter | `lease` (a saved claim; supplies `message` and `receipt`), `receipt` or `message` to override, `lease_seconds`, `error`, `permanent`, `delay_seconds` |
| `settings` | admin API | `queue`, `set`: a queue settings patch |
| `queue`, `message`, `replay` | admin API | `queue`; `id` |
| `sleep` | runner | seconds |

A result is `{status, body, elapsed}`. `$name.path` anywhere in a step or an
expectation is replaced by that path in a saved result.

Expectations map a dotted path to a literal (compared by value and type) or to one
matcher: `not`, `between: [low, high]`, `seconds_from_now: [low, high]`, `matches`
(a regular expression), `exists: true|false`, `keys: [...]`.

## Client libraries

A library passes the suite by running these scenarios through its own API instead of
raw HTTP. It provides an adapter for the six protocol calls that calls the library and
turns its results back into `{status, body}`: an empty claim is status 204, a lease-lost
error is status 409 with `code: lease_lost`, and so on. Setup and inspection stay on the
admin API, so a library does not have to offer them.

- In any language, the adapter is a process: one JSON request per line on stdin
  (`{"call", "api_key", "args"}`), one JSON result per line on stdout (`{"status",
  "body"}`). It runs for the whole suite, with the runner's environment, `LESSORMQ_URL`
  included (`php/src/ProcessAdapter.php`).

  ```sh
  LESSORMQ_ADAPTER_CMD="clients/python/.venv/bin/python clients/python/tests/conformance_adapter.py" conformance/run.sh
  ```

- In PHP it can also be a class implementing `php/src/Adapter.php`, loaded from a file
  that returns `fn(string $url): Adapter`:

  ```sh
  LESSORMQ_ADAPTER=clients/php/tests/Conformance/adapter.php conformance/run.sh
  ```

For PHP, the runner in `php/` takes any `Adapter`; the PHP client's is
`clients/php/tests/Conformance/adapter.php`:

```sh
LESSORMQ_ADAPTER=clients/php/tests/Conformance/adapter.php conformance/run.sh
```

Libraries in other languages port the runner (a few hundred lines) and keep the
scenarios as they are; a new behaviour of the server gets a scenario here first.
