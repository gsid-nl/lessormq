# Contributing to LessorMQ

Thank you for helping. Issues and pull requests are welcome on
[GitHub](https://github.com/gsid-nl/lessormq).

## The one rule

**The server decides; clients only claim, run, ack or fail.** Retries, backoff,
dead-lettering, deduplication and routes belong in the server, as queue settings. A
client library may pass settings along (a lease length, a route), but it never decides
on its own to retry, delay or drop a message. A change that moves policy into a client
will not be merged, however convenient.

## Before you start

For anything larger than a small fix, open an issue first and describe what you want to
change and why. Changes to the protocol (`docs/protocol.md`, `docs/openapi.yaml`) need
the most care: every client library and every deployed worker depends on them.

## Working on the server

```sh
composer install
docker compose up -d --wait        # MariaDB, Redis and the server on :18080
vendor/bin/phpunit                 # unit and functional tests
conformance/run.sh                 # the protocol scenarios against a real server
```

- PHP code follows PSR-12 (`phpcs --standard=PSR12 src tests`).
- Migrations are append-only: never edit or delete one that exists; write a new one,
  and keep DDL online and idempotent (`ADD COLUMN IF NOT EXISTS`, `ALGORITHM=INSTANT`
  or `INPLACE, LOCK=NONE`).
- Times are UTC everywhere.

## Working on a client

Every library passes the conformance suite through an adapter, a program that reads one
JSON request per line and answers one JSON result per line (see
[writing a client](docs/writing-a-client.md)):

```sh
LESSORMQ_ADAPTER_CMD="clients/python/.venv/bin/python clients/python/tests/conformance_adapter.py" conformance/run.sh
```

Each client's README says how to run its own tests.

## Documentation

`docs/api.md` and `docs/examples.md` are generated: change `docs/openapi.yaml` or
`docs/tools/examples.template.md` and run `php docs/tools/build.php`. Every example in
`examples/` runs against a real server in `examples/test.sh`.

## Commits and pull requests

Keep a pull request to one change, with tests. Describe what changed and why in the
commit message. By contributing you agree that your work is licensed under the
[Apache License 2.0](LICENSE).
