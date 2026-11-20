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

## Releases

A maintainer releases by pushing a version tag; `.github/workflows/release.yml` runs the
whole of CI on the tagged commit (the conformance suite through every client included)
and publishes only when it passes:

| Tag | Publishes |
|---|---|
| `0.3.0` | the server image `ghcr.io/gsid-nl/lessormq`, and a GitHub release |
| `python-0.2.0` | `lessormq` on PyPI |
| `typescript-0.2.0` | `lessormq` on npm |
| `php-0.2.0` | `lessormq/client` on Packagist, as tag `v0.2.0` on gsid-nl/lessormq-php |
| `messenger-0.2.0` | `lessormq/messenger` on Packagist, as tag `v0.2.0` on gsid-nl/lessormq-messenger |
| `go-0.2.0` | `github.com/gsid-nl/lessormq-go`, as tag `v0.2.0` on that repository |

The Python and TypeScript packages carry their version (`__init__.py`, `package.json`);
raise it in a commit before tagging, or the release stops. Clients are versioned apart
from the server and from each other. When `lessormq/client` moves to a new minor version,
raise its branch alias in `clients/php/composer.json` and the constraint on it in
`clients/symfony-messenger/composer.json`.

## Commits and pull requests

Keep a pull request to one change, with tests. Describe what changed and why in the
commit message. By contributing you agree that your work is licensed under the
[Apache License 2.0](LICENSE).
