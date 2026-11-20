# LessorMQ

A message queue over HTTP where the server decides what happens to every message.
Workers in any language claim a job, run it, and say whether it worked; retries,
backoff, dead-lettering, deduplication and routes are settings of the queue.

**Documentation: [lessormq.dev](https://lessormq.dev)** (the same pages are in [docs/](docs/README.md)).

## Why

In most queues the client library carries the policy: it counts retries, sleeps between
them, decides when to give up, and moves dead messages somewhere. Every library does
that a little differently, and changing the policy means redeploying every worker.

LessorMQ keeps all of that on the server. A client only claims, runs, and reports, so a
worker in a new language is a loop of a few dozen lines, and it behaves exactly like
every other worker on the queue.

## The name

A lessor is the party that grants a lease and sets its terms. LessorMQ leases each
message to one worker at a time: the worker gets a receipt and a deadline, and only the
holder of the current receipt can report on the message. When the lease runs out, the
server takes the message back and counts the attempt, whatever the worker is still doing.

## Quickstart

```sh
git clone https://github.com/gsid-nl/lessorMQ.git && cd lessorMQ
composer install
docker compose up -d --wait
```

A server with MariaDB and Redis listens on `http://127.0.0.1:18080`. The
[quickstart](docs/quickstart.md) takes a first message through publish, claim, fail and
ack with curl.

## What is in this repository

| | |
|---|---|
| `src/`, `config/`, `migrations/` | the server: PHP 8.4, Symfony, MariaDB, Redis |
| `docs/` | the documentation, and `openapi.yaml`, the API |
| `clients/` | libraries for [PHP](clients/php), [Symfony Messenger](clients/symfony-messenger), [Python](clients/python), [TypeScript](clients/typescript) and [Go](clients/go) |
| `conformance/` | scenarios every client must pass, as data, with a runner |
| `examples/` | tested workers in every language, and an [admin interface](examples/admin-ui) |
| `docker/` | the server image |

## Running it

See [self-hosting](docs/self-hosting.md): requirements, configuration, scheduled jobs,
sizing, metrics and alerts.

## Contributing

Issues and pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md). Report
security problems privately, as [SECURITY.md](SECURITY.md) describes.

## License

[Apache License 2.0](LICENSE).
