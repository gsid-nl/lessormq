---
title: Self-hosting
description: "Running LessorMQ yourself: requirements, configuration, scheduled jobs, sizing, metrics and alerts."
---

# Self-hosting

LessorMQ is a PHP application (Symfony) with MariaDB for messages and Redis for caches
and coordination. The application nodes keep no state of their own, so you run as many
as you need behind a load balancer.

## Requirements

- PHP 8.4 with `pdo_mysql`, served by PHP-FPM behind nginx (or any FastCGI server).
- MariaDB 10.6 or newer. Claims rely on `SKIP LOCKED`, and the migrations on MariaDB's
  online schema changes; MySQL is not supported.
- Redis 6 or newer (or Valkey): queue settings cache, pause state, sweep throttling,
  usage counters. Losing Redis loses nothing that matters; it is rebuilt from MariaDB.

## The image

Every release is an image for amd64 and arm64, `ghcr.io/gsid-nl/lessormq:<version>`
(`:0.3` follows the latest 0.3.x, `:latest` the latest release). It runs PHP-FPM on
port 9000 as uid 1000, with the application in `/app`: put nginx, or any FastCGI server,
in front and send every request to `/app/public/index.php`. Given a command, the image
runs that instead, so the same image runs the migrations and the scheduled jobs:

```sh
docker run --rm --env-file lessormq.env ghcr.io/gsid-nl/lessormq:<version> \
  bin/console doctrine:migrations:migrate --no-interaction
```

## Configuration

Environment variables:

| Variable | |
|---|---|
| `APP_ENV` | `prod` |
| `APP_SECRET` | a random string |
| `DATABASE_QUEUE_URL` | `mysql://user:password@host:3306/lessormq?serverVersion=mariadb-10.11.0&charset=utf8mb4` |
| `REDIS_URL` | `redis://host:6379` |
| `LEASE_SWEEP_SECONDS` | how often, at most, a queue's expired leases are ended (default 60) |
| `CORS_ALLOW_ORIGIN` | a regular expression for the browser origins that may call `/v1` (default `localhost` and `127.0.0.1`); `^$` allows none |

## Setting up

```sh
bin/console doctrine:migrations:migrate --no-interaction
bin/console lessormq:operator-key:create --name=admin
```

The second command prints an operator key once. Use it to create tenants:

```sh
curl -X POST https://mq.example.com/v1/tenants \
  -H "Authorization: Bearer $OPERATOR_KEY" -H 'Content-Type: application/json' \
  -d '{"name": "Acme"}'
```

The answer holds the tenant's first admin key, again shown once. Migrations are safe to
run on every deploy, before the new version starts serving: they change the schema
online, without locking the messages table.

To try LessorMQ locally, `docker compose up` in the repository starts a server with
MariaDB and Redis on port 18080 (see the [quickstart](quickstart.md)).

## Managing it

Everything an administrator does goes through the [API](api.md): tenants and their keys
with an operator key, queues and the dead-letter set with a tenant's admin key, or with the
operator key and the `LessorMQ-Tenant` header.

The repository has a small example interface for that, `examples/admin-ui`: three static
files without a build step. Serve them on an origin the server allows and connect with a
key:

```sh
php -S 127.0.0.1:8081 -t examples/admin-ui
```

It calls the server from the browser, so the server must allow its origin:
`CORS_ALLOW_ORIGIN='^https://admin\.example\.com$'` for an interface on
`admin.example.com`.

## Scheduled jobs

| Command | How often | |
|---|---|---|
| `bin/console app:purge-old-messages` | every 15 minutes | removes finished messages past their queue's retention |
| `bin/console lessormq:tenants:reap` | hourly | removes the data of deleted tenants |

## What to expose

Expose `/v1/*` and `/healthz`. Keep `/metrics` for your monitoring only: it lists every
tenant's queues.

## Sizing

- **Application nodes.** A request takes a few milliseconds of CPU. A claim with
  `wait_seconds` holds a PHP worker for as long as it waits, so size the FPM pool for the
  number of workers that wait at the same time, or have workers wait less (5 seconds is a
  good default for most). Waiting checks for new work once a second.
- **MariaDB.** An empty claim is one indexed read; a claim that gets a message is a short
  transaction on one row. The messages table holds open messages plus finished ones for
  their retention, so retention drives its size more than throughput does.
- **Redis.** Small: a few keys per queue.

## Monitoring

`GET /metrics` serves Prometheus metrics for every queue with open messages:

| Metric | |
|---|---|
| `lessormq_messages{tenant,queue,state}` | open messages: `ready`, `delayed`, `processing` |
| `lessormq_oldest_ready_seconds{tenant,queue}` | how long the oldest ready message has waited |
| `lessormq_oldest_processing_seconds{tenant,queue}` | how long the longest lease has been held |
| `lessormq_max_retry_count{tenant,queue}` | the highest attempt count among open messages |
| `lessormq_paused{tenant,queue}` | 1 while the queue is paused |

Queues get stuck as an age, not as a count: a long queue that drains is fine, a short one
whose head never moves is not.

```yaml
groups:
  - name: lessormq
    rules:
      - alert: LessorMQQueueStuck
        expr: lessormq_oldest_ready_seconds > 900 and on(tenant, queue) lessormq_paused == 0
        for: 5m
        annotations:
          summary: "{{ $labels.tenant }}/{{ $labels.queue }}: nothing has taken work for 15 minutes"
      - alert: LessorMQLeaseHeldLong
        expr: lessormq_oldest_processing_seconds > 3600
        for: 10m
        annotations:
          summary: "{{ $labels.tenant }}/{{ $labels.queue }}: a message has been processing for over an hour"
      - alert: LessorMQPausedLong
        expr: lessormq_paused == 1
        for: 1d
        annotations:
          summary: "{{ $labels.tenant }}/{{ $labels.queue }} has been paused for a day"
```

`/healthz` answers 200 while MariaDB and Redis are reachable, 503 otherwise; use it for
readiness checks.
