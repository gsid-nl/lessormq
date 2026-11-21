---
title: Self-hosting
description: "Running LessorMQ yourself: requirements, configuration, scheduled jobs, sizing, metrics and alerts."
---

# Self-hosting

LessorMQ is a PHP application (Symfony) with a database for messages (MariaDB, MySQL,
PostgreSQL or SQLite) and Redis for caches and coordination. The application nodes keep
no state of their own, so you run as many as you need behind a load balancer.

## Requirements

- PHP 8.4 with the PDO driver of your database (`pdo_mysql`, `pdo_pgsql` or
  `pdo_sqlite`), served by PHP-FPM behind nginx (or any FastCGI server). The image has
  all three.
- A database:

  | Database | Version | |
  |---|---|---|
  | MariaDB | 10.6 or newer | where LessorMQ began; its migrations change the schema online |
  | MySQL | 8.0.21 or newer | |
  | PostgreSQL | 13 or newer | |
  | SQLite | 3.35 or newer, with its math functions (the usual builds) | one machine only: every server process on the same host as the file. For trying LessorMQ, tests, and small single-host setups |

  Claims use `SKIP LOCKED` on the first three; SQLite, which has one writer at a time,
  takes each claim in a single statement.
- Redis 6 or newer (or Valkey): queue settings cache, pause state, sweep throttling,
  usage counters. Losing Redis loses nothing that matters; it is rebuilt from the
  database.

## The image

Every release is an image for amd64 and arm64, `ghcr.io/gsid-nl/lessormq:<version>`
(`:0.3` follows the latest 0.3.x, `:latest` the latest release). It runs PHP-FPM as uid
1000, with the application in `/app`, in two pools: claims on port 9001, every other call
on port 9000. Put nginx, or any FastCGI server, in front and send every request to
`/app/public/index.php`, claims to the claims pool:

```nginx
location ~ ^/v1/queues/[^/]+/claims$ {
    include fastcgi_params;
    fastcgi_pass 127.0.0.1:9001;
    fastcgi_param SCRIPT_FILENAME /app/public/index.php;
    fastcgi_read_timeout 30s;        # a claim may wait 20 seconds
}

location / {
    include fastcgi_params;
    fastcgi_pass 127.0.0.1:9000;
    fastcgi_param SCRIPT_FILENAME /app/public/index.php;
}
```

A claim that waits for work (`wait_seconds`) holds its PHP worker while it waits. In a pool
of their own, waiting claims never leave publishes, acks and the admin API without a
worker. The claims pool has up to 32 workers, started as claims come; a node lets at most
`MAX_WAITING_CLAIMS` claims (default 30) wait their full time at once, and others wait at
most a second. Size it to the workers that wait at the same time, with room in memory
(about 35 MB a waiting worker). Without the location for claims everything still works,
but many waiting claims can hold up the rest.

Given a command, the image runs that instead, so the same image runs the migrations and
the scheduled jobs:

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
| `DATABASE_QUEUE_URL` | the database, with its version (see below) |
| `REDIS_URL` | `redis://host:6379` |
| `LEASE_SWEEP_SECONDS` | how often, at most, a queue's expired leases are ended (default 60) |
| `MAX_WAITING_CLAIMS` | claims that may wait their full `wait_seconds` at once on a node (default 30; 0: no limit); keep it below the claims pool's `pm.max_children` |
| `CORS_ALLOW_ORIGIN` | a regular expression for the browser origins that may call `/v1` (default `localhost` and `127.0.0.1`); `^$` allows none |

`DATABASE_QUEUE_URL` per database; `serverVersion` saves a round trip at every start and
tells MariaDB from MySQL:

```sh
mysql://user:password@host:3306/lessormq?serverVersion=mariadb-10.11.0&charset=utf8mb4
mysql://user:password@host:3306/lessormq?serverVersion=8.4.0&charset=utf8mb4
postgresql://user:password@host:5432/lessormq?serverVersion=17&charset=utf8
sqlite:////var/lib/lessormq/lessormq.db
```

Each database has its own series of migrations; the server runs the one for the database
it is given. On SQLite the first migration switches the file to write-ahead logging, so
reading does not wait for writing; keep the file on a local disk.

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
MariaDB and Redis on port 18080 (see the [quickstart](quickstart.md)). On another
database, add its compose file: `docker compose -f compose.yaml -f compose.postgresql.yaml
up` (or `compose.mysql.yaml`, `compose.sqlite.yaml`).

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

Expose `/v1/*` and `/healthz`. Keep `/metrics` for your monitoring only: it takes no key
and lists every tenant's queues. From outside, the same comes from `/v1/metrics` with an
operator key.

## Sizing

- **Application nodes.** A request takes a few milliseconds of CPU; two nodes of one CPU
  each did about 300 full cycles (publish, claim, ack) a second in our load test
  (`tools/loadtest`), and past that grew slower without failing. A claim with
  `wait_seconds` holds a worker of the claims pool for as long as it waits (see The
  image), and blocks on Redis, so it costs the database nothing meanwhile.
- **The database.** An empty claim is one indexed read; a claim that gets a message is a
  short transaction on one row. The messages table holds open messages plus finished ones for
  their retention, so retention drives its size more than throughput does.
- **Redis.** Small: a few keys per queue.
- **Queue figures.** Pages, metrics and `GET /v1/queues` read only the open and
  dead-letter messages; the completed ones are counted per queue at most every 30
  seconds, which takes about 10 ms per 50,000 of them.

## Monitoring

`GET /metrics` serves Prometheus metrics for every queue of every tenant, idle ones with
zeros:

| Metric | |
|---|---|
| `lessormq_messages{tenant,queue,state}` | open messages: `ready`, `delayed`, `processing` |
| `lessormq_dead_letter_messages{tenant,queue}` | messages in the dead-letter set |
| `lessormq_oldest_ready_seconds{tenant,queue}` | how long the oldest ready message has waited |
| `lessormq_oldest_processing_seconds{tenant,queue}` | how long the longest lease has been held |
| `lessormq_max_retry_count{tenant,queue}` | the highest attempt count among open messages |
| `lessormq_paused{tenant,queue}` | 1 while the queue is paused |
| `lessormq_workers{tenant,queue}` | workers that claimed in the last minute or hold a message |

Each tenant can scrape its own queues at `GET /v1/metrics`, with a key that has only the
`metrics` scope: the same metrics, for that tenant alone. An operator key reads every
tenant's there, or one tenant's with the `LessorMQ-Tenant` header.

```yaml
scrape_configs:
  - job_name: lessormq
    scheme: https
    metrics_path: /v1/metrics
    authorization:
      credentials_file: /etc/prometheus/lessormq-key
    static_configs:
      - targets: [mq.example.com]
```

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
      - alert: LessorMQDeadLetters
        expr: increase(lessormq_dead_letter_messages[1h]) > 0
        annotations:
          summary: "{{ $labels.tenant }}/{{ $labels.queue }}: messages went to the dead-letter set"
      - alert: LessorMQPausedLong
        expr: lessormq_paused == 1
        for: 1d
        annotations:
          summary: "{{ $labels.tenant }}/{{ $labels.queue }} has been paused for a day"
```

`/healthz` answers 200 while the database and Redis are reachable, 503 otherwise; use it
for readiness checks.
