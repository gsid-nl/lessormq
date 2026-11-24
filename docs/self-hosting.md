---
title: Self-hosting
description: "Running LessorMQ yourself: the image or the binary as a service, configuration, scheduled jobs, sizing, metrics and alerts."
---

# Self-hosting

LessorMQ is one program (Go) with a database for messages (MariaDB, MySQL, PostgreSQL or
SQLite). One node needs nothing else. Several nodes behind a load balancer share their
coordination through Redis; the nodes keep nothing that matters themselves, so you run as
many as you need.

## Requirements

- A database:

  | Database | Version | |
  |---|---|---|
  | MariaDB | 10.6 or newer | where LessorMQ began |
  | MySQL | 8.0.21 or newer | |
  | PostgreSQL | 13 or newer | |
  | SQLite | 3.35 or newer | one machine only: a single node with the file on a local disk. For trying LessorMQ, tests, and small single-host setups |

  Claims use `SKIP LOCKED` on the first three; SQLite, which has one writer at a time,
  takes each claim in a single statement.
- Redis 6 or newer (or Valkey), only for several nodes: a publish on one node wakes a
  claim waiting on another, rate limits hold for a tenant as a whole, the list of workers
  spans every node. Losing Redis loses nothing that matters; without it each node goes on
  with what it knows itself.

## The image

Every release is an image for amd64 and arm64, `ghcr.io/gsid-nl/lessormq:<version>`
(`:0.4` follows the latest 0.4.x, `:latest` the latest release): one static binary,
`lessormq`, that serves HTTP on port 8080 as uid 1000. Put it behind your load balancer or
ingress as it is; there is no FastCGI server or proxy to set up. A claim that waits for
work (`wait_seconds`) costs the node a few kilobytes and the database nothing while it
waits, so many waiting workers need no tuning.

It migrates the schema when it starts. Given a command, the image runs that instead, so the
same image runs the scheduled jobs:

```sh
docker run --rm --env-file lessormq.env ghcr.io/gsid-nl/lessormq:<version> purge-old-messages
```

| Command | |
|---|---|
| `lessormq serve` | the server (the default) |
| `lessormq migrate` | only bring the schema up to date |
| `lessormq operator-key:create --name=...` | make an operator key; prints the secret last |
| `lessormq purge-old-messages` | remove finished messages past their queue's retention |
| `lessormq tenants:reap` | remove the data of deleted tenants |
| `lessormq healthcheck` | exit 0 while this node's `/healthz` answers 200 (for a container health check) |

## Without containers

Every GitHub release also has the binary on its own, the same static build as the image:
`lessormq-<version>-linux-amd64.tar.gz` and `-linux-arm64.tar.gz`, with `SHA256SUMS`
over both. Each tarball holds the binary, an example configuration file (`lessormq.conf`)
and systemd units for the server and the [scheduled jobs](#scheduled-jobs) (`systemd/`);
the same files are in the repository, in
[server/packaging](https://github.com/gsid-nl/lessormq/tree/main/server/packaging). As
root:

```sh
# The latest release; or set v to the one you want.
v=$(curl -fsSL https://api.github.com/repos/gsid-nl/lessormq/releases/latest | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p')
curl -fsSLO https://github.com/gsid-nl/lessormq/releases/download/$v/lessormq-$v-linux-amd64.tar.gz
curl -fsSLO https://github.com/gsid-nl/lessormq/releases/download/$v/SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
tar -xzf lessormq-$v-linux-amd64.tar.gz
cd lessormq-$v-linux-amd64

install -m 0755 lessormq /usr/local/bin/lessormq
useradd --system --home-dir /var/lib/lessormq --shell /usr/sbin/nologin lessormq
install -d -m 0750 -o root -g lessormq /etc/lessormq
install -m 0640 -o root -g lessormq lessormq.conf /etc/lessormq/lessormq.conf
install -m 0644 systemd/* /etc/systemd/system/
```

### The database

LessorMQ needs a database and a user that may change its tables; it makes the tables
itself when it starts. On MariaDB or MySQL:

```sql
CREATE DATABASE lessormq CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'lessormq'@'localhost' IDENTIFIED BY 'secret';
GRANT ALL PRIVILEGES ON lessormq.* TO 'lessormq'@'localhost';
```

On PostgreSQL:

```sql
CREATE ROLE lessormq LOGIN PASSWORD 'secret';
CREATE DATABASE lessormq OWNER lessormq;
```

SQLite needs nothing: the server makes the file, in `/var/lib/lessormq`.

### The configuration file

`/etc/lessormq/lessormq.conf` holds the [configuration](#configuration), one `NAME=value`
per line. Set `DATABASE_QUEUE_URL` to the database (a password with characters such as
`@`, `:` or `/` is percent-encoded there). The file holds that password, so only root and
the service read it:

```ini
# /etc/lessormq/lessormq.conf
# LessorMQ's configuration on a host without containers: NAME=value, one per line, the
# names of the environment variables in docs/self-hosting.md. An environment variable
# that is set overrides the line here. The file holds the database password: 0640,
# root:lessormq.

# The database. MySQL: ?serverVersion=8.4.0; PostgreSQL:
# postgresql://lessormq:secret@127.0.0.1:5432/lessormq; SQLite:
# sqlite:////var/lib/lessormq/lessormq.db
DATABASE_QUEUE_URL=mysql://lessormq:secret@127.0.0.1:3306/lessormq?serverVersion=mariadb-10.11.0

# Only on localhost: a proxy in front of it does TLS.
LISTEN=127.0.0.1:8080

# Only for several nodes.
#REDIS_URL=redis://127.0.0.1:6379/0

# The origins of browser interfaces that call /v1, such as the admin interface.
#CORS_ALLOW_ORIGIN=^https://admin\.example\.com$
```

### The service

```ini
# /etc/systemd/system/lessormq.service
[Unit]
Description=LessorMQ
Documentation=https://lessormq.dev/docs/self-hosting
# Only ordering: a database on this host starts first. The server also waits a minute for
# a database that is not there yet.
After=network-online.target mariadb.service mysql.service postgresql.service redis-server.service valkey.service
Wants=network-online.target

[Service]
User=lessormq
Group=lessormq
ExecStart=/usr/local/bin/lessormq --config /etc/lessormq/lessormq.conf serve
Restart=on-failure
RestartSec=5
# Waiting claims answer at once; the rest finishes within 25 seconds.
TimeoutStopSec=30
# /var/lib/lessormq, for a SQLite database.
StateDirectory=lessormq
StateDirectoryMode=0750
UMask=0027

NoNewPrivileges=yes
CapabilityBoundingSet=
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectKernelLogs=yes
ProtectControlGroups=yes
ProtectClock=yes
ProtectHostname=yes
ProtectProc=invisible
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
SystemCallArchitectures=native
SystemCallFilter=@system-service
SystemCallFilter=~@privileged
SystemCallErrorNumber=EPERM

[Install]
WantedBy=multi-user.target
```

```sh
systemctl daemon-reload
systemctl enable --now lessormq.service lessormq-purge.timer lessormq-reap.timer
curl http://127.0.0.1:8080/healthz
```

The server and its jobs run as `lessormq`, without capabilities, with the system read-only
and home directories out of sight. They write only to `/var/lib/lessormq`, where a SQLite
database goes. A database on the same host starts before them. `lessormq-purge.timer` runs
`purge-old-messages` every 15 minutes and `lessormq-reap.timer` runs `tenants:reap` every
hour (`systemctl list-timers 'lessormq-*'`); with several nodes, enable the timers on one of
them. Logs are JSON lines in the journal: `journalctl -u lessormq`.

A command by hand runs as the same user, and reads the same file:

```sh
sudo -u lessormq lessormq operator-key:create --name=admin
```

### TLS

The server speaks plain HTTP, so a proxy in front of it does TLS. A claim waits up to 20
seconds for work (`wait_seconds`), so the proxy must wait longer than that for an answer;
nginx waits 60 seconds by default. A request body may be up to 16 MiB, more than nginx
takes by default. With nginx, exposing only [what should be exposed](#what-to-expose):

```nginx
server {
    listen 443 ssl;
    server_name mq.example.com;
    ssl_certificate /etc/letsencrypt/live/mq.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/mq.example.com/privkey.pem;
    client_max_body_size 16m;

    location /v1/ {
        proxy_pass http://127.0.0.1:8080;
    }
    location = /healthz {
        proxy_pass http://127.0.0.1:8080;
    }
    location / {
        return 404;
    }
}
```

### Upgrading

Download and check the new release as above, install its binary over the old one and
restart the service; it migrates the schema when it starts (read the release notes first:
a migration of a large table can take a while, see [Configuration](#configuration)). A
node that stops answers its waiting claims at once and finishes the rest of its requests,
and a lease does not belong to a node, so restarting several nodes one after the other
loses no work.

```sh
install -m 0755 lessormq /usr/local/bin/lessormq
systemctl restart lessormq.service
```

The units and the example file in the tarball change rarely; compare them with the
installed ones when they do.

## Configuration

Environment variables, or the same names in a configuration file of `NAME=value` lines:
`lessormq --config FILE`, or `/etc/lessormq/lessormq.conf` when it exists (as in
[Without containers](#without-containers)). A variable set in the environment overrides
the file; a name the file does not know stops the server, so a typo is not ignored.
Blank lines and lines that start with `#` are skipped, a value in quotes loses them, and
nothing else in a value is special.

| Variable | |
|---|---|
| `DATABASE_QUEUE_URL` | the database (see below) |
| `REDIS_URL` | `redis://host:6379/0`, only for several nodes |
| `LISTEN` | where to serve (default `:8080`) |
| `LEASE_SWEEP_SECONDS` | how often, at most, a queue's expired leases are ended (default 60) |
| `MAX_WAITING_CLAIMS` | claims that may wait their full `wait_seconds` at once on a node (default 0: no limit) |
| `CORS_ALLOW_ORIGIN` | a regular expression for the browser origins that may call `/v1` (default `localhost` and `127.0.0.1`); `^$` allows none |
| `DB_MAX_CONNECTIONS` | database connections per node (default 32) |
| `MIGRATE` | `0` to start without migrating (default: migrate) |
| `LOG_LEVEL` | `debug`, `info` (default), `warn` or `error`; logs are JSON on stderr |

`DATABASE_QUEUE_URL` per database; `serverVersion` tells MariaDB from MySQL without asking
the server:

```sh
mysql://user:password@host:3306/lessormq?serverVersion=mariadb-10.11.0
mysql://user:password@host:3306/lessormq?serverVersion=8.4.0
postgresql://user:password@host:5432/lessormq
sqlite:////var/lib/lessormq/lessormq.db
```

The schema started as that of LessorMQ 0.3 and before (the PHP server), and so did the
record of its migrations (`doctrine_migration_versions`): an installation of 0.3 moves to
this server as it is, and gets the migrations since when it starts. A database that
stopped at an older schema than 0.3's is upgraded with 0.3 first. On SQLite the file uses
write-ahead logging, so reading does not wait for writing.

Since 0.3, the times of a message are kept to the millisecond, so the wait and run times
of fast work are measured. On MariaDB and MySQL that migration copies the messages table,
and publishing, claiming and acking wait until it is done: on a large table, deploy it at
a quiet moment, or purge old messages first (`lessormq purge-old-messages`).

## Setting up

```sh
lessormq operator-key:create --name=admin
```

The command prints an operator key once. Use it to create tenants:

```sh
curl -X POST https://mq.example.com/v1/tenants \
  -H "Authorization: Bearer $OPERATOR_KEY" -H 'Content-Type: application/json' \
  -d '{"name": "Acme"}'
```

The answer holds the tenant's first admin key, again shown once. Nodes that start at the
same time migrate one after the other: the first takes a lock, the others find the schema
up to date.

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
python3 -m http.server 8081 --bind 127.0.0.1 --directory examples/admin-ui
```

It calls the server from the browser, so the server must allow its origin:
`CORS_ALLOW_ORIGIN='^https://admin\.example\.com$'` for an interface on
`admin.example.com`.

## Scheduled jobs

| Command | How often | |
|---|---|---|
| `lessormq purge-old-messages` | every 15 minutes | removes finished messages past their queue's retention |
| `lessormq tenants:reap` | hourly | removes the data of deleted tenants |

Without containers, the timers of the systemd units run them (see
[The service](#the-service)). With several nodes, one of them runs them.

## What to expose

Expose `/v1/*` and `/healthz`. Keep `/metrics` for your monitoring only: it takes no key
and lists every tenant's queues. From outside, the same comes from `/v1/metrics` with an
operator key.

## Sizing

- **Application nodes.** A request takes well under a millisecond of CPU. In our load test
  (`tools/loadtest`) two nodes of one CPU each kept up with ten times our own peak (330
  messages a second) at a few milliseconds from publish to ack, and the database ran out
  of CPU long before the nodes did. A claim with `wait_seconds` costs a node a few
  kilobytes while it waits, and the database nothing.
- **The database.** An empty claim is one indexed read; a claim that gets a message is a
  short transaction on one row. The messages table holds open messages plus finished ones for
  their retention, so retention drives its size more than throughput does.
- **Redis.** Small: a few keys per queue, and a channel the nodes tell each other on.
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

`/healthz` answers 200 while the database (and Redis, when there is one) is reachable, 503
otherwise; use it for readiness checks.
