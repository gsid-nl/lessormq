---
title: Quickstart
description: "Run LessorMQ locally with docker compose and take a first message through publish, claim, fail and ack with curl."
---

# Quickstart

A server on your machine, a tenant, and a first message through its whole life, in a few
minutes. You need Docker, curl and jq.

## Start a server

```sh
git clone https://github.com/gsid-nl/lessormq.git && cd lessormq
docker compose up -d --wait
```

The server listens on `http://127.0.0.1:18080`, with MariaDB and Redis next to it.

## Get keys

An operator key manages tenants; a tenant's key works with its queues.

```sh
OPERATOR_KEY=$(docker compose exec -T app lessormq operator-key:create | tail -n 1)

export LESSORMQ_URL=http://127.0.0.1:18080
export LESSORMQ_KEY=$(curl -sS -X POST $LESSORMQ_URL/v1/tenants \
  -H "Authorization: Bearer $OPERATOR_KEY" -H 'Content-Type: application/json' \
  -d '{"name": "Quickstart"}' | jq -r .admin_key.secret)
```

## Publish

```sh
curl -sS -X POST $LESSORMQ_URL/v1/queues/mail/messages \
  -H "Authorization: Bearer $LESSORMQ_KEY" -H 'Content-Type: application/json' \
  -d '{"body": {"to": "ada@example.com"}}' | jq .
```

## Claim

```sh
curl -sS -X POST $LESSORMQ_URL/v1/queues/mail/claims \
  -H "Authorization: Bearer $LESSORMQ_KEY" -H 'Content-Type: application/json' \
  -d '{"wait_seconds": 5}' | tee lease.json | jq .
```

The answer holds the message, a `receipt`, and `lease_until`: until then the message is
yours, and only this receipt can report on it.

## Report

Say it failed, and see what the queue decides:

```sh
curl -sS -X POST $LESSORMQ_URL/v1/messages/$(jq -r .message.id lease.json)/fail \
  -H "Authorization: Bearer $LESSORMQ_KEY" -H 'Content-Type: application/json' \
  -d "{\"receipt\": \"$(jq -r .receipt lease.json)\", \"error\": {\"message\": \"mail server down\"}}" | jq .
```

`"outcome": "retrying"` with an `available_at` ten seconds from now: the queue's
backoff. Claim again after that, and this time ack:

```sh
curl -sS -X POST $LESSORMQ_URL/v1/queues/mail/claims \
  -H "Authorization: Bearer $LESSORMQ_KEY" -H 'Content-Type: application/json' \
  -d '{"wait_seconds": 15}' > lease.json && jq .message.attempt lease.json

curl -sS -w '%{http_code}\n' -X POST $LESSORMQ_URL/v1/messages/$(jq -r .message.id lease.json)/ack \
  -H "Authorization: Bearer $LESSORMQ_KEY" -H 'Content-Type: application/json' \
  -d "{\"receipt\": \"$(jq -r .receipt lease.json)\"}"
```

Attempt 2, and a 204: done.

## Change the rules

The queue's settings decide everything above. Give `mail` five attempts and a gentler
backoff:

```sh
curl -sS -X PATCH $LESSORMQ_URL/v1/queues/mail \
  -H "Authorization: Bearer $LESSORMQ_KEY" -H 'Content-Type: application/json' \
  -d '{"max_attempts": 5, "backoff": {"initial_seconds": 30, "max_seconds": 600}}' | jq .settings
```

Every worker on the queue follows the new rules at once, whatever language it is written
in. Next: [concepts](concepts.md), or a real worker in your language in the
[examples](examples.md).
