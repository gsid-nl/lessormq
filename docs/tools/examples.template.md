---
title: Examples
description: "Complete LessorMQ workers in PHP, Symfony Messenger, Python, TypeScript and Go, and the whole protocol with curl."
---

# Examples

Each example publishes three messages and works them off. The second one fails on its
first attempt, so you see the queue bring it back. Every example runs against a real
server in the repository's test suite (`examples/test.sh`), so what you see here works.

All of them read `LESSORMQ_URL` and `LESSORMQ_KEY` (a key with the publish and consume
scopes) and use the queue `examples`. With the queue's default backoff the failed message
comes back after 10 seconds; set `"backoff": {"initial_seconds": 0}` on the queue to see
it at once.

## curl

The protocol by hand: publish, claim, fail, claim again, ack.

{{ examples/curl/quickstart.sh }}

## PHP

With the [PHP library](https://github.com/gsid-nl/lessormq/blob/main/clients/php/README.md)
(`composer require lessormq/client`). The worker acks when the handler returns and fails
when it throws; it extends the lease on its own while a long handler runs.

{{ examples/php/worker.php }}

## Symfony Messenger

With the [Messenger transport](https://github.com/gsid-nl/lessormq/blob/main/clients/symfony-messenger/README.md)
(`composer require lessormq/messenger`), your existing handlers run unchanged. Leave
retries to the queue:

```yaml
# config/packages/messenger.yaml
framework:
    messenger:
        transports:
            mail:
                dsn: 'lessormq://%env(LESSORMQ_KEY)%@mq.example.com/mail'
                options:
                    wait_seconds: 5
                retry_strategy:
                    max_retries: 0
        routing:
            App\Message\SendWelcomeMail: mail
```

```php
use LessorMQ\Messenger\DedupKeyStamp;

// One open message per user, however often this runs
$bus->dispatch(new SendWelcomeMail($user->id), [new DedupKeyStamp('welcome-' . $user->id)]);
```

Throw `UnrecoverableMessageHandlingException` for input that can never succeed; any other
exception is a failed attempt, and the queue decides what follows. Run
`messenger:consume mail --keepalive` for handlers that take longer than the lease.

## Python

The worker from `pip install lessormq` (Python 3.10+). The handler returns: ack; it
raises: fail, as permanent for a `PermanentFailure`. The lease is extended while it runs.
There is an asyncio version too, `AsyncClient` and `AsyncWorker`.

{{ examples/python/worker.py }}

## TypeScript

The worker from `npm install lessormq`, built on `fetch`: Node 22.6+ (`node worker.ts`),
Bun and Deno. The handler resolves: ack; it throws: fail, as permanent for a
`PermanentFailure` (or any error with `permanent: true`). An `AbortSignal` stops it after
the current message.

{{ examples/typescript/worker.ts }}

## Go

The worker from `go get github.com/gsid-nl/lessormq-go`, on `net/http` with `context`
throughout. The handler returns nil: ack; an error: fail, as permanent when wrapped with
`lessormq.Permanent`. Cancelling the context stops the worker after the current message;
`Concurrency` runs several handlers side by side.

{{ examples/go/main.go }}
