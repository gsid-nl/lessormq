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

```bash
#!/usr/bin/env bash
# The whole protocol with curl: publish, claim, fail, claim again, ack.
# Needs LESSORMQ_URL and LESSORMQ_KEY (a key with the publish and consume scopes).
set -euo pipefail

url=${LESSORMQ_URL:-http://127.0.0.1:18080}
queue=${LESSORMQ_QUEUE:-examples}
auth="Authorization: Bearer $LESSORMQ_KEY"
json="Content-Type: application/json"

# Publish
curl -sS -X POST "$url/v1/queues/$queue/messages" -H "$auth" -H "$json" \
  -d '{"body": {"to": "ada@example.com"}, "dedup_key": "welcome-ada"}' | jq .

# Claim, waiting up to 5 seconds for a message
lease=$(curl -sS -X POST "$url/v1/queues/$queue/claims" -H "$auth" -H "$json" \
  -d '{"consumer": "curl", "wait_seconds": 5}')
echo "$lease" | jq .
id=$(echo "$lease" | jq -r .message.id)
receipt=$(echo "$lease" | jq -r .receipt)

# The job failed: report it, and let the queue decide what happens next
curl -sS -X POST "$url/v1/messages/$id/fail" -H "$auth" -H "$json" \
  -d "{\"receipt\": \"$receipt\", \"error\": {\"type\": \"SmtpTimeout\", \"message\": \"mail server did not answer\"}}" | jq .

# Claim again once the backoff has passed, and report success this time
sleep "${BACKOFF_WAIT:-11}"
lease=$(curl -sS -X POST "$url/v1/queues/$queue/claims" -H "$auth" -H "$json" -d '{"wait_seconds": 5}')
echo "$lease" | jq '{receipt, attempt: .message.attempt}'
curl -sS -o /dev/null -w "ack: %{http_code}\n" -X POST "$url/v1/messages/$(echo "$lease" | jq -r .message.id)/ack" \
  -H "$auth" -H "$json" -d "{\"receipt\": \"$(echo "$lease" | jq -r .receipt)\"}"
```

[examples/curl/quickstart.sh](https://github.com/gsid-nl/lessormq/blob/main/examples/curl/quickstart.sh)

## PHP

With the [PHP library](https://github.com/gsid-nl/lessormq/blob/main/clients/php/README.md)
(`composer require lessormq/client`). The worker acks when the handler returns and fails
when it throws; it extends the lease on its own while a long handler runs.

```php
<?php

// Publish three messages and work them off with the LessorMQ worker.
// LESSORMQ_URL and LESSORMQ_KEY (publish and consume scopes); composer install first.

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use LessorMQ\Client;
use LessorMQ\Job;
use LessorMQ\Message;
use LessorMQ\Worker;
use LessorMQ\WorkerOptions;

$client = new Client(getenv('LESSORMQ_URL') ?: 'http://127.0.0.1:18080', getenv('LESSORMQ_KEY'));
$queue = getenv('LESSORMQ_QUEUE') ?: 'examples';

foreach ([['n' => 1], ['n' => 2, 'fail_once' => true], ['n' => 3]] as $body) {
    printf("published %s\n", $client->publish($queue, $body)->id);
}

$handled = 0;
$worker = null;
$worker = new Worker($client, $queue, function (Message $message, Job $job) use (&$handled, &$worker): void {
    $n = $message->body['n'];

    if (($message->body['fail_once'] ?? false) && $message->attempt === 1) {
        printf("n=%d attempt=%d failed\n", $n, $message->attempt);
        throw new RuntimeException('temporarily unavailable');
    }

    printf("n=%d attempt=%d handled\n", $n, $message->attempt);

    if (++$handled === 3) {
        $worker->stop();
    }
}, new WorkerOptions(consumer: 'php-example', waitSeconds: 5, maxSeconds: 60));

$worker->run();
printf("done: %d handled\n", $handled);
exit($handled === 3 ? 0 : 1);
```

[examples/php/worker.php](https://github.com/gsid-nl/lessormq/blob/main/examples/php/worker.php)

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

```python
"""Publish three messages and work them off with the LessorMQ worker.

LESSORMQ_URL and LESSORMQ_KEY (publish and consume scopes); `pip install lessormq`.
"""
import os
import sys

from lessormq import Client, Job, Message, Worker

client = Client(os.environ.get("LESSORMQ_URL", "http://127.0.0.1:18080"), os.environ["LESSORMQ_KEY"])
queue = os.environ.get("LESSORMQ_QUEUE", "examples")

for body in ({"n": 1}, {"n": 2, "fail_once": True}, {"n": 3}):
    print("published", client.publish(queue, body).id)

handled = 0


def handle(message: Message, job: Job) -> None:
    global handled
    n = message.body["n"]
    if message.body.get("fail_once") and message.attempt == 1:
        print(f"n={n} attempt={message.attempt} failed")
        raise RuntimeError("temporarily unavailable")

    print(f"n={n} attempt={message.attempt} handled")
    handled += 1
    if handled == 3:
        worker.stop()


worker = Worker(client, queue, handle, consumer="python-example", wait_seconds=5, max_seconds=60)
worker.run()
print(f"done: {handled} handled")
sys.exit(0 if handled == 3 else 1)
```

[examples/python/worker.py](https://github.com/gsid-nl/lessormq/blob/main/examples/python/worker.py)

## TypeScript

The worker from `npm install lessormq`, built on `fetch`: Node 22.6+ (`node worker.ts`),
Bun and Deno. The handler resolves: ack; it throws: fail, as permanent for a
`PermanentFailure` (or any error with `permanent: true`). An `AbortSignal` stops it after
the current message.

```typescript
// Publish three messages and work them off with the LessorMQ worker.
// LESSORMQ_URL and LESSORMQ_KEY (publish and consume scopes); `npm install lessormq`.
// Runs on Node 22.6+ (`node worker.ts`), Bun and Deno.

import { Client, Worker } from "lessormq";

const client = new Client(process.env.LESSORMQ_URL ?? "http://127.0.0.1:18080", process.env.LESSORMQ_KEY!);
const queue = process.env.LESSORMQ_QUEUE ?? "examples";

for (const body of [{ n: 1 }, { n: 2, fail_once: true }, { n: 3 }]) {
  console.log("published", (await client.publish(queue, body)).id);
}

const controller = new AbortController();
process.on("SIGTERM", () => controller.abort());

let handled = 0;
const worker = new Worker<{ n: number; fail_once?: boolean }>(client, queue, async (message) => {
  const { n, fail_once } = message.body;
  if (fail_once && message.attempt === 1) {
    console.log(`n=${n} attempt=${message.attempt} failed`);
    throw new Error("temporarily unavailable");
  }

  console.log(`n=${n} attempt=${message.attempt} handled`);
  if (++handled === 3) controller.abort();
}, { consumer: "ts-example", waitSeconds: 5, maxSeconds: 60, signal: controller.signal });

await worker.run();
console.log(`done: ${handled} handled`);
process.exit(handled === 3 ? 0 : 1);
```

[examples/typescript/worker.ts](https://github.com/gsid-nl/lessormq/blob/main/examples/typescript/worker.ts)

## Go

The worker from `go get github.com/gsid-nl/lessormq-go`, on `net/http` with `context`
throughout. The handler returns nil: ack; an error: fail, as permanent when wrapped with
`lessormq.Permanent`. Cancelling the context stops the worker after the current message;
`Concurrency` runs several handlers side by side.

```go
// Publish three messages and work them off with the LessorMQ worker.
// LESSORMQ_URL and LESSORMQ_KEY (publish and consume scopes); go get github.com/gsid-nl/lessormq-go.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	lessormq "github.com/gsid-nl/lessormq-go"
)

func getenv(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	client := lessormq.New(getenv("LESSORMQ_URL", "http://127.0.0.1:18080"), os.Getenv("LESSORMQ_KEY"))
	queue := getenv("LESSORMQ_QUEUE", "examples")

	for _, body := range []map[string]any{{"n": 1}, {"n": 2, "fail_once": true}, {"n": 3}} {
		published, err := client.Publish(ctx, queue, body, nil)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("published", published.ID)
	}

	var handled atomic.Int32
	worker := lessormq.NewWorker(client, queue, func(ctx context.Context, job *lessormq.Job) error {
		var body struct {
			N        int  `json:"n"`
			FailOnce bool `json:"fail_once"`
		}
		if err := job.Message.Decode(&body); err != nil {
			return lessormq.Permanent(err)
		}
		if body.FailOnce && job.Message.Attempt == 1 {
			fmt.Printf("n=%d attempt=%d failed\n", body.N, job.Message.Attempt)
			return errors.New("temporarily unavailable")
		}
		fmt.Printf("n=%d attempt=%d handled\n", body.N, job.Message.Attempt)
		if handled.Add(1) == 3 {
			cancel()
		}
		return nil
	}, lessormq.WorkerOptions{Consumer: "go-example", WaitSeconds: 5})

	_ = worker.Run(ctx)
	fmt.Printf("done: %d handled\n", handled.Load())
	if handled.Load() != 3 {
		os.Exit(1)
	}
}
```

[examples/go/main.go](https://github.com/gsid-nl/lessormq/blob/main/examples/go/main.go)
