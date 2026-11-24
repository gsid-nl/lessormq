---
title: "LessorMQ for Celery"
description: "Celery on LessorMQ, the message queue over HTTP where the server decides and workers only claim, run, ack or fail."
---

# LessorMQ for Celery

Celery on [LessorMQ](https://lessormq.dev/), the message queue over HTTP where the server
decides and workers only claim, run, ack or fail. It is a kombu transport (broker URL
`lessormq://`) and a base task class. Celery publishes and runs tasks as usual, but
retries, backoff, the number of attempts and the dead-letter set are settings of the
queue on the server: `retry()` and `autoretry_for` fail the attempt and leave the rest
to the queue, and nothing is ever republished by the worker.

The package is `lessormq-celery`, for Celery 5.5 or later on Python 3.10 or later. It
builds on `lessormq`, the
[Python client](https://github.com/gsid-nl/lessormq/tree/main/clients/python).

## Install

```sh
pip install lessormq-celery
```

## Configure

```python
import os

import lessormq_celery  # registers the lessormq:// broker URL
from celery import Celery

app = Celery("shop", task_cls=lessormq_celery.Task)
app.conf.update(
    broker_url="lessormq://mq.example.com",
    broker_transport_options={"api_key": os.environ["LESSORMQ_KEY"]},
    result_backend="redis://localhost/0",  # optional; any backend but rpc://
    worker_prefetch_multiplier=1,
)
```

`lessormq://` speaks HTTPS; `lessormq+http://` plain HTTP, for a server inside the same
cluster. The key may also be the URL's password, `lessormq://:KEY@mq.example.com`, which
Celery masks in its logs. Every task must derive from `lessormq_celery.Task`, which
`task_cls` does for `@app.task` and `@shared_task`; a `base=` class of your own derives
from it too. The worker refuses to start otherwise, naming the tasks.

Transport options (`broker_transport_options`):

| Option | Default | |
|---|---|---|
| `api_key` | the URL's password | a key with the `publish` scope (to send tasks) and the `consume` scope (to run a worker) |
| `wait_seconds` | 5 | how long a claim waits for a message (at most 20) |
| `lease_seconds` | the queue's | lease per claim, and per extension |
| `extend_every` | a third of the lease | seconds between extensions of a held lease |
| `consumer` | host:pid | the worker's name in the server's list of workers |
| `route` | none | the path this worker runs tasks through (VPN exit, proxy, account), see [routes](../concepts.md) |
| `http_retries`, `retry_delay`, `timeout` | 3, 0.1, 10 | the HTTP client's, see the Python client |

There is no option for retries, delays or dead-lettering: set those on the queue
(`PATCH /v1/queues/{queue}`, see [queue settings](../concepts.md)).
A Celery queue is the LessorMQ queue of the same name (`celery` by default).

## Dispatch

```python
from lessormq_celery import PermanentFailure


@app.task(bind=True)
def send_invoice(self, invoice_id: int) -> None:
    invoice = Invoice.objects.filter(pk=invoice_id).first()
    if invoice is None:
        raise PermanentFailure(f"no invoice {invoice_id}")
    mailer.send(invoice)


send_invoice.delay(1042)
send_invoice.apply_async((1042,), countdown=600, queue="mail")
```

- Tasks with the JSON serializer (Celery's default) are stored as JSON, so a message
  reads as what it is in the queue and the dead-letter set: `{"payload": [[1042], {},
  {...}], "headers": {"task": "shop.tasks.send_invoice", ...}}`. Other serializers are
  kept as base64.
- `countdown` and `eta` become the message's `available_at`: the server holds the task
  back, not the worker.
- The task id is the message's dedup key (with its retry count): a publish Celery
  repeats after an error is the same message, and while a task is waiting or running,
  sending the same task id again returns it.
- Canvas works (chains, groups, chords). With a result backend that has no native
  chords, `celery.chord_unlock` polls with Celery's own retry, which publishes its next
  run; that is the one place where a retry is a new message.

## Run workers

```sh
celery -A shop worker -Q celery,mail --concurrency 4
```

The worker is Celery's (tested with the prefork, threads and solo pools). It claims a
message when it has room for one (`worker_prefetch_multiplier` per process), and extends
the lease of every message it holds, running or waiting, from a thread. Keep the
prefetch multiplier at 1: a held message is leased, and no idle worker can have it.

With several queues the worker claims from each in turn, and each claim waits up to
`wait_seconds` on an empty queue: lower it when one worker serves many queues. On SIGTERM
(warm shutdown) the worker finishes the tasks it runs, acks or fails them, and gives the
messages it held but had not started back to the queue without counting an attempt.

## How it behaves

The queue decides everything that happens after a failure; the worker only reports it.

| Celery | LessorMQ |
|---|---|
| a task returns | the message is acked |
| a task raises | the message is failed with the exception's class, message and traceback; the queue's `backoff` and `max_attempts` decide whether and when it runs again, and when it goes to the dead-letter set |
| `PermanentFailure` raised (or mixed into your exception: `class BadInput(ValueError, PermanentFailure)`), or `Reject(requeue=False)` | failed as permanent; the queue's `on_permanent` decides (by default: the dead-letter set at once) |
| `self.retry()`, `autoretry_for` | the attempt fails as above. `countdown`, `eta`, `max_retries`, `retry_backoff` and new arguments are ignored (logged once per task): the task runs again with its arguments after the queue's backoff. `retry()` always raises, also with `throw=False` |
| `max_retries`, `default_retry_delay`, `retry_jitter` | not used: the queue's `max_attempts` and `backoff` |
| `self.request.retries` | the attempts before this one, from the server; `self.request.lessormq_attempt` and `lessormq_max_attempts` are the server's counts |
| state `RETRY` / `FAILURE` in the result backend | `RETRY` (and `on_retry`, `task_retry`) for a failed attempt the queue will try again; `FAILURE` (and `on_failure`, `task_failure`) on the last attempt and for a permanent failure. So `AsyncResult.get()` waits for the outcome of the last attempt |
| `acks_late` | always on: the message is settled after the task ran. The worker refuses a task with it off |
| `acks_on_failure_or_timeout`, `reject_on_worker_lost` | not used: a failure is a fail, whatever Celery would do with the message |
| a child process that dies (`WorkerLostError`), `time_limit` | a failed attempt, counted by the server |
| `retry_on_worker_lost` | refused: the server counts the attempt and decides |
| `soft_time_limit` | the task's own exception, failed as any other |
| `Reject(requeue=True)` | the message is released: back to the queue, the attempt not counted |
| `Ignore`, an expired task (`expires`) | acked |
| a worker killed without a warm shutdown | its leases run out; the server counts the attempts and hands the messages out again |
| a failed-task store | not needed: the queue's dead-letter set (status `failed`) keeps them for the queue's retention; inspect and replay them with the admin API |
| `priority` | ignored: LessorMQ hands out messages roughly in order of availability |
| remote control (`celery inspect`, `revoke`), events, Flower | not carried: LessorMQ has no broadcast. Workers and their messages are in the server's list of workers and the queue's stats |
| `celery purge` | not supported; delete messages with the admin API |
| `result_backend = "rpc://"` | refused: it needs reply queues on the broker. Any other result backend works |

- A message Celery rejects without running it (an unknown task, for one, during a
  deploy) is an ordinary failure, so the next attempt can reach a worker that knows the
  task. A message on the queue that this transport did not write is failed as permanent.
- Delivery is at least once: a task can run twice, when its lease ran out under a slow
  worker or an ack got lost. Make tasks safe to repeat.

## Moving from another queue

- Point `broker_url` at LessorMQ, create the app with `task_cls=lessormq_celery.Task`,
  and derive your own base task classes from it.
- Move the retry policy to the queue: `max_attempts` is `max_retries` plus one (Celery's
  default is 3 retries: 4 attempts), `backoff` replaces `countdown`, `retry_backoff` and
  `retry_jitter`. Exceptions that should never be retried become `PermanentFailure`;
  `autoretry_for` no longer narrows what is retried, since every failure is reported and
  the queue decides.
- `retry()` calls can stay; their arguments other than `exc` stop mattering.
- Replace `rpc://` with another result backend, and remote control (revoking, rate
  limits at runtime) with the admin API: pause a queue, delete or replay messages.
- Run old and new side by side: two Celery apps (or two queues on two brokers), and
  move task routes one at a time. Drain the old broker before you switch it off.
- Workers on different network paths (VPN exits, proxies) set `route`, and the queue's
  `route_policy` keeps a message away from a route it failed on.

## Development

```sh
cd clients/celery
python3 -m venv .venv
.venv/bin/pip install -e ../python -e '.[dev]'
.venv/bin/pytest
.venv/bin/mypy
```

The unit tests run the transport against a scripted server and the task and request
classes without a broker. The end-to-end tests start real Celery workers (prefork) on a
running server, in a tenant of their own that they remove afterwards: a task run and
acked, a failing task back after the queue's backoff with the server's attempt count
and then in the dead-letter set, `retry()` with its countdown ignored, a permanent
failure in the dead-letter set at once, a child process that dies, `countdown`, a warm
shutdown, and a chord. Without these variables they are skipped:

```sh
LESSORMQ_URL=http://127.0.0.1:8080 LESSORMQ_OPERATOR_KEY=lmq_... .venv/bin/pytest tests/test_end_to_end.py
```

The protocol calls are the Python client's, which passes the
[conformance suite](https://github.com/gsid-nl/lessormq/tree/main/conformance) through
`clients/python/tests/conformance_adapter.py`.

_From [clients/celery/README.md](https://github.com/gsid-nl/lessormq/blob/main/clients/celery/README.md)._
