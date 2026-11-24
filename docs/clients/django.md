---
title: "LessorMQ for Django"
description: "A backend for Django's tasks framework (django.tasks) on LessorMQ, the message queue over HTTP where the server decides and workers only claim, run, ack or fail."
---

# LessorMQ for Django

A backend for Django's tasks framework (`django.tasks`) on [LessorMQ](../../README.md),
the message queue over HTTP where the server decides and workers only claim, run, ack or
fail. `enqueue()` publishes a task to a LessorMQ queue and `manage.py lessormq_worker`
runs it. Retries, backoff and the dead-letter set are settings of the queue on the
server, not of Django or of the worker.

The package is `lessormq-django`. It needs Django 6.0 or later (the first with
`django.tasks`), and so Python 3.12 or later, and builds on `lessormq`, the
[Python client](https://github.com/gsid-nl/lessormq/tree/main/clients/python).

## Install

```sh
pip install lessormq-django
```

Add the app, for its management command:

```python
INSTALLED_APPS = [
    # ...
    "lessormq_django",
]
```

## Configure

```python
TASKS = {
    "default": {
        "BACKEND": "lessormq_django.LessorMQBackend",
        "QUEUES": ["default", "mail"],
        "OPTIONS": {
            "URL": "https://mq.example.com",
            "API_KEY": os.environ["LESSORMQ_KEY"],
            "QUEUE_PREFIX": "shop-",
        },
    }
}
```

| Option | Default | |
|---|---|---|
| `URL` | required | the server |
| `API_KEY` | required | a key with the `publish` scope (to enqueue and read results) and the `consume` scope (to run workers); web processes and workers may have different keys |
| `QUEUE_PREFIX` | `""` | put before each Django queue name: queue `mail` is LessorMQ queue `shop-mail` |
| `CLIENT` | `{}` | keyword arguments for `lessormq.Client`: `timeout`, `http_retries`, `retry_delay`, `http` (an `httpx.Client` of your own) |

There is no option for retries, delays or dead-lettering: set those on the queue
(`PATCH /v1/queues/{queue}`, see [queue settings](../concepts.md)).
`manage.py check` reports a backend without `URL` or `API_KEY`, and a queue name that
LessorMQ does not accept (letters, digits, `.`, `_` and `-`).

## Dispatch

```python
from django.db import transaction
from django.tasks import task
from lessormq_django import PermanentFailure


@task(queue_name="mail")
def send_invoice(invoice_id: int) -> None:
    invoice = Invoice.objects.filter(pk=invoice_id).first()
    if invoice is None:
        raise PermanentFailure(f"no invoice {invoice_id}")
    mailer.send(invoice)


result = send_invoice.enqueue(1042)
result.id  # the LessorMQ message id
```

- Arguments are JSON, as for every Django backend. The message body is plain JSON, so
  it reads as what it is in the queue and in the dead-letter set:
  `{"task": "shop.tasks.send_invoice", "args": [1042], "kwargs": {}}`.
- `send_invoice.using(run_after=timezone.now() + timedelta(minutes=10)).enqueue(1042)`
  holds the task back: `run_after` becomes the message's `available_at`.
- Enqueue after the transaction that wrote the data commits, or a worker may run the
  task before the row exists: `transaction.on_commit(lambda: send_invoice.enqueue(invoice.pk))`.
- `await send_invoice.aenqueue(1042)` from async code; async tasks (`async def`) run too.
- Every enqueue carries a dedup key the client makes up, so a publish it retries after
  a network error never creates a second message.

## Run workers

```sh
python manage.py lessormq_worker
python manage.py lessormq_worker --queue mail --route vpn-2
```

| Option | Default | |
|---|---|---|
| `--backend` | `default` | the task backend in `TASKS` |
| `--queue` | every queue in `QUEUES` | a Django queue name; repeat it for more |
| `--consumer` | host:pid | the worker's name in the server's list of workers |
| `--route` | none | the path this worker runs tasks through (VPN exit, proxy, account), see [routes](../concepts.md) |
| `--wait-seconds` | 20 | long-poll wait per claim, at most 20 |
| `--lease-seconds` | the queue's | lease per claim, and per extension |
| `--max-tasks` | none | stop each queue's worker after this many tasks |
| `--max-seconds` | none | stop after running this long |

Each queue gets a thread that runs its tasks one at a time; run more processes for more
at once. SIGTERM and SIGINT stop the worker after the tasks it is running. A claim that
is waiting finishes its wait first, so give the process a grace period of
`--wait-seconds` plus the longest task (in Kubernetes, `terminationGracePeriodSeconds`).

## How it behaves

The worker is the Python client's `Worker`. The queue decides everything that happens
after a failure; the worker only reports it.

| Django | LessorMQ |
|---|---|
| a task returns | the message is acked |
| a task raises | the message is failed with the exception's class and message; the queue's `backoff` and `max_attempts` decide whether and when it runs again, and when it goes to the dead-letter set |
| `PermanentFailure` raised (or mixed into your exception: `class BadInput(ValueError, PermanentFailure)`) | failed as permanent; the queue's `on_permanent` decides (by default: the dead-letter set at once) |
| retries, backoff, max attempts | none in Django: the queue's settings are the only ones |
| `context.attempt` (`takes_context=True`) | the server's attempt number, counting this one |
| `run_after` | the message's `available_at` |
| `priority` | not supported: a priority other than 0 raises `InvalidTask`. LessorMQ hands out messages roughly in order of availability |
| `queue_name` | the LessorMQ queue, after `QUEUE_PREFIX` |
| timeouts | none in Django. The worker extends the lease every third of it while a task runs; a worker that dies stops extending, the lease runs out, the attempt counts and another worker gets the message |
| failed tasks | the queue's dead-letter set (status `failed`), kept for the queue's retention; inspect and replay them with the admin API |
| signals | `task_enqueued` on enqueue; `task_started` and `task_finished` for every attempt, also an attempt the queue will try again |

- A message that is not a Django task (not a JSON object with a task path, a list of
  args and an object of kwargs) is failed as permanent, with type
  `lessormq_django.exceptions.MalformedTask`.
- A task this worker cannot import is an ordinary failure (`UnknownTask`), not a
  permanent one: during a deploy a worker with the old code can claim a task only the
  new code has, and the next attempt can land on a worker that knows it.
- A return value that is not JSON fails the task, as in Django's own backends. The
  value itself is not kept anywhere.
- Database connections are closed around each task when they are stale or broken, as
  around a request (`CONN_MAX_AGE`).
- Delivery is at least once: a task can run twice, when its lease ran out under a slow
  worker or an ack got lost. Make tasks safe to repeat.

### Results

`get_result()` and `refresh()` read the message back with `API_KEY`: the `publish` scope
may read the messages of its queues. The result is the message as the server has it:

| `TaskResult` | from the message |
|---|---|
| `status` | `pending`: `READY`, `processing`: `RUNNING`, `completed`: `SUCCESSFUL`, `failed`: `FAILED` (the dead-letter set) |
| `attempts`, `worker_ids` | one entry per attempt; the server names the worker of the last claim only, the others are empty |
| `errors` | the last error only, with the class the worker reported and where it was raised; an attempt whose lease ran out is `lessormq_django.LeaseExpired` |
| `enqueued_at` | `created_at` |
| `started_at`, `last_attempted_at` | when the current or last attempt was claimed; none while the task waits for its next attempt |
| `finished_at` | `completed_at` or `failed_at` |
| `return_value` | LessorMQ keeps no return values: a successful result raises `NotImplementedError` (instead of answering `None`). Store what a task produces yourself |

A message that is gone (after the queue's retention, discarded, or deleted) raises
`TaskResultDoesNotExist`.

## Moving from another queue

- From Celery: `@shared_task` becomes `@task`, `.delay()` becomes `.enqueue()`,
  `apply_async(countdown=60)` becomes `.using(run_after=timezone.now() + timedelta(seconds=60)).enqueue()`.
  Drop `autoretry_for`, `max_retries`, `retry_backoff` and `self.retry()`: let the task
  raise, and set the queue's `max_attempts` and `backoff` to what they did.
  `AsyncResult.get()` has no counterpart: a task that produces something stores it.
- From another `django.tasks` backend: change `BACKEND`, and run `lessormq_worker`
  instead of that backend's worker. Retry settings of that backend move to the queue.
- Side by side: configure both backends in `TASKS` (say `"default"` the old one and
  `"lessormq"` this one) and move tasks one at a time, with `@task(backend="lessormq")`
  or `.using(backend="lessormq")`. Remove the old backend once its queue is empty.
- Workers on different network paths (VPN exits, proxies) pass `--route`, and the queue's
  `route_policy` keeps a message away from a route it failed on.

## Development

```sh
cd clients/django
python3 -m venv .venv
.venv/bin/pip install -e ../python -e '.[dev]'
.venv/bin/pytest
.venv/bin/mypy
```

The unit tests run the backend and the worker against a scripted server. The end-to-end
tests run against a real server, in a tenant of their own that they remove afterwards:
a task run and acked, a failing task back after the queue's backoff with the server's
attempt count and then in the dead-letter set, a permanent failure there at once, and
`run_after`. Without these variables they are skipped:

```sh
LESSORMQ_URL=http://127.0.0.1:8080 LESSORMQ_OPERATOR_KEY=lmq_... .venv/bin/pytest tests/test_end_to_end.py
```

The protocol calls are the Python client's, which passes the
[conformance suite](https://github.com/gsid-nl/lessormq/tree/main/conformance) through
`clients/python/tests/conformance_adapter.py`.

_From [clients/django/README.md](https://github.com/gsid-nl/lessormq/blob/main/clients/django/README.md)._
