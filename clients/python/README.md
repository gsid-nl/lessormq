# LessorMQ for Python

Client and worker for [LessorMQ](https://lessormq.dev), the message queue over HTTP where
the server decides and clients only claim, run, ack or fail. Blocking and asyncio, on
[httpx](https://www.python-httpx.org/), typed.

```sh
pip install lessormq
```

## Publish

```python
from lessormq import Client

client = Client("https://mq.example.com", os.environ["LESSORMQ_KEY"])

client.publish("mail", {"to": "ada@example.com", "template": "welcome"})

client.publish(
    "invoices",
    {"invoice": 1042},
    dedup_key="invoice-1042",   # one open message per key
    delay_seconds=600,
    headers={"type": "InvoiceDue"},
)
```

Every publish carries a dedup key (your own, or one the client makes up), so the client
can safely retry it after a network error without creating a second message.

## Work

```python
from lessormq import Job, Message, PermanentFailure, Worker


def send(message: Message, job: Job) -> None:
    if "to" not in message.body:
        raise PermanentFailure("no recipient")
    send_mail(message.body)


Worker(client, "mail", send, route=os.environ.get("VPN_ID")).run()
```

- The handler returns: the message is acked.
- It raises: the message is failed with the exception's class and text. Raise
  `PermanentFailure`, or mix it into your own exception (`class BadInput(ValueError,
  PermanentFailure)`), when the input itself is unusable.
- What happens after a failure is not up to the worker. The queue's settings decide
  whether and when the message comes back, and when it goes to the dead-letter set.
- While the handler runs, a background thread extends the lease every third of it. A
  handler that outlives its lease anyway is not reported: the message is someone else's
  by then.
- SIGTERM and SIGINT stop the worker after the current message.

`Worker` options: `consumer` (the host name by default), `route`, `wait_seconds` (20),
`lease_seconds`, `extend_every`, `auto_extend`, `handle_signals`, `max_messages`,
`max_seconds`, `error_pause`.

## asyncio

```python
from lessormq import AsyncClient, AsyncJob, AsyncWorker, Message


async def send(message: Message, job: AsyncJob) -> None:
    await mailer.send(message.body)


async def main() -> None:
    async with AsyncClient("https://mq.example.com", os.environ["LESSORMQ_KEY"]) as client:
        await client.publish("mail", {"to": "ada@example.com"})
        await AsyncWorker(client, "mail", send).run()
```

Several `AsyncWorker`s can share one event loop and one client; the lease is extended by
a task while the handler awaits.

## Without the worker

```python
lease = client.claim("mail", wait_seconds=20)
if lease is not None:
    try:
        send_mail(lease.message.body)
        client.ack(lease)
    except Exception as e:
        result = client.fail(lease, e)   # result.outcome: retrying, dead_lettered or discarded
```

`client.extend(lease)` keeps a lease while a long job runs, and `client.release(lease)`
hands a message back without counting the attempt. All of them raise `LeaseLostError`
once the lease has ended: drop the message then, it is someone else's now.

## Errors

Everything the library raises is a `LessorMQError`: `TransportError` when the server
cannot be reached, `ApiError` (with `status`, `code` and `errors`) when it answers with
a problem, in subclasses for the codes a client acts on: `LeaseLostError`,
`ValidationError`, `AuthenticationError`, `ForbiddenError`, `NotFoundError`,
`ConflictError`, `PayloadTooLargeError`, `RateLimitedError` and `ServerError`.

Network errors, 5xx and 429 are retried (`http_retries=3`, `retry_delay=0.1` seconds
times the attempt, or the server's `Retry-After`). That is the only retrying the
library does.

## Conformance

The library passes the LessorMQ conformance suite through
`tests/conformance_adapter.py`:

```sh
LESSORMQ_ADAPTER_CMD="clients/python/.venv/bin/python clients/python/tests/conformance_adapter.py" conformance/run.sh
```
