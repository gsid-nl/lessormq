from __future__ import annotations

import time

import httpx

from fake import FAIL_RESULT, LEASE, Fake
from lessormq import AsyncClient, AsyncJob, AsyncWorker, Client, Job, Message, PermanentFailure, Worker

SHORT_LEASE = {**LEASE, "lease_until": "2030-01-01T00:00:30Z"}


def client(fake: Fake) -> Client:
    return Client("https://mq.test", "k", retry_delay=0, http=httpx.Client(transport=fake.transport()))


def worker(fake: Fake, handler, **options) -> Worker:  # type: ignore[no-untyped-def]
    options.setdefault("handle_signals", False)
    options.setdefault("max_messages", 1)
    return Worker(client(fake), "mail", handler, consumer="w1", wait_seconds=1, **options)


def test_a_handler_that_returns_acks() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, LEASE)).on("/v1/messages/m1/ack", (204, None))
    seen: list[Message] = []

    handled = worker(fake, lambda message, job: seen.append(message)).run()

    assert handled == 1
    assert seen[0].body == {"to": "ada@example.com"}
    assert fake.calls("/v1/messages/m1/ack") == [{"receipt": "r1"}]
    assert fake.calls("/v1/queues/mail/claims")[0] == {"consumer": "w1", "wait_seconds": 1}


def test_a_handler_that_raises_fails_and_the_worker_goes_on() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, LEASE)).on("/v1/messages/m1/fail", (200, FAIL_RESULT))

    def handler(message: Message, job: Job) -> None:
        raise PermanentFailure("no recipient")

    assert worker(fake, handler, max_messages=2).run() == 2

    sent = fake.calls("/v1/messages/m1/fail")
    assert len(sent) == 2
    assert sent[0]["permanent"] is True
    assert sent[0]["error"]["type"] == "lessormq._errors.PermanentFailure"


def test_the_lease_is_extended_while_the_handler_runs() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, SHORT_LEASE)).on(
        "/v1/messages/m1/extend", (200, {"lease_until": "2030-01-01T00:01:00Z"})
    ).on("/v1/messages/m1/ack", (204, None))

    worker(fake, lambda message, job: time.sleep(0.35), extend_every=0.1, lease_seconds=60).run()

    extends = fake.calls("/v1/messages/m1/extend")
    assert 2 <= len(extends) <= 4
    assert extends[0] == {"receipt": "r1", "lease_seconds": 60}
    assert len(fake.calls("/v1/messages/m1/ack")) == 1


def test_a_lost_lease_is_not_reported() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, SHORT_LEASE)).on(
        "/v1/messages/m1/extend", (409, {"code": "lease_lost"})
    )

    worker(fake, lambda message, job: time.sleep(0.25), extend_every=0.1).run()

    assert fake.calls("/v1/messages/m1/ack") == []
    assert fake.calls("/v1/messages/m1/fail") == []


def test_ack_lost_is_survived() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, LEASE)).on("/v1/messages/m1/ack", (409, {"code": "lease_lost"}))

    assert worker(fake, lambda message, job: None, max_messages=2).run() == 2


def test_a_claim_failure_pauses_and_goes_on() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (503, {"code": "internal"}), (200, LEASE)).on(
        "/v1/messages/m1/ack", (204, None)
    )
    c = Client("https://mq.test", "k", retry_delay=0, http_retries=0, http=httpx.Client(transport=fake.transport()))

    started = time.monotonic()
    handled = Worker(c, "mail", lambda m, j: None, error_pause=0.2, handle_signals=False, max_messages=1).run()

    assert handled == 1
    assert time.monotonic() - started >= 0.2


def test_stopping_during_a_claim_gives_the_message_back() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, LEASE)).on("/v1/messages/m1/release", (204, None))
    ran: list[Message] = []
    w = worker(fake, lambda message, job: ran.append(message))

    original = fake.handler

    def stop_then_answer(request: httpx.Request) -> httpx.Response:
        w.stop()
        return original(request)

    fake.handler = stop_then_answer  # type: ignore[method-assign]
    w._client = Client("https://mq.test", "k", http=httpx.Client(transport=httpx.MockTransport(stop_then_answer)))

    assert w.run_once() is False
    assert ran == []
    assert fake.calls("/v1/messages/m1/release") == [{"receipt": "r1"}]


def test_max_seconds_ends_the_run() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (204, None))

    started = time.monotonic()
    worker(fake, lambda m, j: None, max_messages=None, max_seconds=0.2).run()

    assert time.monotonic() - started < 2


async def test_async_worker_acks_fails_and_extends() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, SHORT_LEASE)).on(
        "/v1/messages/m1/extend", (200, {"lease_until": "2030-01-01T00:01:00Z"})
    ).on("/v1/messages/m1/ack", (204, None)).on("/v1/messages/m1/fail", (200, FAIL_RESULT))
    calls = 0

    async def handler(message: Message, job: AsyncJob) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            import asyncio

            await asyncio.sleep(0.25)
            return
        raise RuntimeError("second one breaks")

    c = AsyncClient("https://mq.test", "k", retry_delay=0, http=httpx.AsyncClient(transport=fake.async_transport()))
    handled = await AsyncWorker(c, "mail", handler, extend_every=0.1, max_messages=2, handle_signals=False).run()

    assert handled == 2
    assert len(fake.calls("/v1/messages/m1/extend")) >= 1
    assert fake.calls("/v1/messages/m1/ack") == [{"receipt": "r1"}]
    assert fake.calls("/v1/messages/m1/fail")[0]["error"]["type"] == "RuntimeError"


async def test_async_worker_takes_plain_functions() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, LEASE)).on("/v1/messages/m1/ack", (204, None))
    c = AsyncClient("https://mq.test", "k", http=httpx.AsyncClient(transport=fake.async_transport()))

    assert await AsyncWorker(c, "mail", lambda m, j: None, max_messages=1).run() == 1
