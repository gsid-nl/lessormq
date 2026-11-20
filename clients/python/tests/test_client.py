from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest

from fake import FAIL_RESULT, LEASE, Fake
from lessormq import (
    AsyncClient,
    Client,
    FailResult,
    LeaseLostError,
    PermanentFailure,
    RateLimitedError,
    ServerError,
    TransportError,
    ValidationError,
)

PUBLISHED = {
    "id": "m1",
    "queue": "mail",
    "status": "pending",
    "created_at": "2030-01-01T00:00:00Z",
    "available_at": "2030-01-01T00:00:00Z",
    "duplicate": False,
}


def client(fake: Fake, **options: object) -> Client:
    return Client("https://mq.test/", "lmq_key", retry_delay=0, http=httpx.Client(transport=fake.transport()), **options)  # type: ignore[arg-type]


def test_publish_sends_the_body_and_an_automatic_dedup_key() -> None:
    fake = Fake().on("/v1/queues/mail/messages", (201, PUBLISHED))

    published = client(fake).publish("mail", {"to": "ada@example.com"}, headers={"type": "Welcome"}, max_attempts=3)

    sent = fake.calls("/v1/queues/mail/messages")[0]
    assert sent["body"] == {"to": "ada@example.com"}
    assert sent["headers"] == {"type": "Welcome"}
    assert sent["max_attempts"] == 3
    assert sent["dedup_key"].startswith("lmq-auto-")
    assert "delay_seconds" not in sent
    assert published.id == "m1" and published.duplicate is False
    assert published.available_at == datetime(2030, 1, 1, tzinfo=timezone.utc)
    request = fake.requests[0]
    assert request.headers["Authorization"] == "Bearer lmq_key"
    assert request.headers["User-Agent"].startswith("lessormq-python/")


def test_publish_keeps_null_bodies_and_formats_times_in_utc() -> None:
    fake = Fake().on("/v1/queues/q%2F1/messages", (201, PUBLISHED))
    at = datetime(2030, 1, 1, 2, 0, tzinfo=timezone.utc).astimezone()

    client(fake).publish("q/1", None, dedup_key="k1", available_at=at)

    sent = fake.calls("/v1/queues/q%2F1/messages")[0]
    assert sent == {"body": None, "dedup_key": "k1", "available_at": "2030-01-01T02:00:00Z"}


def test_publish_without_a_dedup_key_is_not_retried() -> None:
    fake = Fake().on("/v1/queues/mail/messages", (503, {"code": "internal"}), (201, PUBLISHED))

    with pytest.raises(ServerError):
        client(fake, auto_dedup_key=False).publish("mail", {})

    assert len(fake.requests) == 1


def test_claim_returns_none_when_nothing_arrived() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (204, None))

    assert client(fake).claim("mail", wait_seconds=5, route="vpn-1", consumer="w1") is None
    assert fake.calls("/v1/queues/mail/claims")[0] == {"wait_seconds": 5, "route": "vpn-1", "consumer": "w1"}


def test_claim_waits_longer_than_the_wait() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (204, None))

    client(fake, timeout=3.0).claim("mail", wait_seconds=20)

    assert fake.requests[0].extensions["timeout"]["read"] == 23.0


def test_claim_returns_the_lease() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, LEASE))

    lease = client(fake).claim("mail")

    assert lease is not None
    assert lease.receipt == "r1"
    assert lease.message.body == {"to": "ada@example.com"}
    assert lease.message.headers == {"type": "Welcome"}
    assert lease.message.attempt == 1 and not lease.message.is_last_attempt
    assert lease.lease_until == datetime(2030, 1, 1, 0, 0, 30, tzinfo=timezone.utc)


def test_extend_moves_the_lease() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, LEASE)).on(
        "/v1/messages/m1/extend", (200, {"lease_until": "2030-01-01T00:05:00Z"})
    )
    c = client(fake)
    lease = c.claim("mail")
    assert lease is not None

    c.extend(lease, 300)

    assert fake.calls("/v1/messages/m1/extend")[0] == {"receipt": "r1", "lease_seconds": 300}
    assert lease.lease_until == datetime(2030, 1, 1, 0, 5, tzinfo=timezone.utc)


def test_fail_reports_the_exception() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, LEASE)).on("/v1/messages/m1/fail", (200, FAIL_RESULT))
    c = client(fake)
    lease = c.claim("mail")
    assert lease is not None

    try:
        raise TimeoutError("mail server slow")
    except TimeoutError as e:
        result = c.fail(lease, e)

    sent = fake.calls("/v1/messages/m1/fail")[0]
    assert sent["receipt"] == "r1"
    assert sent["permanent"] is False
    assert sent["error"]["type"] == "TimeoutError"
    assert sent["error"]["message"] == "mail server slow"
    assert sent["error"]["details"]["file"].endswith("test_client.py")
    assert result == FailResult("retrying", datetime(2030, 1, 1, 0, 0, 10, tzinfo=timezone.utc), 1, 7)


class BadInput(ValueError, PermanentFailure):
    pass


def test_fail_marks_permanent_failures() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, LEASE)).on("/v1/messages/m1/fail", (200, FAIL_RESULT))
    c = client(fake)
    lease = c.claim("mail")
    assert lease is not None

    c.fail(lease, BadInput("no recipient"))
    c.fail(lease, "plain text", details={"step": 2}, type="Custom")

    first, second = fake.calls("/v1/messages/m1/fail")
    assert first["permanent"] is True
    assert first["error"]["type"] == "test_client.BadInput"
    assert "details" not in first["error"]
    assert second == {
        "receipt": "r1",
        "error": {"message": "plain text", "type": "Custom", "details": {"step": 2}},
        "permanent": False,
    }


def test_lease_lost_is_raised() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (200, LEASE)).on(
        "/v1/messages/m1/ack", (409, {"code": "lease_lost", "detail": "The lease ended."})
    )
    c = client(fake)
    lease = c.claim("mail")
    assert lease is not None

    with pytest.raises(LeaseLostError) as caught:
        c.ack(lease)

    assert caught.value.status == 409 and str(caught.value) == "The lease ended."


def test_server_errors_and_network_errors_are_retried() -> None:
    fake = Fake().on(
        "/v1/queues/mail/claims",
        httpx.ConnectError("refused"),
        httpx.Response(502, text="<html>Bad gateway</html>"),
        (200, LEASE),
    )

    assert client(fake).claim("mail") is not None
    assert len(fake.requests) == 3


def test_retries_run_out() -> None:
    fake = Fake().on("/v1/queues/mail/claims", httpx.ConnectError("refused"))

    with pytest.raises(TransportError):
        client(fake, http_retries=2).claim("mail")

    assert len(fake.requests) == 3


def test_rate_limits_wait_for_retry_after() -> None:
    fake = Fake().on("/v1/queues/mail/claims", (429, {"code": "rate_limited"}, {"Retry-After": "0"}))

    with pytest.raises(RateLimitedError) as caught:
        client(fake, http_retries=1).claim("mail")

    assert caught.value.retry_after == 0
    assert len(fake.requests) == 2


def test_validation_errors_carry_the_fields() -> None:
    errors = [{"field": "lease_seconds", "message": "too long"}]
    fake = Fake().on("/v1/queues/mail/claims", (400, {"code": "validation_failed", "errors": errors}))

    with pytest.raises(ValidationError) as caught:
        client(fake).claim("mail", lease_seconds=99999)

    assert caught.value.errors == errors
    assert len(fake.requests) == 1


def test_stats() -> None:
    stats = {"ready": 3, "delayed": 1, "processing": 2, "completed": 9, "failed": 1,
             "oldest_ready_seconds": 12, "oldest_processing_seconds": 4}
    fake = Fake().on("/v1/queues/mail", (200, {"name": "mail", "stats": stats}))

    result = client(fake).stats("mail")

    assert result.ready == 3 and result.failed == 1 and result.oldest_ready_seconds == 12


def test_the_key_stays_out_of_repr() -> None:
    assert "lmq_key" not in repr(client(Fake()))


async def test_async_client_speaks_the_same_protocol() -> None:
    fake = Fake().on("/v1/queues/mail/claims", httpx.ConnectError("refused"), (200, LEASE)).on(
        "/v1/messages/m1/release", (204, None)
    )
    async with AsyncClient(
        "https://mq.test", "lmq_key", retry_delay=0, http=httpx.AsyncClient(transport=fake.async_transport())
    ) as c:
        lease = await c.claim("mail", wait_seconds=1)
        assert lease is not None
        await c.release(lease, delay_seconds=30)

    assert fake.calls("/v1/messages/m1/release")[0] == {"receipt": "r1", "delay_seconds": 30}
    assert len(fake.requests) == 3
