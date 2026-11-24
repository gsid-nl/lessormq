from __future__ import annotations

import base64
import json
import time
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest
from kombu import Connection, Exchange, Queue

import lessormq_celery  # noqa: F401  (registers the lessormq scheme)
from lessormq_celery.transport import MalformedMessage, Options, Outcome, decode, encode
from lessormq import Lease
from tests.fake import FAIL_RESULT, Fake

PUBLISHED = {
    "id": "m1",
    "queue": "celery",
    "status": "pending",
    "created_at": "2030-01-01T00:00:00Z",
    "available_at": "2030-01-01T00:00:00Z",
    "duplicate": False,
}


def kombu_message(payload: Any, **headers: Any) -> dict[str, Any]:
    return {
        "content-type": "application/json",
        "content-encoding": "utf-8",
        "headers": {"task": "shop.add", "id": "t1", "retries": 0, **headers},
        "properties": {"correlation_id": "t1", "priority": 0},
        "payload": payload,
    }


def lease(body: Any, *, attempt: int = 1, lease_until: str = "2030-01-01T00:05:00Z") -> dict[str, Any]:
    return {
        "receipt": "r1",
        "lease_until": lease_until,
        "message": {
            "id": "m1",
            "queue": "celery",
            "body": body,
            "headers": {},
            "attempt": attempt,
            "max_attempts": 3,
            "created_at": "2030-01-01T00:00:00Z",
        },
    }


def connect(fake: Fake, **options: Any) -> Connection:
    return Connection(
        "lessormq://:secret@mq.test",
        transport_options={"http": httpx.Client(transport=fake.transport()), "retry_delay": 0, **options},
    )


@pytest.mark.parametrize(
    ("url", "options", "expected"),
    [
        ("lessormq://:k1@mq.example.com", {}, ("https://mq.example.com", "k1")),
        ("lessormq+https://:k1@mq.example.com:8443/", {}, ("https://mq.example.com:8443", "k1")),
        ("lessormq+http://lessormq:8080", {"api_key": "k2"}, ("http://lessormq:8080", "k2")),
        ("lessormq://:k%2F1@mq.example.com", {}, ("https://mq.example.com", "k/1")),
    ],
)
def test_the_url_names_the_server_and_the_key(url: str, options: dict[str, Any], expected: tuple[str, str]) -> None:
    connection = Connection(url, transport_options=options)
    parsed = connection.transport.options

    assert (parsed.url, parsed.api_key) == expected
    assert "k1" not in connection.as_uri()


def test_the_options_are_read_from_the_transport_options() -> None:
    connection = Connection(
        "lessormq://mq.example.com",
        transport_options={"api_key": "k", "wait_seconds": 2, "lease_seconds": 60, "route": "vpn-2", "timeout": 3},
    )
    options: Options = connection.transport.options

    assert (options.wait_seconds, options.lease_seconds, options.route) == (2, 60, "vpn-2")
    assert options.client["timeout"] == 3
    assert ":" in options.consumer


def test_without_a_key_the_client_cannot_be_made() -> None:
    with pytest.raises(ValueError, match="API key"):
        Connection("lessormq://mq.example.com").transport.lessormq


def test_a_json_task_stays_json_in_the_message() -> None:
    raw = {
        "body": base64.b64encode(b'[[1, 2], {}, {"callbacks": null}]').decode(),
        "content-type": "application/json",
        "content-encoding": "utf-8",
        "headers": {"task": "shop.add", "id": "t1", "retries": 0, "eta": "2030-01-01T12:00:00+02:00"},
        "properties": {"body_encoding": "base64", "delivery_tag": "x", "delivery_info": {}, "priority": 0},
    }

    body, headers, dedup_key, available_at = encode(raw)

    assert body["payload"] == [[1, 2], {}, {"callbacks": None}]
    assert "body" not in body
    assert body["properties"] == {"priority": 0}
    assert headers == {"task": "shop.add", "task_id": "t1"}
    assert dedup_key == "celery-t1-0"
    assert available_at == datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)


def test_any_other_body_is_kept_as_base64() -> None:
    raw = {
        "body": base64.b64encode(b"\x80\x04pickled").decode(),
        "content-type": "application/x-python-serialize",
        "content-encoding": "binary",
        "headers": {},
        "properties": {"body_encoding": "base64"},
    }

    body, headers, dedup_key, available_at = encode(raw)

    assert base64.b64decode(body["body"]) == b"\x80\x04pickled"
    assert "payload" not in body
    assert (headers, dedup_key, available_at) == ({}, None, None)


def test_a_claimed_message_carries_the_servers_attempts() -> None:
    claimed = Lease.from_api(lease(kombu_message([[1, 2], {}, {}]), attempt=3))

    raw = decode(claimed, "tag-1")

    assert json.loads(base64.b64decode(raw["body"])) == [[1, 2], {}, {}]
    assert raw["headers"]["retries"] == 2
    # Celery's own retries (of its built-in tasks) were counted at publish: on top.
    republished = Lease.from_api(lease(kombu_message([[], {}, {}], retries=4), attempt=2))
    assert decode(republished, "tag-2")["headers"]["retries"] == 5
    assert raw["headers"]["lessormq_attempt"] == 3
    assert raw["headers"]["lessormq_max_attempts"] == 3
    assert raw["properties"]["delivery_tag"] == "tag-1"
    assert raw["properties"]["delivery_info"] == {"exchange": "", "routing_key": "celery", "redelivered": True}
    assert raw["properties"]["correlation_id"] == "t1"


@pytest.mark.parametrize("body", [{"to": "ada@example.com"}, "text", {"body": "not base64!"}])
def test_a_message_this_transport_did_not_write_is_malformed(body: Any) -> None:
    with pytest.raises(MalformedMessage):
        decode(Lease.from_api(lease(body)), "tag")


def test_publish_and_consume_through_kombu() -> None:
    fake = Fake().on("/v1/queues/celery/messages", (201, PUBLISHED))
    fake.on("/v1/queues/celery/claims", (200, lease(kombu_message({"to": "ada"}))))
    fake.on("/v1/messages/m1/ack", (204, None))
    received: list[Any] = []

    with connect(fake, wait_seconds=1) as connection:
        queue = Queue("celery", Exchange("celery", "direct"), "celery")
        with connection.Producer() as producer:
            producer.publish({"to": "ada"}, exchange=queue.exchange, routing_key="celery", declare=[queue])
        with connection.Consumer(queue, callbacks=[lambda body, message: received.append((body, message))]):
            connection.drain_events(timeout=2)
            received[0][1].ack()

    sent = fake.calls("/v1/queues/celery/messages")[0]
    assert sent["body"]["payload"] == {"to": "ada"}
    assert fake.calls("/v1/queues/celery/claims")[0]["wait_seconds"] == 1
    assert received[0][0] == {"to": "ada"}
    assert fake.calls("/v1/messages/m1/ack") == [{"receipt": "r1"}]
    assert fake.requests[0].headers["Authorization"] == "Bearer secret"


def test_a_direct_exchange_without_bindings_delivers_to_the_queue_of_the_key() -> None:
    fake = Fake().on("/v1/queues/mail/messages", (201, PUBLISHED))

    with connect(fake) as connection, connection.Producer() as producer:
        producer.publish({}, exchange=Exchange("celery", "direct"), routing_key="mail")

    assert len(fake.calls("/v1/queues/mail/messages")) == 1


def claim_one(fake: Fake, connection: Connection, **lease_fields: Any) -> Any:
    fake.on("/v1/queues/celery/claims", (200, lease(kombu_message([[], {}, {}]), **lease_fields)), (204, None))
    channel = connection.default_channel
    message = channel.basic_get("celery")
    assert message is not None
    return channel, message


def test_a_told_failure_is_a_fail_whether_celery_acks_or_rejects() -> None:
    fake = Fake().on("/v1/messages/m1/fail", (200, FAIL_RESULT))
    with connect(fake) as connection:
        channel, message = claim_one(fake, connection)
        channel.lessormq_outcome(message.delivery_tag, Outcome.fail(ValueError("bad"), details={"traceback": "tb"}))
        channel.lessormq_outcome(message.delivery_tag, Outcome.ack(), replace=False)
        message.ack()

    sent = fake.calls("/v1/messages/m1/fail")
    assert sent == [
        {
            "receipt": "r1",
            "error": {"message": "bad", "type": "ValueError", "details": {"traceback": "tb"}},
            "permanent": False,
        }
    ]
    assert fake.calls("/v1/messages/m1/ack") == []


def test_a_reject_nothing_explained_is_an_ordinary_failure() -> None:
    fake = Fake().on("/v1/messages/m1/fail", (200, FAIL_RESULT))
    with connect(fake) as connection:
        channel, message = claim_one(fake, connection)
        message.reject()

    sent = fake.calls("/v1/messages/m1/fail")[0]
    assert sent["permanent"] is False
    assert sent["error"]["type"] == "celery.exceptions.Reject"


def test_a_requeue_is_a_release() -> None:
    fake = Fake().on("/v1/messages/m1/release", (204, None))
    with connect(fake) as connection:
        channel, message = claim_one(fake, connection)
        message.reject(requeue=True)

    assert fake.calls("/v1/messages/m1/release") == [{"receipt": "r1"}]


def test_closing_releases_what_waits_and_keeps_what_runs() -> None:
    fake = Fake().on("/v1/messages/m1/release", (204, None)).on("/v1/messages/m1/ack", (204, None))
    connection = connect(fake)
    channel, waiting = claim_one(fake, connection)
    connection.close()
    assert fake.calls("/v1/messages/m1/release") == [{"receipt": "r1"}]

    fake = Fake().on("/v1/messages/m1/release", (204, None)).on("/v1/messages/m1/ack", (204, None))
    connection = connect(fake)
    channel, running = claim_one(fake, connection)
    channel.lessormq_running(running.delivery_tag)
    connection.close()
    assert fake.calls("/v1/messages/m1/release") == []
    # The task ends after the channel closed: its outcome still reaches the server.
    channel.basic_ack(running.delivery_tag)
    assert fake.calls("/v1/messages/m1/ack") == [{"receipt": "r1"}]


def test_a_lost_lease_is_not_reported() -> None:
    fake = Fake().on("/v1/messages/m1/ack", (409, {"code": "lease_lost"}))
    with connect(fake) as connection:
        channel, message = claim_one(fake, connection)
        message.ack()

    assert len(fake.calls("/v1/messages/m1/ack")) == 1


def test_held_leases_are_extended_until_settled() -> None:
    fake = Fake().on("/v1/messages/m1/extend", (200, {"lease_until": "2030-01-01T00:10:00Z"}))
    fake.on("/v1/messages/m1/ack", (204, None))
    with connect(fake, extend_every=0.1, lease_seconds=30) as connection:
        channel, message = claim_one(fake, connection)
        time.sleep(0.75)
        message.ack()
        extends = len(fake.calls("/v1/messages/m1/extend"))
        time.sleep(0.5)

    assert 2 <= extends <= 6
    assert len(fake.calls("/v1/messages/m1/extend")) == extends
    assert fake.calls("/v1/messages/m1/extend")[0] == {"receipt": "r1", "lease_seconds": 30}


def test_a_malformed_message_is_failed_as_permanent_and_skipped() -> None:
    fake = Fake().on("/v1/queues/celery/claims", (200, lease({"to": "ada"})), (204, None))
    fake.on("/v1/messages/m1/fail", (200, FAIL_RESULT))
    with connect(fake) as connection:
        assert connection.default_channel.basic_get("celery") is None

    sent = fake.calls("/v1/messages/m1/fail")[0]
    assert sent["permanent"] is True
    assert sent["error"]["type"] == "lessormq_celery.transport.MalformedMessage"


def test_a_failed_claim_is_logged_and_the_worker_goes_on() -> None:
    fake = Fake().on("/v1/queues/celery/claims", (401, {"code": "unauthorized"}))
    with connect(fake, error_pause=0) as connection:
        assert connection.default_channel.basic_get("celery") is None
