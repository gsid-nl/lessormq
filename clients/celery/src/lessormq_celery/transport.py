"""A kombu transport on LessorMQ, for Celery.

The server decides about every message: when a failed one runs again, how often, and
when it goes to the dead-letter set. This transport only publishes, claims, and settles
each delivery with an ack, a fail or a release. Which of those a delivery comes to is
told by :class:`lessormq_celery.Request`, from what the task did; kombu's ack and reject
alone cannot say that a task failed. While the worker holds a message (running, or
waiting in its prefetch buffer) a thread extends the lease.

    broker_url = "lessormq://mq.example.com"           # HTTPS
    broker_url = "lessormq+http://lessormq:8080"       # plain HTTP, inside a cluster
    broker_transport_options = {"api_key": os.environ["LESSORMQ_KEY"]}
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import socket
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from queue import Empty
from typing import Any
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from kombu.transport import virtual

from lessormq import Client, Lease, LeaseLostError, LessorMQError, RateLimitedError, ServerError, TransportError

__all__ = ["Channel", "MalformedMessage", "Options", "Outcome", "Transport"]

logger = logging.getLogger("lessormq_celery")

USER_AGENT = "lessormq-celery/1"


class MalformedMessage(Exception):
    """A message on the queue that is not a kombu message this transport wrote.

    Reported as permanent: no worker can run it.
    """


@dataclass(frozen=True)
class Outcome:
    """What a delivery comes to: ``ack``, ``fail`` or ``release``.

    For a fail, ``error`` is the exception (or a message, with ``type``), and
    ``permanent`` says the input itself is unusable. The server decides what follows.
    """

    action: str
    error: BaseException | str | None = None
    permanent: bool = False
    type: str | None = None
    details: Any = None

    @staticmethod
    def ack() -> Outcome:
        return Outcome("ack")

    @staticmethod
    def release() -> Outcome:
        return Outcome("release")

    @staticmethod
    def fail(
        error: BaseException | str, *, permanent: bool = False, type: str | None = None, details: Any = None
    ) -> Outcome:
        return Outcome("fail", error, permanent, type, details)


@dataclass
class Options:
    """The server, the key, and how the worker claims and keeps its leases. Retries are
    the queue's business, so there is nothing about them here."""

    url: str
    api_key: str
    wait_seconds: int = 5
    lease_seconds: int | None = None
    extend_every: float | None = None
    consumer: str = ""
    route: str | None = None
    error_pause: float = 1.0
    client: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_connection(cls, connection: Any) -> Options:
        """From the broker URL (``lessormq://``, ``lessormq+http://``, ``lessormq+https://``,
        with the key as the password: ``lessormq://:KEY@host``) and the transport
        options."""
        raw = str(connection.hostname or "")
        if "://" not in raw:
            raw = "https://" + raw
        parts = urlsplit(raw)
        scheme = "http" if parts.scheme == "http" else "https"
        host = parts.hostname or "localhost"
        if ":" in host:
            host = f"[{host}]"
        url = f"{scheme}://{host}" + (f":{parts.port}" if parts.port else "")
        if connection.port and not parts.port:
            url += f":{connection.port}"

        topts = dict(connection.transport_options or {})
        api_key = topts.pop("api_key", None) or (unquote(parts.password) if parts.password else "")
        lease_seconds = topts.pop("lease_seconds", None)
        extend_every = topts.pop("extend_every", None)
        client = {"user_agent": USER_AGENT}
        for name in ("http_retries", "retry_delay", "timeout", "http"):
            if name in topts:
                client[name] = topts.pop(name)
        return cls(
            url=url,
            api_key=str(api_key or ""),
            wait_seconds=int(topts.pop("wait_seconds", 5)),
            lease_seconds=int(lease_seconds) if lease_seconds is not None else None,
            extend_every=float(extend_every) if extend_every is not None else None,
            consumer=str(topts.pop("consumer", None) or f"{socket.gethostname()}:{os.getpid()}"),
            route=topts.pop("route", None),
            error_pause=float(topts.pop("error_pause", 1.0)),
            client=client,
        )


class _Held:
    """A delivery the worker holds: its lease, whether its task runs, and its outcome
    once the Request has told it."""

    def __init__(self, lease: Lease, interval: float) -> None:
        self.lease = lease
        self.interval = interval
        self.next_extend = time.monotonic() + interval
        self.running = False
        self.lost = False
        self.outcome: Outcome | None = None


class Channel(virtual.Channel):
    """Queues are LessorMQ queues; exchanges and bindings live in this process, as in
    every virtual kombu transport."""

    # No broadcast: remote control, events and broadcast queues are not carried.
    supports_fanout = False

    def __init__(self, connection: Any, **kwargs: Any) -> None:
        super().__init__(connection, **kwargs)
        self._transport = connection
        self._held: dict[str, _Held] = {}
        self._lock = threading.Lock()
        self._keeper: threading.Thread | None = None

    @property
    def options(self) -> Options:
        return self._transport.options  # type: ignore[no-any-return]

    @property
    def lessormq(self) -> Client:
        return self._transport.lessormq  # type: ignore[no-any-return]

    # What lessormq_celery.Request tells about a delivery.

    def lessormq_running(self, delivery_tag: str) -> None:
        """The task of this delivery started: when the channel closes before it ends,
        its message is not given back but settled when the task ends."""
        with self._lock:
            held = self._held.get(delivery_tag)
            if held is not None:
                held.running = True

    def lessormq_outcome(self, delivery_tag: str, outcome: Outcome, *, replace: bool = True) -> None:
        """What the delivery comes to, applied when Celery acks or rejects it. With
        ``replace=False`` only when nothing was told before."""
        with self._lock:
            held = self._held.get(delivery_tag)
            if held is not None and (replace or held.outcome is None):
                held.outcome = outcome

    # Publishing.

    def _put(self, queue: str, message: dict[str, Any], **kwargs: Any) -> None:
        body, headers, dedup_key, available_at = encode(message)
        self.lessormq.publish(queue, body, headers=headers, dedup_key=dedup_key, available_at=available_at)

    def _lookup(self, exchange: str, routing_key: str, default: str | None = None) -> list[str]:
        queues: list[str] = super()._lookup(exchange, routing_key, default)
        if not queues and exchange and routing_key and self.typeof(exchange).type == "direct":
            # Bindings are only known in the process that declared them. A direct
            # exchange without one for this key delivers to the queue of that name,
            # rather than dropping the message.
            return [routing_key]
        return queues

    # Consuming.

    def _get(self, queue: str, timeout: float | None = None) -> dict[str, Any]:
        options = self.options
        try:
            lease = self.lessormq.claim(
                queue,
                consumer=options.consumer,
                route=options.route,
                wait_seconds=options.wait_seconds,
                lease_seconds=options.lease_seconds,
            )
        except LessorMQError as e:
            # As the client's Worker: log, pause, claim again. The worker stays up while
            # the server is away; leases it holds run out and the server counts them.
            logger.error("Claim from %s failed: %s", queue, e)
            time.sleep(options.error_pause)
            raise Empty() from None
        if lease is None:
            raise Empty()

        tag = uuid4().hex
        try:
            raw = decode(lease, tag)
        except MalformedMessage as e:
            logger.error("Message %s is not a kombu message; failing it as permanent", lease.message.id)
            self._settle(_Held(lease, 0), Outcome.fail(e, permanent=True))
            raise Empty() from None
        self._hold(tag, lease)
        return raw

    def basic_ack(self, delivery_tag: str, multiple: bool = False) -> None:
        held = self._take(delivery_tag)
        if held is not None:
            self._settle(held, held.outcome or Outcome.ack())
        super().basic_ack(delivery_tag, multiple)

    def basic_reject(self, delivery_tag: str, requeue: bool = False) -> None:
        held = self._take(delivery_tag)
        if held is not None:
            default = Outcome.release() if requeue else _rejected()
            self._settle(held, held.outcome or default)
        # Settled above: kombu must not put the message back itself.
        super().basic_reject(delivery_tag, requeue=False)

    def _restore(self, message: Any) -> None:
        """kombu gives back what the worker holds when the channel closes (shutdown,
        lost connection): a release, so the attempt does not count. A message whose task
        is running stays held, and is settled when the task ends."""
        tag = message.delivery_tag
        with self._lock:
            held = self._held.get(tag)
            if held is None or held.running:
                return
            del self._held[tag]
        self._settle(held, held.outcome or Outcome.release())

    def _size(self, queue: str) -> int:
        try:
            return self.lessormq.stats(queue).ready
        except LessorMQError:
            return 0

    def _purge(self, queue: str) -> int:
        raise NotImplementedError("LessorMQ does not purge waiting messages; delete them with the admin API")

    def _delete(self, queue: str, *args: Any, **kwargs: Any) -> None:
        # A queue exists from its first message on, and stays.
        pass

    def _has_queue(self, queue: str, **kwargs: Any) -> bool:
        return True

    # Leases.

    def _hold(self, tag: str, lease: Lease) -> None:
        interval = self.options.extend_every or max(1.0, lease.seconds_left() / 3)
        with self._lock:
            self._held[tag] = _Held(lease, interval)
            if self._keeper is None:
                self._keeper = threading.Thread(target=self._keep, name="lessormq-extend", daemon=True)
                self._keeper.start()

    def _take(self, tag: str) -> _Held | None:
        with self._lock:
            return self._held.pop(tag, None)

    def _keep(self) -> None:
        """Extend every held lease when it is due, until none is held."""
        while True:
            now = time.monotonic()
            with self._lock:
                if not self._held:
                    self._keeper = None
                    return
                due = [held for held in self._held.values() if not held.lost and held.next_extend <= now]
            for held in due:
                try:
                    self.lessormq.extend(held.lease, self.options.lease_seconds)
                    held.next_extend = time.monotonic() + held.interval
                except LeaseLostError:
                    held.lost = True
                except LessorMQError as e:
                    logger.warning("Extend failed; trying again: %s", e)
                    held.next_extend = time.monotonic() + min(held.interval, 1.0)
            time.sleep(0.2)

    def _settle(self, held: _Held, outcome: Outcome) -> None:
        """Ack, fail or release, keeping the worker alive whatever happens: a lost lease
        means the message is someone else's now; any other failure means the lease will
        expire and the server will make the message available again."""
        lease = held.lease
        if held.lost:
            logger.warning(
                "Lease of %s lost before %s; the message is no longer ours", lease.message.id, outcome.action
            )
            return
        try:
            if outcome.action == "ack":
                self.lessormq.ack(lease)
            elif outcome.action == "release":
                self.lessormq.release(lease)
            else:
                error = outcome.error if outcome.error is not None else "(no message)"
                result = self.lessormq.fail(
                    lease, error, permanent=outcome.permanent, type=outcome.type, details=outcome.details
                )
                logger.info(
                    "Message %s failed (%s), attempt %d of %d: %s",
                    lease.message.id,
                    result.outcome,
                    result.attempt,
                    result.max_attempts,
                    error,
                )
        except LeaseLostError:
            logger.warning(
                "Lease of %s lost before %s; the message is no longer ours", lease.message.id, outcome.action
            )
        except LessorMQError as e:
            logger.error(
                "%s of %s failed; the lease will expire and the message come back: %s",
                outcome.action.capitalize(),
                lease.message.id,
                e,
            )


class Transport(virtual.Transport):
    """kombu transport ``lessormq``, registered when :mod:`lessormq_celery` is imported."""

    Channel = Channel

    driver_type = "lessormq"
    driver_name = "lessormq"
    # The whole URL reaches the transport, which reads the scheme, host and key itself.
    can_parse_url = True
    # Claims wait on the server (wait_seconds); this pause only applies while the
    # worker's prefetch buffer is full.
    polling_interval = 0.2

    implements = virtual.Transport.implements.extend(
        asynchronous=False,
        exchange_type=frozenset(["direct", "topic"]),
        heartbeats=False,
    )

    # Celery retries a publish on these, as the client already did (with the same dedup
    # key, so never twice).
    connection_errors = virtual.Transport.connection_errors + (  # type: ignore[assignment]
        TransportError,
        ServerError,
        RateLimitedError,
    )

    def __init__(self, client: Any, **kwargs: Any) -> None:
        super().__init__(client, **kwargs)
        self.options = Options.from_connection(client)
        self._lessormq: Client | None = None
        self._lessormq_lock = threading.Lock()

    @property
    def lessormq(self) -> Client:
        """The LessorMQ client of this connection, shared by its channels."""
        with self._lessormq_lock:
            if self._lessormq is None:
                if not self.options.api_key:
                    raise ValueError(
                        "LessorMQ needs an API key: broker_transport_options['api_key'], or the URL's password "
                        "(lessormq://:KEY@host)"
                    )
                self._lessormq = Client(self.options.url, self.options.api_key, **self.options.client)
            return self._lessormq

    def driver_version(self) -> str:
        from lessormq import __version__

        return __version__


def encode(message: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str], str | None, datetime | None]:
    """A kombu message as a LessorMQ message: the body, the headers, the dedup key and
    when it becomes available.

    A JSON task stays JSON in the body (``payload``), so it reads as what it is in the
    queue and the dead-letter set; any other body is kept as base64 (``body``).
    """
    properties = dict(message.get("properties") or {})
    encoding = properties.pop("body_encoding", None)
    properties.pop("delivery_tag", None)
    properties.pop("delivery_info", None)
    raw = message.get("body")
    data: bytes
    if encoding == "base64":
        data = base64.b64decode(raw or b"")
    elif isinstance(raw, str):
        data = raw.encode(message.get("content-encoding") or "utf-8")
    else:
        data = bytes(raw or b"")

    headers = dict(message.get("headers") or {})
    content_type = message.get("content-type")
    content_encoding = message.get("content-encoding")
    body: dict[str, Any] = {
        "content-type": content_type,
        "content-encoding": content_encoding,
        "headers": headers,
        "properties": properties,
    }
    payload = _json(data, content_type, content_encoding, headers)
    if payload is not _NOT_JSON:
        body["payload"] = payload
    else:
        body["body"] = base64.b64encode(data).decode("ascii")

    shown = {}
    task, task_id = headers.get("task"), headers.get("id")
    if isinstance(task, str):
        shown["task"] = task
    if isinstance(task_id, str):
        shown["task_id"] = task_id
    # One open message per task id and retry: a publish Celery repeats after an error
    # is the same message, a retry of its own built-in tasks a new one.
    dedup_key = f"celery-{task_id}-{headers.get('retries') or 0}" if isinstance(task_id, str) else None
    if dedup_key is not None and len(dedup_key) > 255:
        dedup_key = None
    return body, shown, dedup_key, _eta(headers.get("eta"))


def decode(lease: Lease, delivery_tag: str) -> dict[str, Any]:
    """A claimed LessorMQ message as the raw message kombu delivers.

    The attempt counts of the server go into the headers: ``retries`` (the attempts
    before this one, which Celery shows as ``task.request.retries``), and
    ``lessormq_attempt`` and ``lessormq_max_attempts``.

    :raises MalformedMessage: when it is not a message this transport wrote
    """
    message = lease.message
    data = message.body
    if not isinstance(data, dict) or not ("payload" in data or "body" in data):
        raise MalformedMessage("the message body is not a kombu message")
    headers = data.get("headers") or {}
    properties = data.get("properties") or {}
    if not isinstance(headers, dict) or not isinstance(properties, dict):
        raise MalformedMessage("the headers or properties of the message are not objects")
    if "payload" in data:
        body = json.dumps(data["payload"]).encode("utf-8")
    else:
        try:
            body = base64.b64decode(str(data["body"]), validate=True)
        except (binascii.Error, ValueError) as e:
            raise MalformedMessage(f"the message body is not base64: {e}") from e

    try:
        published_retries = int(headers.get("retries") or 0)
    except (TypeError, ValueError):
        published_retries = 0
    headers = {
        **headers,
        # The attempts before this one, on top of the retries Celery counted when it
        # published (only its own tasks, such as chord_unlock, publish their retries).
        "retries": published_retries + max(message.attempt - 1, 0),
        "lessormq_attempt": message.attempt,
        "lessormq_max_attempts": message.max_attempts,
        "lessormq_message_id": message.id,
    }
    return {
        "body": base64.b64encode(body).decode("ascii"),
        "content-type": data.get("content-type"),
        "content-encoding": data.get("content-encoding"),
        "headers": headers,
        "properties": {
            **properties,
            "body_encoding": "base64",
            "delivery_tag": delivery_tag,
            "delivery_info": {
                "exchange": "",
                "routing_key": message.queue,
                "redelivered": message.attempt > 1,
            },
        },
    }


_NOT_JSON = object()


def _json(data: bytes, content_type: Any, content_encoding: Any, headers: dict[str, Any]) -> Any:
    if content_type != "application/json" or headers.get("compression"):
        return _NOT_JSON
    try:
        return json.loads(data.decode(content_encoding or "utf-8"))
    except (UnicodeDecodeError, LookupError, ValueError):
        return _NOT_JSON


def _eta(value: Any) -> datetime | None:
    """Celery's eta (from ``countdown`` or ``eta``) as the message's ``available_at``:
    the server holds the message back, not the worker."""
    if not isinstance(value, str) or not value:
        return None
    try:
        eta = datetime.fromisoformat(value)
    except ValueError:
        return None
    # A naive eta is the publisher's local time, as Celery reads it.
    return eta if eta.tzinfo is not None else eta.astimezone()


def _rejected() -> Outcome:
    """A reject the Request did not explain: Celery turned the message down without
    running it (an unknown task, an invalid message). An ordinary failure, so a worker
    that knows the task can still get it; the worker's log says why."""
    return Outcome.fail(
        "the worker rejected the message without running it (unknown task or invalid message); see its log",
        type="celery.exceptions.Reject",
    )
