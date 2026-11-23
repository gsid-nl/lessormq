"""The six protocol calls of LessorMQ (docs/protocol.md), nothing more, sync and async.

The client never decides what happens to a message: no retry of a failed job, no delay,
no dead-lettering. It reports, and the queue decides. It does retry its own HTTP calls
when that is safe: network errors, 5xx and 429, for every call that can be repeated
(all of them, given the automatic dedup key on publish).
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import quote

import httpx

from ._errors import ApiError, PermanentFailure, RateLimitedError, ServerError, TransportError
from ._models import FailResult, Lease, Published, Stats, format_time, parse_time

__all__ = ["AsyncClient", "Client"]

USER_AGENT = "lessormq-python/1"
_MAX_ERROR_MESSAGE = 65535
_MAX_ERROR_TYPE = 255


@dataclass(frozen=True)
class _Call:
    """One protocol call as HTTP: what to send, whether it may be sent again, and how
    long it may take."""

    method: str
    path: str
    payload: Any
    repeatable: bool
    extra_timeout: float = 0.0


@dataclass(frozen=True)
class _Response:
    status: int
    body: Any


class _Base:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        http_retries: int,
        retry_delay: float,
        auto_dedup_key: bool,
        timeout: float,
        user_agent: str,
        logger: logging.Logger | None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._http_retries = http_retries
        self._retry_delay = retry_delay
        self._auto_dedup_key = auto_dedup_key
        self._timeout = timeout
        self._user_agent = user_agent
        self._logger = logger or logging.getLogger("lessormq")

    def __repr__(self) -> str:
        # Never the key.
        return f"{type(self).__name__}({self._base_url!r})"

    # The calls, as HTTP. Shared by Client and AsyncClient.

    def _publish_call(
        self,
        queue: str,
        body: Any,
        headers: dict[str, str] | None,
        dedup_key: str | None,
        delay_seconds: int | None,
        available_at: datetime | None,
        max_attempts: int | None,
    ) -> _Call:
        if dedup_key is None and self._auto_dedup_key:
            dedup_key = "lmq-auto-" + secrets.token_hex(12)
        payload: dict[str, Any] = {"body": body}
        if headers:
            payload["headers"] = headers
        if dedup_key is not None:
            payload["dedup_key"] = dedup_key
        if delay_seconds is not None:
            payload["delay_seconds"] = delay_seconds
        if available_at is not None:
            payload["available_at"] = format_time(available_at)
        if max_attempts is not None:
            payload["max_attempts"] = max_attempts
        # Without a dedup key a repeated publish could create a second message.
        return _Call("POST", f"/v1/queues/{_segment(queue)}/messages", payload, dedup_key is not None)

    def _claim_call(
        self,
        queue: str,
        consumer: str | None,
        route: str | None,
        wait_seconds: int,
        lease_seconds: int | None,
        max_messages: int | None = None,
    ) -> _Call:
        payload: dict[str, Any] = {}
        if consumer is not None:
            payload["consumer"] = consumer
        if route is not None:
            payload["route"] = route
        if wait_seconds > 0:
            payload["wait_seconds"] = wait_seconds
        if lease_seconds is not None:
            payload["lease_seconds"] = lease_seconds
        if max_messages is not None:
            payload["max_messages"] = max_messages
        # A claim whose answer got lost leaves a lease that simply expires and counts as
        # an attempt, so claims are retried like the rest.
        return _Call("POST", f"/v1/queues/{_segment(queue)}/claims", payload, True, float(max(wait_seconds, 0)))

    @staticmethod
    def _ack_batch_call(leases: list[Lease]) -> _Call:
        items = [{"message": lease.message.id, "receipt": lease.receipt} for lease in leases]
        return _Call("POST", "/v1/acks", {"leases": items}, True)

    @staticmethod
    def _batch(response: _Response) -> list[Lease]:
        if response.status == 204:
            return []
        return [Lease.from_api(item) for item in response.body.get("items", [])]

    @staticmethod
    def _outcomes(response: _Response, count: int) -> list[str]:
        items = response.body.get("items", [])
        return [str(items[i].get("outcome", "not_found")) if i < len(items) else "not_found" for i in range(count)]

    @staticmethod
    def _extend_call(lease: Lease, lease_seconds: int | None) -> _Call:
        payload: dict[str, Any] = {"receipt": lease.receipt}
        if lease_seconds is not None:
            payload["lease_seconds"] = lease_seconds
        return _Call("POST", _message_path(lease, "extend"), payload, True)

    @staticmethod
    def _ack_call(lease: Lease) -> _Call:
        return _Call("POST", _message_path(lease, "ack"), {"receipt": lease.receipt}, True)

    @staticmethod
    def _fail_call(
        lease: Lease,
        error: BaseException | str,
        permanent: bool | None,
        details: Any,
        type: str | None,
    ) -> _Call:
        if isinstance(error, BaseException):
            text = str(error)
            if type is None:
                type = _type_name(error)
            if details is None:
                details = _origin(error)
            if permanent is None:
                permanent = isinstance(error, PermanentFailure)
        else:
            text = error
        report: dict[str, Any] = {"message": text[:_MAX_ERROR_MESSAGE] or "(no message)"}
        if type is not None:
            report["type"] = type[:_MAX_ERROR_TYPE]
        if details is not None:
            report["details"] = details
        payload = {"receipt": lease.receipt, "error": report, "permanent": bool(permanent)}
        return _Call("POST", _message_path(lease, "fail"), payload, True)

    @staticmethod
    def _release_call(lease: Lease, delay_seconds: int) -> _Call:
        payload: dict[str, Any] = {"receipt": lease.receipt}
        if delay_seconds > 0:
            payload["delay_seconds"] = delay_seconds
        return _Call("POST", _message_path(lease, "release"), payload, True)

    @staticmethod
    def _stats_call(queue: str) -> _Call:
        return _Call("GET", f"/v1/queues/{_segment(queue)}", None, True)

    # Sending, shared parts.

    def _request(self, call: _Call) -> dict[str, Any]:
        headers = {
            "Authorization": "Bearer " + self._api_key,
            "Accept": "application/json",
            "User-Agent": self._user_agent,
        }
        content = None
        if call.payload is not None:
            headers["Content-Type"] = "application/json"
            content = json.dumps(call.payload, ensure_ascii=False, separators=(",", ":")).encode()
        return {
            "method": call.method,
            "url": self._base_url + call.path,
            "headers": headers,
            "content": content,
            "timeout": self._timeout + call.extra_timeout,
        }

    def _outcome(self, call: _Call, response: httpx.Response, attempt: int) -> _Response | tuple[ApiError, float]:
        """The response, or the error and how long to wait before sending again."""
        try:
            body = response.json() if response.content else None
        except ValueError:
            # A proxy's error page, say: the status says enough.
            body = None
        if response.status_code < 400:
            return _Response(response.status_code, body)

        retry_after = _retry_after(response)
        error = ApiError.from_response(response.status_code, body, retry_after)
        # A 429 was not processed at all, so it is always safe to send again.
        retry = isinstance(error, RateLimitedError) or (call.repeatable and isinstance(error, ServerError))
        if retry and attempt <= self._http_retries:
            return error, self._pause(attempt, retry_after, call.path, str(error))
        raise error

    def _transport_failed(self, call: _Call, error: httpx.HTTPError, attempt: int) -> float:
        """How long to wait before sending again, or raises TransportError."""
        if call.repeatable and attempt <= self._http_retries:
            return self._pause(attempt, None, call.path, str(error) or type(error).__name__)
        raise TransportError(f"{call.method} {call.path} failed: {error!s}") from error

    def _pause(self, attempt: int, retry_after: int | None, path: str, reason: str) -> float:
        delay = float(retry_after) if retry_after is not None else self._retry_delay * attempt
        self._logger.warning(
            "LessorMQ request failed, retrying",
            extra={"path": path, "attempt": attempt, "delay": delay, "reason": reason},
        )
        return delay


class Client(_Base):
    """LessorMQ over HTTP, blocking. Thread-safe: a worker's extend timer shares it.

    ``http`` lets you bring your own ``httpx.Client`` (proxies, TLS, transports); its
    timeout is replaced per request.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        http_retries: int = 3,
        retry_delay: float = 0.1,
        auto_dedup_key: bool = True,
        timeout: float = 10.0,
        user_agent: str = USER_AGENT,
        http: httpx.Client | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        :param http_retries: retries after a network error, a 5xx or a 429, for calls that
            are safe to repeat (all but a publish without a dedup key)
        :param retry_delay: base delay between retries in seconds, times the attempt number
        :param auto_dedup_key: give every publish without a dedup key one of its own, so a
            retried publish can never create a second message
        :param timeout: seconds per request; a claim gets its wait on top
        """
        super().__init__(
            base_url,
            api_key,
            http_retries=http_retries,
            retry_delay=retry_delay,
            auto_dedup_key=auto_dedup_key,
            timeout=timeout,
            user_agent=user_agent,
            logger=logger,
        )
        self._owns_http = http is None
        self._http = http or httpx.Client()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def publish(
        self,
        queue: str,
        body: Any,
        *,
        headers: dict[str, str] | None = None,
        dedup_key: str | None = None,
        delay_seconds: int | None = None,
        available_at: datetime | None = None,
        max_attempts: int | None = None,
    ) -> Published:
        """Put a message on a queue.

        With a dedup key (yours, or the automatic one), an open message with that key is
        returned instead of a new one: ``published.duplicate``.
        """
        call = self._publish_call(queue, body, headers, dedup_key, delay_seconds, available_at, max_attempts)
        return Published.from_api(self._send(call).body)

    def claim(
        self,
        queue: str,
        *,
        consumer: str | None = None,
        route: str | None = None,
        wait_seconds: int = 0,
        lease_seconds: int | None = None,
    ) -> Lease | None:
        """Lease the next message of a queue, waiting up to ``wait_seconds`` (at most 20)
        for one. None when nothing became available, or the queue is paused."""
        response = self._send(self._claim_call(queue, consumer, route, wait_seconds, lease_seconds))
        return None if response.status == 204 else Lease.from_api(response.body)

    def extend(self, lease: Lease, lease_seconds: int | None = None) -> Lease:
        """Move the end of the lease to now plus ``lease_seconds`` (or the queue's lease).

        :raises LeaseLostError: when the lease already ended
        """
        response = self._send(self._extend_call(lease, lease_seconds))
        lease.lease_until = parse_time(str(response.body["lease_until"]))
        return lease

    def ack(self, lease: Lease) -> None:
        """Report success.

        :raises LeaseLostError: when the lease already ended
        """
        self._send(self._ack_call(lease))

    def claim_batch(
        self,
        queue: str,
        max_messages: int,
        *,
        consumer: str | None = None,
        route: str | None = None,
        wait_seconds: int = 0,
        lease_seconds: int | None = None,
    ) -> list[Lease]:
        """Lease up to ``max_messages`` (1 to 100) ready messages at once, oldest first, each
        with a receipt of its own. Waits up to ``wait_seconds`` only while there is none;
        an empty list when nothing became available. Report each lease as usual, or ack
        them together with :meth:`ack_batch`."""
        return self._batch(
            self._send(self._claim_call(queue, consumer, route, wait_seconds, lease_seconds, max_messages))
        )

    def ack_batch(self, leases: list[Lease]) -> list[str]:
        """Report success for up to 100 leases in one request. The outcomes are in the order
        of ``leases``: ``acked`` (also when acked before), ``lease_lost``, ``not_found`` or
        ``forbidden``; one that cannot be acked does not hold up the others."""
        return self._outcomes(self._send(self._ack_batch_call(leases)), len(leases))

    def fail(
        self,
        lease: Lease,
        error: BaseException | str,
        *,
        permanent: bool | None = None,
        details: Any = None,
        type: str | None = None,
    ) -> FailResult:
        """Report failure. The server decides what follows and says so in the result.

        :param error: the exception, or a message
        :param permanent: the input itself cannot be processed; by default, whether
            ``error`` is a :class:`PermanentFailure`
        :param details: any JSON value with more context; by default where the exception
            was raised
        :param type: the error type; by default the exception's class
        :raises LeaseLostError: when the lease already ended
        """
        return FailResult.from_api(self._send(self._fail_call(lease, error, permanent, details, type)).body)

    def release(self, lease: Lease, delay_seconds: int = 0) -> None:
        """Give the message back without an outcome; the attempt does not count.

        :raises LeaseLostError: when the lease already ended
        """
        self._send(self._release_call(lease, delay_seconds))

    def stats(self, queue: str) -> Stats:
        """A queue's counts. Needs the consume or admin scope. Not part of the protocol;
        for monitoring."""
        return Stats.from_api(self._send(self._stats_call(queue)).body.get("stats") or {})

    def _send(self, call: _Call) -> _Response:
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self._http.request(**self._request(call))
            except httpx.HTTPError as e:
                time.sleep(self._transport_failed(call, e, attempt))
                continue
            outcome = self._outcome(call, response, attempt)
            if isinstance(outcome, _Response):
                return outcome
            time.sleep(outcome[1])


class AsyncClient(_Base):
    """LessorMQ over HTTP for asyncio. The same calls as :class:`Client`, awaited."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        http_retries: int = 3,
        retry_delay: float = 0.1,
        auto_dedup_key: bool = True,
        timeout: float = 10.0,
        user_agent: str = USER_AGENT,
        http: httpx.AsyncClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(
            base_url,
            api_key,
            http_retries=http_retries,
            retry_delay=retry_delay,
            auto_dedup_key=auto_dedup_key,
            timeout=timeout,
            user_agent=user_agent,
            logger=logger,
        )
        self._owns_http = http is None
        self._http = http or httpx.AsyncClient()

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def publish(
        self,
        queue: str,
        body: Any,
        *,
        headers: dict[str, str] | None = None,
        dedup_key: str | None = None,
        delay_seconds: int | None = None,
        available_at: datetime | None = None,
        max_attempts: int | None = None,
    ) -> Published:
        """See :meth:`Client.publish`."""
        call = self._publish_call(queue, body, headers, dedup_key, delay_seconds, available_at, max_attempts)
        return Published.from_api((await self._send(call)).body)

    async def claim(
        self,
        queue: str,
        *,
        consumer: str | None = None,
        route: str | None = None,
        wait_seconds: int = 0,
        lease_seconds: int | None = None,
    ) -> Lease | None:
        """See :meth:`Client.claim`."""
        response = await self._send(self._claim_call(queue, consumer, route, wait_seconds, lease_seconds))
        return None if response.status == 204 else Lease.from_api(response.body)

    async def extend(self, lease: Lease, lease_seconds: int | None = None) -> Lease:
        """See :meth:`Client.extend`."""
        response = await self._send(self._extend_call(lease, lease_seconds))
        lease.lease_until = parse_time(str(response.body["lease_until"]))
        return lease

    async def ack(self, lease: Lease) -> None:
        """See :meth:`Client.ack`."""
        await self._send(self._ack_call(lease))

    async def claim_batch(
        self,
        queue: str,
        max_messages: int,
        *,
        consumer: str | None = None,
        route: str | None = None,
        wait_seconds: int = 0,
        lease_seconds: int | None = None,
    ) -> list[Lease]:
        """See :meth:`Client.claim_batch`."""
        return self._batch(
            await self._send(self._claim_call(queue, consumer, route, wait_seconds, lease_seconds, max_messages))
        )

    async def ack_batch(self, leases: list[Lease]) -> list[str]:
        """See :meth:`Client.ack_batch`."""
        return self._outcomes(await self._send(self._ack_batch_call(leases)), len(leases))

    async def fail(
        self,
        lease: Lease,
        error: BaseException | str,
        *,
        permanent: bool | None = None,
        details: Any = None,
        type: str | None = None,
    ) -> FailResult:
        """See :meth:`Client.fail`."""
        return FailResult.from_api((await self._send(self._fail_call(lease, error, permanent, details, type))).body)

    async def release(self, lease: Lease, delay_seconds: int = 0) -> None:
        """See :meth:`Client.release`."""
        await self._send(self._release_call(lease, delay_seconds))

    async def stats(self, queue: str) -> Stats:
        """See :meth:`Client.stats`."""
        return Stats.from_api((await self._send(self._stats_call(queue))).body.get("stats") or {})

    async def _send(self, call: _Call) -> _Response:
        attempt = 0
        while True:
            attempt += 1
            try:
                response = await self._http.request(**self._request(call))
            except httpx.HTTPError as e:
                await asyncio.sleep(self._transport_failed(call, e, attempt))
                continue
            outcome = self._outcome(call, response, attempt)
            if isinstance(outcome, _Response):
                return outcome
            await asyncio.sleep(outcome[1])


def _segment(value: str) -> str:
    return quote(value, safe="")


def _message_path(lease: Lease, action: str) -> str:
    return f"/v1/messages/{_segment(lease.message.id)}/{action}"


def _type_name(error: BaseException) -> str:
    cls = type(error)
    return cls.__qualname__ if cls.__module__ == "builtins" else f"{cls.__module__}.{cls.__qualname__}"


def _origin(error: BaseException) -> dict[str, Any] | None:
    """Where the exception was raised: the innermost frame of its traceback."""
    frames = traceback.extract_tb(error.__traceback__) if error.__traceback__ else []
    if not frames:
        return None
    return {"file": frames[-1].filename, "line": frames[-1].lineno}


def _retry_after(response: httpx.Response) -> int | None:
    value = response.headers.get("Retry-After")
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None
