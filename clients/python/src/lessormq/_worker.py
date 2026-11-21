"""Workers: claim messages of one queue and run a handler on each.

The handler returns: the message is acked. It raises: the message is failed with the
exception's class and text, as permanent when it is a :class:`PermanentFailure`. What
happens next (another attempt, when, or the dead-letter set) is decided by the queue.
While the handler runs, the lease is extended on a timer.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import signal
import socket
import threading
import time
from collections.abc import Awaitable, Callable
from typing import Any

from ._client import AsyncClient, Client
from ._errors import LeaseLostError, LessorMQError
from ._models import Lease, Message

__all__ = ["AsyncJob", "AsyncWorker", "Job", "Worker"]

_STOP_SIGNALS = tuple(s for s in (getattr(signal, "SIGTERM", None), signal.SIGINT) if s is not None)


class Job:
    """What a handler gets besides the message: the lease, and a way to keep it.

    The worker already extends the lease on a timer; call :meth:`extend` yourself only
    with ``auto_extend=False``, or to ask for a longer lease at once.
    """

    def __init__(self, client: Client, lease: Lease) -> None:
        self._client = client
        self.lease = lease

    def extend(self, lease_seconds: int | None = None) -> None:
        """:raises LeaseLostError: when the lease already ended"""
        self._client.extend(self.lease, lease_seconds)


class AsyncJob:
    """:class:`Job` for :class:`AsyncWorker`."""

    def __init__(self, client: AsyncClient, lease: Lease) -> None:
        self._client = client
        self.lease = lease

    async def extend(self, lease_seconds: int | None = None) -> None:
        """:raises LeaseLostError: when the lease already ended"""
        await self._client.extend(self.lease, lease_seconds)


class _Settings:
    """How a worker claims and keeps its leases. What happens to a failed message is
    the queue's business, so there is nothing about retries here."""

    def __init__(
        self,
        consumer: str | None,
        route: str | None,
        wait_seconds: int,
        lease_seconds: int | None,
        extend_every: float | None,
        auto_extend: bool,
        handle_signals: bool,
        max_messages: int | None,
        max_seconds: float | None,
        error_pause: float,
    ) -> None:
        self.consumer = consumer if consumer is not None else default_consumer()
        self.route = route
        self.wait_seconds = wait_seconds
        self.lease_seconds = lease_seconds
        self.extend_every = extend_every
        self.auto_extend = auto_extend
        self.handle_signals = handle_signals
        self.max_messages = max_messages
        self.max_seconds = max_seconds
        self.error_pause = error_pause

    def interval(self, lease: Lease) -> float:
        return self.extend_every if self.extend_every is not None else max(1.0, lease.seconds_left() / 3)

    def done(self, handled: int, started: float) -> bool:
        if self.max_messages is not None and handled >= self.max_messages:
            return True
        return self.max_seconds is not None and time.monotonic() - started >= self.max_seconds


class Worker:
    """Claims messages of one queue and runs a handler on each, blocking::

        def send(message: Message, job: Job) -> None:
            mailer.send(message.body)

        Worker(client, "mail", send).run()

    The lease is extended from a background thread while the handler runs. On SIGTERM
    or SIGINT (when run from the main thread) the worker stops after the current message.
    """

    def __init__(
        self,
        client: Client,
        queue: str,
        handler: Callable[[Message, Job], Any],
        *,
        consumer: str | None = None,
        route: str | None = None,
        wait_seconds: int = 20,
        lease_seconds: int | None = None,
        extend_every: float | None = None,
        auto_extend: bool = True,
        handle_signals: bool = True,
        max_messages: int | None = None,
        max_seconds: float | None = None,
        error_pause: float = 1.0,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        :param consumer: shown to administrators; the host name by default
        :param route: the path this worker runs jobs through (a VPN exit, a proxy)
        :param wait_seconds: long-poll wait per claim (at most 20)
        :param lease_seconds: lease per claim; the queue's when None
        :param extend_every: seconds between automatic extends; a third of the lease when None
        :param auto_extend: extend the lease while a handler runs
        :param handle_signals: stop after the current message on SIGTERM or SIGINT
        :param max_messages: stop after this many messages
        :param max_seconds: stop after running this long
        :param error_pause: seconds to wait after the server could not be reached
        """
        self._client = client
        self._queue = queue
        self._handler = handler
        self._settings = _Settings(
            consumer, route, wait_seconds, lease_seconds, extend_every, auto_extend,
            handle_signals, max_messages, max_seconds, error_pause,
        )
        self._logger = logger or logging.getLogger("lessormq")
        self._stopping = threading.Event()

    def run(self) -> int:
        """Claim and handle messages until :meth:`stop`, a signal, or a configured limit.

        :returns: messages handled
        """
        self._stopping.clear()
        handled = 0
        started = time.monotonic()
        restore = self._trap_signals() if self._settings.handle_signals else None
        try:
            while not self._stopping.is_set() and not self._settings.done(handled, started):
                if self.run_once():
                    handled += 1
        finally:
            if restore is not None:
                restore()
        return handled

    def run_once(self) -> bool:
        """One claim, and the handler on what it got.

        :returns: whether a message was handled
        """
        settings = self._settings
        try:
            lease = self._client.claim(
                self._queue,
                consumer=settings.consumer,
                route=settings.route,
                wait_seconds=settings.wait_seconds,
                lease_seconds=settings.lease_seconds,
            )
        except LessorMQError as e:
            self._logger.error("Claim failed", extra={"queue": self._queue, "error": str(e)})
            self._stopping.wait(settings.error_pause)
            return False

        if lease is None:
            return False
        if self._stopping.is_set():
            # Asked to stop while the claim waited: give the message back untouched.
            self._report(lambda: self._client.release(lease), lease, "release")
            return False

        self._handle(lease)
        return True

    def stop(self) -> None:
        """Stop after the current message. Safe from any thread or a signal handler."""
        self._stopping.set()

    def _handle(self, lease: Lease) -> None:
        lease_lost = threading.Event()
        done = threading.Event()
        extender = None
        if self._settings.auto_extend:
            extender = threading.Thread(
                target=self._keep, args=(lease, done, lease_lost), name="lessormq-extend", daemon=True
            )
            extender.start()

        error: BaseException | None = None
        try:
            self._handler(lease.message, Job(self._client, lease))
        except Exception as e:
            error = e
        finally:
            done.set()
            if extender is not None:
                extender.join()

        if lease_lost.is_set():
            self._logger.warning(
                "Lease lost while the handler ran; not reporting", extra={"message_id": lease.message.id}
            )
            return
        if error is None:
            self._report(lambda: self._client.ack(lease), lease, "ack")
            return

        def fail() -> None:
            result = self._client.fail(lease, error)
            self._logger.info(
                "Message failed",
                extra={
                    "message_id": lease.message.id,
                    "error": str(error),
                    "outcome": result.outcome,
                    "attempt": result.attempt,
                    "max_attempts": result.max_attempts,
                },
            )

        self._report(fail, lease, "fail")

    def _keep(self, lease: Lease, done: threading.Event, lease_lost: threading.Event) -> None:
        """Extend the lease every interval until the handler is done or the lease is lost."""
        while not done.wait(self._settings.interval(lease)):
            try:
                self._client.extend(lease, self._settings.lease_seconds)
            except LeaseLostError:
                lease_lost.set()
                return
            except LessorMQError as e:
                self._logger.warning("Extend failed; trying again", extra={"error": str(e)})

    def _report(self, call: Callable[[], None], lease: Lease, what: str) -> None:
        """Ack, fail or release, keeping the worker alive whatever happens: a lost lease
        means the message is someone else's now; any other failure means the lease will
        expire and the server will make the message available again."""
        try:
            call()
        except LeaseLostError:
            self._logger.warning(
                f"Lease lost before {what}; the message is no longer ours", extra={"message_id": lease.message.id}
            )
        except LessorMQError as e:
            self._logger.error(
                f"{what.capitalize()} failed; the lease will expire and the message come back",
                extra={"message_id": lease.message.id, "error": str(e)},
            )

    def _trap_signals(self) -> Callable[[], None] | None:
        if threading.current_thread() is not threading.main_thread():
            return None
        previous = {sig: signal.getsignal(sig) for sig in _STOP_SIGNALS}
        for sig in _STOP_SIGNALS:
            signal.signal(sig, lambda *_: self.stop())

        def restore() -> None:
            for sig, handler in previous.items():
                signal.signal(sig, handler)

        return restore


class AsyncWorker:
    """:class:`Worker` for asyncio: the handler is a coroutine function (a plain function
    works too), and the lease is extended by a task while it runs::

        async def send(message: Message, job: AsyncJob) -> None:
            await mailer.send(message.body)

        await AsyncWorker(client, "mail", send).run()

    Several workers can run side by side in one event loop, on one :class:`AsyncClient`.
    """

    def __init__(
        self,
        client: AsyncClient,
        queue: str,
        handler: Callable[[Message, AsyncJob], Awaitable[Any] | Any],
        *,
        consumer: str | None = None,
        route: str | None = None,
        wait_seconds: int = 20,
        lease_seconds: int | None = None,
        extend_every: float | None = None,
        auto_extend: bool = True,
        handle_signals: bool = True,
        max_messages: int | None = None,
        max_seconds: float | None = None,
        error_pause: float = 1.0,
        logger: logging.Logger | None = None,
    ) -> None:
        """The same settings as :class:`Worker`."""
        self._client = client
        self._queue = queue
        self._handler = handler
        self._settings = _Settings(
            consumer, route, wait_seconds, lease_seconds, extend_every, auto_extend,
            handle_signals, max_messages, max_seconds, error_pause,
        )
        self._logger = logger or logging.getLogger("lessormq")
        self._stopping = asyncio.Event()

    async def run(self) -> int:
        """Claim and handle messages until :meth:`stop`, a signal, or a configured limit.

        :returns: messages handled
        """
        self._stopping.clear()
        handled = 0
        started = time.monotonic()
        restore = self._trap_signals() if self._settings.handle_signals else None
        try:
            while not self._stopping.is_set() and not self._settings.done(handled, started):
                if await self.run_once():
                    handled += 1
        finally:
            if restore is not None:
                restore()
        return handled

    async def run_once(self) -> bool:
        """One claim, and the handler on what it got.

        :returns: whether a message was handled
        """
        settings = self._settings
        try:
            lease = await self._client.claim(
                self._queue,
                consumer=settings.consumer,
                route=settings.route,
                wait_seconds=settings.wait_seconds,
                lease_seconds=settings.lease_seconds,
            )
        except LessorMQError as e:
            self._logger.error("Claim failed", extra={"queue": self._queue, "error": str(e)})
            try:
                await asyncio.wait_for(self._stopping.wait(), settings.error_pause)
            except asyncio.TimeoutError:
                pass
            return False

        if lease is None:
            return False
        if self._stopping.is_set():
            await self._report(self._client.release(lease), lease, "release")
            return False

        await self._handle(lease)
        return True

    def stop(self) -> None:
        """Stop after the current message. Call from the event loop's thread."""
        self._stopping.set()

    async def _handle(self, lease: Lease) -> None:
        lease_lost = asyncio.Event()
        keeper = asyncio.ensure_future(self._keep(lease, lease_lost)) if self._settings.auto_extend else None

        error: BaseException | None = None
        try:
            result = self._handler(lease.message, AsyncJob(self._client, lease))
            if inspect.isawaitable(result):
                await result
        except Exception as e:
            error = e
        finally:
            if keeper is not None:
                keeper.cancel()
                try:
                    await keeper
                except asyncio.CancelledError:
                    pass

        if lease_lost.is_set():
            self._logger.warning(
                "Lease lost while the handler ran; not reporting", extra={"message_id": lease.message.id}
            )
            return
        if error is None:
            await self._report(self._client.ack(lease), lease, "ack")
            return

        async def fail() -> None:
            result = await self._client.fail(lease, error)
            self._logger.info(
                "Message failed",
                extra={
                    "message_id": lease.message.id,
                    "error": str(error),
                    "outcome": result.outcome,
                    "attempt": result.attempt,
                    "max_attempts": result.max_attempts,
                },
            )

        await self._report(fail(), lease, "fail")

    async def _keep(self, lease: Lease, lease_lost: asyncio.Event) -> None:
        while True:
            await asyncio.sleep(self._settings.interval(lease))
            try:
                await self._client.extend(lease, self._settings.lease_seconds)
            except LeaseLostError:
                lease_lost.set()
                return
            except LessorMQError as e:
                self._logger.warning("Extend failed; trying again", extra={"error": str(e)})

    async def _report(self, call: Awaitable[None], lease: Lease, what: str) -> None:
        try:
            await call
        except LeaseLostError:
            self._logger.warning(
                f"Lease lost before {what}; the message is no longer ours", extra={"message_id": lease.message.id}
            )
        except LessorMQError as e:
            self._logger.error(
                f"{what.capitalize()} failed; the lease will expire and the message come back",
                extra={"message_id": lease.message.id, "error": str(e)},
            )

    def _trap_signals(self) -> Callable[[], None] | None:
        loop = asyncio.get_running_loop()
        installed = []
        for sig in _STOP_SIGNALS:
            try:
                loop.add_signal_handler(sig, self.stop)
            except (NotImplementedError, RuntimeError, ValueError):
                # Windows, or not the main thread: no signal handling.
                continue
            installed.append(sig)

        def restore() -> None:
            for sig in installed:
                loop.remove_signal_handler(sig)

        return restore


def default_consumer() -> str | None:
    """The host and the process, so the workers on one host are told apart."""
    host = socket.gethostname()
    return f"{host}:{os.getpid()}" if host else None
