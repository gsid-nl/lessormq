"""The worker side of a delivery: tells the transport what it came to.

Celery settles a message with an ack or a reject, and acks a task that failed as well
(``task_acks_on_failure_or_timeout``). Only the Request, in the worker's main process,
sees why: the exception, a retry, a lost child process, a time limit. It tells the
transport, which then fails the message (the queue decides what follows), acks it, or
releases it.
"""

from __future__ import annotations

from typing import Any

from billiard.einfo import ExceptionWithTraceback
from celery.exceptions import Ignore, Reject, Retry, Terminated, TimeLimitExceeded, WorkerLostError
from celery.utils.serialization import UnpickleableExceptionWrapper
from celery.worker.request import Request as CeleryRequest

from lessormq import PermanentFailure

from .transport import Outcome

__all__ = ["Request", "ServerRetry", "outcome_of"]


class ServerRetry(Retry):
    """A failed attempt that is not the last: Celery records state RETRY, and the
    transport fails the message. When it runs again is the queue's backoff, not
    ``countdown`` or ``eta``.

    Raised by :meth:`lessormq_celery.Task.retry`, and for any exception of an attempt
    the queue will try again.
    """

    def humanize(self) -> str:
        return "after the queue's backoff"


class Request(CeleryRequest):
    """Celery's Request, telling the transport what each delivery came to."""

    def on_accepted(self, pid: int, time_accepted: float) -> None:
        self._tell("lessormq_running")
        super().on_accepted(pid, time_accepted)

    def on_timeout(self, soft: bool, timeout: float) -> None:
        if not soft:
            # The hard limit killed the child: a failed attempt, whether Celery acks or
            # rejects it next.
            self._tell("lessormq_outcome", Outcome.fail(TimeLimitExceeded(timeout)))
        super().on_timeout(soft, timeout)

    def on_failure(self, exc_info: Any, send_failed_event: bool = True, return_ok: bool = False) -> None:
        outcome = outcome_of(exc_info)
        if outcome is not None:
            self._tell("lessormq_outcome", outcome)
        super().on_failure(exc_info, send_failed_event, return_ok)

    def acknowledge(self) -> None:
        # An ack nothing explained is a success (or an expired task, or Ignore).
        if not self.acknowledged:
            self._tell("lessormq_outcome", Outcome.ack(), replace=False)
        super().acknowledge()

    def reject(self, requeue: bool = False) -> None:
        if not self.acknowledged and requeue:
            self._tell("lessormq_outcome", Outcome.release(), replace=False)
        super().reject(requeue)

    def _tell(self, what: str, *args: Any, **kwargs: Any) -> None:
        message = self.message
        method = getattr(getattr(message, "channel", None), what, None)
        if method is not None:
            method(message.delivery_tag, *args, **kwargs)


def outcome_of(exc_info: Any) -> Outcome | None:
    """What a task's exception means for its message."""
    exc = exc_info.exception
    if isinstance(exc, ExceptionWithTraceback):
        exc = exc.exc
    details = {"traceback": exc_info.traceback} if getattr(exc_info, "traceback", None) else None

    if isinstance(exc, ServerRetry):
        return _fail(exc.exc if isinstance(exc.exc, BaseException) else exc, details)
    if isinstance(exc, Retry):
        # Celery's own retry, kept for its built-in tasks (chord_unlock polls this way):
        # it published the next run itself.
        return Outcome.ack()
    if isinstance(exc, Reject):
        if exc.requeue:
            return Outcome.release()
        if isinstance(exc.reason, BaseException):
            return _fail(exc.reason, details, permanent=True)
        return Outcome.fail(
            str(exc.reason or "rejected"), permanent=True, type="celery.exceptions.Reject", details=details
        )
    if isinstance(exc, Ignore):
        return Outcome.ack()
    if isinstance(exc, Terminated):
        # Revoked with terminate: Celery leaves the message alone; so does the transport.
        return None
    if isinstance(exc, (SystemExit, KeyboardInterrupt)):
        exc = WorkerLostError(f"Worker exited prematurely: {type(exc).__name__}.")
    return _fail(exc, details)


def _fail(error: BaseException, details: Any, *, permanent: bool | None = None) -> Outcome:
    if permanent is None:
        permanent = isinstance(error, PermanentFailure)
    if isinstance(error, UnpickleableExceptionWrapper):
        # The worker could not bring the exception over from the child: its class by name.
        module, name = error.exc_module, error.exc_cls_name
        kind = name if module == "builtins" else f"{module}.{name}"
        return Outcome.fail(error.text or str(error), permanent=permanent, type=kind, details=details)
    return Outcome.fail(error, permanent=permanent, details=details)
