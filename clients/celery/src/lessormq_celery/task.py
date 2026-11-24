"""The base task for Celery on LessorMQ: retries are the queue's.

A failed attempt is reported to the server, which decides whether and when the task
runs again. So ``retry()`` publishes nothing: it raises :class:`ServerRetry`, which the
transport turns into a fail. Any other exception of an attempt the queue will try again
is a :class:`ServerRetry` too, so Celery records state RETRY and calls ``on_retry``; on
the last attempt (or for a permanent failure) the exception stays as it is, and Celery
records FAILURE.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import celery
from celery.exceptions import Ignore, Reject, Retry

from lessormq import PermanentFailure

from .request import ServerRetry

__all__ = ["Task"]

logger = logging.getLogger("lessormq_celery")


class Task(celery.Task):
    """Use it as the app's task class: ``Celery("shop", task_cls=lessormq_celery.Task)``.

    The worker refuses to start with a task that does not derive from it.
    """

    # Ack (or fail) after the task ran, never before: an early ack would complete the
    # message whatever the task does.
    acks_late = True
    # Celery would publish a copy after a lost child process; the server counts that
    # attempt and decides instead.
    retry_on_worker_lost = False
    Request = "lessormq_celery.request:Request"

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if not _from_lessormq(self.request):
            return super().__call__(*args, **kwargs)
        try:
            return super().__call__(*args, **kwargs)
        except (Retry, Ignore, Reject):
            raise
        except Exception as exc:
            if isinstance(exc, PermanentFailure) or _last_attempt(self.request):
                raise
            raise ServerRetry(exc=exc) from exc

    def retry(
        self,
        args: Any = None,
        kwargs: Any = None,
        exc: BaseException | None = None,
        throw: bool = True,
        eta: Any = None,
        countdown: Any = None,
        max_retries: int | None = None,
        **options: Any,
    ) -> Any:
        """Fail this attempt; the queue decides whether and when the task runs again.

        ``countdown``, ``eta``, ``max_retries`` and new ``args`` or ``kwargs`` are
        ignored: the queue's backoff and ``max_attempts`` decide, and the task runs again
        with the arguments it was published with. Raises always, also with
        ``throw=False``, since an attempt that returns is acked and never runs again.
        Outside a LessorMQ worker (called directly, eagerly) and for Celery's own tasks
        (``celery.*``) it is Celery's ``retry``.
        """
        request = self.request
        if not _from_lessormq(request) or self.name.startswith("celery."):
            return super().retry(args, kwargs, exc, throw, eta, countdown, max_retries, **options)
        ignored = [
            name
            for name, value in (
                ("args", args),
                ("kwargs", kwargs),
                ("eta", eta),
                ("countdown", countdown),
                ("max_retries", max_retries),
            )
            if value is not None
        ] + sorted(options)
        if (ignored or not throw) and self.name not in _warned:
            _warned.add(self.name)
            logger.warning(
                "%s.retry(): %s ignored; the queue's backoff and max_attempts decide",
                self.name,
                ", ".join(ignored + ([] if throw else ["throw=False"])),
            )
        if exc is None:
            exc = sys.exc_info()[1]
        if _last_attempt(request):
            # The queue gives up after this attempt: Celery records FAILURE.
            if exc is not None:
                raise exc
            raise self.MaxRetriesExceededError(f"{self.name}[{request.id}]: the last attempt asked for a retry")
        raise ServerRetry(exc=exc)


# Tasks whose ignored retry() arguments were logged, once per process.
_warned: set[str] = set()


def _from_lessormq(request: Any) -> bool:
    """Whether this run is a delivery of the LessorMQ transport, which puts the server's
    attempt counts into the headers."""
    return request is not None and not request.called_directly and request.get("lessormq_attempt") is not None


def _last_attempt(request: Any) -> bool:
    try:
        return int(request.get("lessormq_attempt")) >= int(request.get("lessormq_max_attempts"))
    except (TypeError, ValueError):
        return False
