"""The task backend: enqueue publishes to LessorMQ, and ``get_result`` reads the message
back with the same key (the publish scope may read the messages of its queues).

What happens to a task after a failure (another attempt, when, or the dead-letter set)
is decided by the queue on the server, never here: see the README, "How it behaves".
"""

from __future__ import annotations

import re
import time
from dataclasses import replace
from datetime import datetime
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast
from urllib.parse import quote

import httpx
from django.conf import settings
from django.core import checks
from django.core.exceptions import ImproperlyConfigured
from django.tasks import Task, TaskResult, TaskResultStatus
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import TaskError
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued
from django.utils import timezone
from django.utils.json import normalize_json
from lessormq import ApiError, Client, NotFoundError, RateLimitedError, ServerError, TransportError

from . import _messages
from .exceptions import LeaseExpired, MalformedTask

__all__ = ["LessorMQBackend", "LessorMQTaskResult"]

USER_AGENT = "lessormq-django/1"
_P = ParamSpec("_P")
_R = TypeVar("_R")
_QUEUE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")
_STATUSES = {
    "pending": TaskResultStatus.READY,
    "processing": TaskResultStatus.RUNNING,
    "completed": TaskResultStatus.SUCCESSFUL,
    "failed": TaskResultStatus.FAILED,
}


if TYPE_CHECKING:
    # Generic in django-stubs only.
    _TaskResult = TaskResult[..., Any]
else:
    _TaskResult = TaskResult


class LessorMQTaskResult(_TaskResult):
    """A :class:`~django.tasks.TaskResult` from LessorMQ, which keeps no return values.

    ``return_value`` of a successful result says so instead of answering ``None``, which
    a task may well have returned.
    """

    __slots__ = ()

    @property
    def return_value(self) -> Any:
        if self.status == TaskResultStatus.SUCCESSFUL:
            raise NotImplementedError(
                "LessorMQ does not keep the return values of tasks; store what a task produces yourself"
            )
        return TaskResult.return_value.fget(self)  # type: ignore[attr-defined]


class LessorMQBackend(BaseTaskBackend):
    """Runs Django tasks through LessorMQ queues::

        TASKS = {
            "default": {
                "BACKEND": "lessormq_django.LessorMQBackend",
                "QUEUES": ["default", "mail"],
                "OPTIONS": {"URL": "https://mq.example.com", "API_KEY": "lmq_..."},
            }
        }

    ``manage.py lessormq_worker`` runs the tasks. OPTIONS:

    - ``URL``, ``API_KEY`` (required): the server, and a key with the publish scope (to
      enqueue and read results) and the consume scope (to run a worker);
    - ``QUEUE_PREFIX``: put before each Django queue name to make the LessorMQ queue;
    - ``CLIENT``: keyword arguments for :class:`lessormq.Client` (``timeout``,
      ``http_retries``, ``retry_delay``, ``http``).
    """

    supports_defer = True
    supports_async_task = True
    # LessorMQ hands out messages roughly in order of availability; it has no priority.
    supports_priority = False
    # A message is read back with the publish scope: the result is what the server has.
    supports_get_result = True

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)
        self.url: str = self.options.get("URL") or ""
        self.queue_prefix: str = self.options.get("QUEUE_PREFIX") or ""
        self._api_key: str = self.options.get("API_KEY") or ""
        self._client_options: dict[str, Any] = dict(self.options.get("CLIENT") or {})
        self._client_options.setdefault("user_agent", USER_AGENT)
        self._client: Client | None = None

    def __repr__(self) -> str:
        # Never the keys.
        return f"{type(self).__name__}({self.alias!r}, {self.url!r})"

    @property
    def client(self) -> Client:
        """The LessorMQ client of this backend, for publishing and for workers."""
        if self._client is None:
            if not self.url or not self._api_key:
                raise ImproperlyConfigured(f"Task backend {self.alias!r} needs OPTIONS URL and API_KEY")
            self._client = Client(self.url, self._api_key, **self._client_options)
        return self._client

    def queue_for(self, queue_name: str) -> str:
        """The LessorMQ queue of a Django queue name."""
        return self.queue_prefix + queue_name

    def enqueue(self, task: Task[_P, _R], args: list[Any], kwargs: dict[str, Any]) -> TaskResult[_P, _R]:
        self.validate_task(task)
        # JSON only, as for every Django backend; a value that is not raises here, before
        # anything is published.
        arguments: list[Any] = normalize_json(args)  # type: ignore[assignment]
        keywords: dict[str, Any] = normalize_json(kwargs)  # type: ignore[assignment]

        available_at = None
        if task.run_after is not None:
            available_at = task.run_after
            if timezone.is_naive(available_at):
                # USE_TZ = False: Django's times are local times.
                available_at = timezone.make_aware(available_at)

        published = self.client.publish(
            self.queue_for(task.queue_name),
            _messages.encode(task, arguments, keywords),
            available_at=available_at,
        )
        result = LessorMQTaskResult(
            task=task,
            id=published.id,
            status=TaskResultStatus.READY,
            enqueued_at=_django_time(published.created_at),
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=arguments,
            kwargs=keywords,
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )
        task_enqueued.send(type(self), task_result=result)
        return cast("TaskResult[_P, _R]", result)

    def get_result(self, result_id: str) -> TaskResult[..., Any]:
        """The task's state as the server has it: status, attempts, timestamps and the
        last error.

        :raises TaskResultDoesNotExist: when the message is gone (removed after the
            queue's retention, discarded, deleted) or is not a Django task
        """
        data = self._read_message(result_id)
        if data is None:
            raise TaskResultDoesNotExist(result_id)
        try:
            path, args, kwargs = _messages.decode(data.get("body"))
        except MalformedTask:
            raise TaskResultDoesNotExist(result_id) from None
        return self._result(data, _messages.load(path), args, kwargs)

    def check(self, **kwargs: Any) -> list[checks.CheckMessage]:
        errors: list[checks.CheckMessage] = []
        if not self.url or not self._api_key:
            errors.append(
                checks.Error(
                    f"Task backend {self.alias!r} needs OPTIONS URL and API_KEY.",
                    id="lessormq_django.E001",
                )
            )
        for name in sorted(self.queues):
            if not _QUEUE_NAME.match(self.queue_for(name)):
                errors.append(
                    checks.Error(
                        f"{self.queue_for(name)!r} (queue {name!r} of task backend {self.alias!r}) is not a "
                        "valid LessorMQ queue name: letters, digits, '.', '_' and '-', up to 200.",
                        id="lessormq_django.E002",
                    )
                )
        return errors

    def _result(
        self, data: dict[str, Any], task: Task[..., Any], args: list[Any], kwargs: dict[str, Any]
    ) -> TaskResult[..., Any]:
        queue = str(data.get("queue", ""))
        queue_name = queue[len(self.queue_prefix):] if queue.startswith(self.queue_prefix) else queue
        task = replace(task, queue_name=queue_name, backend=self.alias, run_after=None)

        attempts = int(data.get("attempts") or 0)
        consumer = data.get("consumer") or ""
        status = str(data.get("status"))
        finished = data.get("completed_at") if status == "completed" else data.get("failed_at")
        last_error = data.get("last_error")
        return LessorMQTaskResult(
            task=task,
            id=str(data["id"]),
            status=_STATUSES.get(status, TaskResultStatus.READY),
            enqueued_at=_django_time(data.get("created_at")),
            # The server keeps the claim of the current or last attempt only.
            started_at=_django_time(data.get("started_at")),
            last_attempted_at=_django_time(data.get("started_at")),
            finished_at=_django_time(finished),
            args=args,
            kwargs=kwargs,
            backend=self.alias,
            errors=[_task_error(last_error)] if isinstance(last_error, dict) else [],
            # One entry per attempt, so ``attempts`` is the server's count. The server
            # names the worker of the last claim only; earlier ones stay empty.
            worker_ids=[""] * max(attempts - 1, 0) + [str(consumer)] if attempts else [],
        )

    def _read_message(self, message_id: str) -> dict[str, Any] | None:
        """GET /v1/messages/{id} with the backend's key (its publish scope allows it): not
        one of the protocol calls of lessormq.Client. Retried like those on network
        errors, 5xx and 429."""
        options = self._client_options
        http: httpx.Client = options.get("http") or httpx.Client()
        retries = int(options.get("http_retries", 3))
        delay = float(options.get("retry_delay", 0.1))
        url = self.url.rstrip("/") + "/v1/messages/" + quote(message_id, safe="")
        headers = {
            "Authorization": "Bearer " + self._api_key,
            "Accept": "application/json",
            "User-Agent": str(options.get("user_agent", USER_AGENT)),
        }
        timeout = float(options.get("timeout", 10.0))
        try:
            attempt = 0
            while True:
                attempt += 1
                try:
                    response = http.get(url, headers=headers, timeout=timeout)
                except httpx.HTTPError as e:
                    if attempt > retries:
                        raise TransportError(f"GET /v1/messages/{message_id} failed: {e!s}") from e
                    time.sleep(delay * attempt)
                    continue
                if response.status_code < 400:
                    body: dict[str, Any] = response.json()
                    return body
                error = ApiError.from_response(response.status_code, _json(response), _retry_after(response))
                if isinstance(error, NotFoundError):
                    return None
                if isinstance(error, (ServerError, RateLimitedError)) and attempt <= retries:
                    wait = error.retry_after if isinstance(error, RateLimitedError) else None
                    time.sleep(float(wait) if wait is not None else delay * attempt)
                    continue
                raise error
        finally:
            if "http" not in options:
                http.close()


def _task_error(error: dict[str, Any]) -> TaskError:
    """A recorded error as Django has errors: the exception's class path and a traceback.

    The server keeps the type, the message and the details a worker sent. The Python
    client sends the class (``ValueError``, ``shop.errors.OutOfStock``) and where it was
    raised; a lease that ran out has no type.
    """
    kind = error.get("type")
    if not isinstance(kind, str) or not kind:
        path = f"{LeaseExpired.__module__}.LeaseExpired" if error.get("expired") else "builtins.Exception"
    else:
        path = kind if "." in kind else "builtins." + kind
    lines = ["Traceback (most recent call last):"]
    details = error.get("details")
    if isinstance(details, dict) and "file" in details and "line" in details:
        lines.append(f'  File "{details["file"]}", line {details["line"]}')
    lines.append(f"{path.rsplit('.', 1)[-1]}: {error.get('message', '')}")
    return TaskError(exception_class_path=path, traceback="\n".join(lines) + "\n")


def _django_time(value: Any) -> datetime | None:
    """An API time as Django keeps times: aware with USE_TZ, local and naive without."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        parsed = datetime.fromisoformat(value)
    else:
        return None
    if not settings.USE_TZ:
        return timezone.make_naive(parsed)
    return parsed


def _json(response: httpx.Response) -> Any:
    try:
        return response.json() if response.content else None
    except ValueError:
        return None


def _retry_after(response: httpx.Response) -> int | None:
    value = response.headers.get("Retry-After")
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None
