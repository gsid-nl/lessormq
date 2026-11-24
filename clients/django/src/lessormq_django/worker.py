"""The worker: claims messages of the backend's queues and runs the Django task in each.

Built on :class:`lessormq.Worker`, which acks when the task returns, fails when it
raises (permanent for a :class:`~lessormq.PermanentFailure`) and extends the lease while
it runs. Whether and when a failed task runs again is the queue's decision.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import threading
from collections.abc import Callable
from traceback import format_exception
from typing import Any

from django.db import close_old_connections, connections
from django.tasks import TaskContext, TaskResult, TaskResultStatus, task_backends
from django.tasks.base import TaskError
from django.tasks.signals import task_finished, task_started
from django.utils import timezone
from django.utils.json import normalize_json
from lessormq import Job, Message, Worker

from . import _messages
from .backend import LessorMQBackend, _django_time

__all__ = ["TaskHandler", "run_worker"]

logger = logging.getLogger("lessormq_django")


class TaskHandler:
    """The handler of one queue: runs the task a message names, with Django's signals,
    and closes stale database connections around it as a request would."""

    def __init__(self, backend_alias: str, queue_name: str, consumer: str) -> None:
        self.backend_alias = backend_alias
        self.queue_name = queue_name
        self.consumer = consumer

    def __call__(self, message: Message, job: Job) -> None:
        close_old_connections()
        try:
            self.run(message)
        finally:
            close_old_connections()

    def run(self, message: Message) -> Any:
        """Run the task in ``message``; returns what the task returned, raises what it
        raised."""
        path, args, kwargs = _messages.decode(message.body)
        task = _messages.load(path).using(queue_name=self.queue_name, backend=self.backend_alias)
        backend = type(task.get_backend())
        now = timezone.now()
        result: TaskResult[..., Any] = TaskResult(
            task=task,
            id=message.id,
            status=TaskResultStatus.RUNNING,
            enqueued_at=_django_time(message.created_at),
            started_at=now,
            last_attempted_at=now,
            finished_at=None,
            args=args,
            kwargs=kwargs,
            backend=self.backend_alias,
            errors=[],
            # One entry per attempt, so context.attempt is the server's attempt number.
            # The workers of earlier attempts are not known here.
            worker_ids=[""] * (message.attempt - 1) + [self.consumer],
        )
        task_started.send(sender=backend, task_result=result)
        try:
            if task.takes_context:
                value = task.call(TaskContext(task_result=result), *result.args, **result.kwargs)
            else:
                value = task.call(*result.args, **result.kwargs)
            # As in Django's own backends: a return value that is not JSON fails the task.
            object.__setattr__(result, "_return_value", normalize_json(value))
        except Exception as error:
            object.__setattr__(result, "finished_at", timezone.now())
            object.__setattr__(result, "status", TaskResultStatus.FAILED)
            kind = type(error)
            result.errors.append(
                TaskError(
                    exception_class_path=f"{kind.__module__}.{kind.__qualname__}",
                    traceback="".join(format_exception(error)),
                )
            )
            # Inside the except block, so receivers see the exception in sys.exc_info().
            task_finished.send(sender=backend, task_result=result)
            raise
        object.__setattr__(result, "finished_at", timezone.now())
        object.__setattr__(result, "status", TaskResultStatus.SUCCESSFUL)
        task_finished.send(sender=backend, task_result=result)
        return value


def run_worker(
    queues: list[str],
    *,
    backend: str = "default",
    consumer: str | None = None,
    route: str | None = None,
    wait_seconds: int = 20,
    lease_seconds: int | None = None,
    max_tasks: int | None = None,
    max_seconds: float | None = None,
    handle_signals: bool = True,
    on_ready: Callable[[list[Worker]], None] | None = None,
) -> int:
    """Run the tasks of Django queues ``queues`` of task backend ``backend``, a thread per
    queue, until SIGTERM or SIGINT (each worker finishes its current task), or until
    ``max_tasks`` per queue or ``max_seconds``.

    :returns: the tasks run
    """
    instance = task_backends[backend]
    if not isinstance(instance, LessorMQBackend):
        raise TypeError(f"Task backend {backend!r} is not a LessorMQBackend")
    if not queues:
        raise ValueError("name at least one queue")
    for name in queues:
        if instance.queues and name not in instance.queues:
            raise ValueError(f"queue {name!r} is not one of the QUEUES of task backend {backend!r}")

    client = instance.client
    consumer = consumer or default_consumer()
    workers = [
        Worker(
            client,
            instance.queue_for(name),
            TaskHandler(backend, name, consumer),
            consumer=consumer,
            route=route,
            wait_seconds=wait_seconds,
            lease_seconds=lease_seconds,
            max_messages=max_tasks,
            max_seconds=max_seconds,
            # Signals reach the main thread only; it stops every worker.
            handle_signals=False,
        )
        for name in queues
    ]
    counts = [0] * len(workers)
    crashed: list[BaseException] = []

    def work(index: int) -> None:
        try:
            counts[index] = workers[index].run()
        except BaseException as e:
            # Not one queue left unattended in a process that looks alive: stop them all,
            # and fail, so whatever supervises the process starts it again.
            logger.exception("The worker of queue %s stopped; stopping the others", queues[index])
            crashed.append(e)
            for worker in workers:
                worker.stop()
        finally:
            connections.close_all()

    threads = [
        threading.Thread(target=work, args=(i,), name=f"lessormq-{name}", daemon=True)
        for i, name in enumerate(queues)
    ]
    restore = _trap_signals(workers) if handle_signals else None
    try:
        for thread in threads:
            thread.start()
        if on_ready is not None:
            on_ready(workers)
        for thread in threads:
            # A timeout, so a signal handler gets to run between joins.
            while thread.is_alive():
                thread.join(0.5)
    finally:
        if restore is not None:
            restore()
    if crashed:
        raise crashed[0]
    return sum(counts)


def default_consumer() -> str:
    """The host and the process, as the LessorMQ clients name a worker."""
    return f"{socket.gethostname()}:{os.getpid()}"


def _trap_signals(workers: list[Worker]) -> Callable[[], None] | None:
    if threading.current_thread() is not threading.main_thread():
        return None
    stops = [s for s in (getattr(signal, "SIGTERM", None), signal.SIGINT) if s is not None]
    previous = {sig: signal.getsignal(sig) for sig in stops}

    def stop(*_: object) -> None:
        logger.info("Stopping after the current tasks")
        for worker in workers:
            worker.stop()

    for sig in stops:
        signal.signal(sig, stop)

    def restore() -> None:
        for sig, handler in previous.items():
            signal.signal(sig, handler)

    return restore
