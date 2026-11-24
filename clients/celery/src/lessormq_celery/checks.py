"""The worker refuses to start on LessorMQ with a task that would decide about retries
itself or ack before it ran, or with a result backend that needs broadcast queues."""

from __future__ import annotations

import logging
from typing import Any

from celery.exceptions import ImproperlyConfigured
from celery.signals import worker_init

from .task import Task
from .transport import Transport

logger = logging.getLogger("lessormq_celery")


def problems(app: Any) -> list[str]:
    """What keeps ``app`` from running on LessorMQ, one line each."""
    found = []
    for name, task in sorted(app.tasks.items()):
        if not isinstance(task, Task):
            found.append(
                f"task {name} does not derive from lessormq_celery.Task: create the app with "
                "Celery(..., task_cls=lessormq_celery.Task), and derive a base= class from it"
            )
            continue
        if not task.acks_late:
            found.append(f"task {name} has acks_late off: it would ack its message before it runs")
        if getattr(task, "retry_on_worker_lost", False):
            found.append(f"task {name} has retry_on_worker_lost on: the queue decides about retries")
    backend = str(app.conf.result_backend or "")
    if backend.startswith("rpc"):
        found.append("result_backend rpc:// sends results through broker queues; use another result backend")
    return found


@worker_init.connect(weak=False)
def check_worker(sender: Any = None, **kwargs: Any) -> None:
    app = getattr(sender, "app", None)
    if app is None or not isinstance(app.connection_for_read().transport, Transport):
        return
    found = problems(app)
    if found:
        raise ImproperlyConfigured("Not ready for LessorMQ:\n- " + "\n- ".join(found))
    if (app.conf.worker_prefetch_multiplier or 0) > 1:
        logger.warning(
            "worker_prefetch_multiplier is %s: each process holds that many messages, leased and kept alive, "
            "that idle workers cannot have; 1 is usually better on LessorMQ",
            app.conf.worker_prefetch_multiplier,
        )
