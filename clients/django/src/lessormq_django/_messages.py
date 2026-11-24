"""A Django task as a LessorMQ message body, and back.

The body is plain JSON, so a message reads as what it is in the queue and in the
dead-letter set: ``{"task": "shop.tasks.send_invoice", "args": [1042], "kwargs": {}}``.
"""

from __future__ import annotations

from typing import Any

from django.tasks import Task
from django.utils.module_loading import import_string

from .exceptions import MalformedTask, UnknownTask


def encode(task: Task[..., Any], args: list[Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    return {"task": task.module_path, "args": args, "kwargs": kwargs}


def decode(body: Any) -> tuple[str, list[Any], dict[str, Any]]:
    """The task path, args and kwargs in a message body.

    :raises MalformedTask: when the body is not a Django task
    """
    if not isinstance(body, dict):
        raise MalformedTask("the message body is not a JSON object with a task")
    path = body.get("task")
    args = body.get("args", [])
    kwargs = body.get("kwargs", {})
    if not isinstance(path, str) or not path:
        raise MalformedTask("the message body has no task path")
    if not isinstance(args, list) or not isinstance(kwargs, dict):
        raise MalformedTask("the args of the message are not a list, or its kwargs not an object")
    return path, args, kwargs


def load(path: str) -> Task[..., Any]:
    """The task a message names.

    :raises UnknownTask: when this process cannot import it
    """
    try:
        task = import_string(path)
    except ImportError as e:
        raise UnknownTask(f"cannot import task {path}: {e}") from e
    if not isinstance(task, Task):
        raise UnknownTask(f"{path} is not a task (decorate it with @task)")
    return task
