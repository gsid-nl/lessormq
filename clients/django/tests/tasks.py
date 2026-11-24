"""Tasks for the tests. What they saw goes into ``seen``."""

from __future__ import annotations

from typing import Any

from django.tasks import TaskContext, task

from lessormq_django import PermanentFailure

seen: list[Any] = []


@task
def add(a: int, b: int) -> int:
    seen.append(("add", a, b))
    return a + b


@task(queue_name="mail")
def send(to: str, *, template: str = "welcome") -> None:
    seen.append(("send", to, template))


@task(takes_context=True)
def attempt(context: TaskContext) -> int:
    seen.append(("attempt", context.attempt, context.task_result.id))
    return context.attempt


@task(takes_context=True)
def flaky(context: TaskContext, marker: str) -> None:
    seen.append(("flaky", marker, context.attempt))
    raise ConnectionError(f"upstream down ({marker})")


@task
def unusable(marker: str) -> None:
    seen.append(("unusable", marker))
    raise PermanentFailure(f"no such invoice ({marker})")


@task
async def asynchronous(value: str) -> str:
    seen.append(("asynchronous", value))
    return value


@task
def not_json() -> object:
    return object()


def plain() -> None:
    """Not a task."""
