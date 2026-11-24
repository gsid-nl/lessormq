"""Against a running server: a tenant of its own, the worker in this process.

    LESSORMQ_URL=http://127.0.0.1:8080 LESSORMQ_OPERATOR_KEY=lmq_... pytest tests/test_end_to_end.py

Skipped without those two variables.
"""

from __future__ import annotations

import os
import secrets
import time
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import httpx
import pytest
from django.tasks import TaskResultStatus
from django.tasks.signals import task_started
from django.utils import timezone

from lessormq import PermanentFailure
from lessormq_django.worker import run_worker
from tests import tasks

URL = os.environ.get("LESSORMQ_URL", "")
OPERATOR_KEY = os.environ.get("LESSORMQ_OPERATOR_KEY", "")
BACKOFF = 2

pytestmark = pytest.mark.skipif(not (URL and OPERATOR_KEY), reason="needs LESSORMQ_URL and LESSORMQ_OPERATOR_KEY")


@pytest.fixture(scope="module")
def api_key() -> Iterator[str]:
    """The admin key of a tenant made for these tests, removed afterwards."""
    tenant = "django-" + secrets.token_hex(4)
    with httpx.Client(base_url=URL, headers={"Authorization": "Bearer " + OPERATOR_KEY}) as operator:
        created = operator.post("/v1/tenants", json={"id": tenant, "name": "lessormq-django tests"})
        created.raise_for_status()
        yield created.json()["admin_key"]["secret"]
        operator.patch(f"/v1/tenants/{tenant}", json={"status": "suspended"}).raise_for_status()
        operator.delete(f"/v1/tenants/{tenant}").raise_for_status()


@pytest.fixture(scope="module")
def app_key(api_key: str) -> str:
    """What the app runs with: publish and consume, no admin, so results are read with
    the publish scope."""
    with httpx.Client(base_url=URL, headers={"Authorization": "Bearer " + api_key}) as admin:
        created = admin.post("/v1/api-keys", json={"name": "django", "scopes": ["publish", "consume"]})
        created.raise_for_status()
        return str(created.json()["secret"])


@pytest.fixture
def prefix(settings: Any, api_key: str, app_key: str) -> str:
    """Queues of this test only, with two attempts and a fixed backoff."""
    prefix = "t" + secrets.token_hex(3) + "-"
    settings.TASKS = {
        "default": {
            "BACKEND": "lessormq_django.LessorMQBackend",
            "QUEUES": ["default", "mail"],
            "OPTIONS": {"URL": URL, "API_KEY": app_key, "QUEUE_PREFIX": prefix},
        }
    }
    settings_patch = {"max_attempts": 2, "backoff": {"initial_seconds": BACKOFF, "jitter": 0}}
    with httpx.Client(base_url=URL, headers={"Authorization": "Bearer " + api_key}) as admin:
        admin.patch(f"/v1/queues/{prefix}default", json=settings_patch).raise_for_status()
    return prefix


def work(max_tasks: int = 1, wait_seconds: int = 5) -> int:
    return run_worker(["default"], wait_seconds=wait_seconds, max_tasks=max_tasks, handle_signals=False)


def test_a_task_is_run_by_the_worker_and_acked(prefix: str) -> None:
    result = tasks.add.enqueue(2, 3)

    assert work() == 1
    result.refresh()

    assert tasks.seen == [("add", 2, 3)]
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.attempts == 1
    assert result.finished_at is not None
    with pytest.raises(NotImplementedError):
        result.return_value


def test_a_failing_task_comes_back_after_the_backoff_with_the_servers_attempt(prefix: str) -> None:
    started: list[float] = []

    def on_started(sender: Any, **kwargs: Any) -> None:
        started.append(time.monotonic())

    result = tasks.flaky.enqueue("e2e")
    task_started.connect(on_started)
    try:
        work()
        failed_at = time.monotonic()
        result.refresh()
        assert result.status == TaskResultStatus.READY
        assert result.attempts == 1
        assert result.errors[0].exception_class is ConnectionError

        work()
    finally:
        task_started.disconnect(on_started)
    result.refresh()

    assert tasks.seen == [("flaky", "e2e", 1), ("flaky", "e2e", 2)]
    assert started[1] - failed_at >= BACKOFF - 0.5
    # Two attempts and the queue gave up: the dead-letter set.
    assert result.status == TaskResultStatus.FAILED
    assert result.attempts == 2
    assert result.errors[0].traceback.endswith("ConnectionError: upstream down (e2e)\n")


def test_a_permanent_failure_goes_to_the_dead_letter_set_at_once(prefix: str) -> None:
    result = tasks.unusable.enqueue("p")

    work()
    result.refresh()

    assert tasks.seen == [("unusable", "p")]
    assert result.status == TaskResultStatus.FAILED
    assert result.attempts == 1
    assert result.errors[0].exception_class is PermanentFailure


def test_run_after_holds_the_task_back(prefix: str) -> None:
    started = time.monotonic()
    tasks.add.using(run_after=timezone.now() + timedelta(seconds=2)).enqueue(1, 1)
    tasks.add.using(run_after=timezone.now() - timedelta(minutes=5)).enqueue(2, 2)

    work(max_tasks=2)

    assert tasks.seen == [("add", 2, 2), ("add", 1, 1)]
    # The Python client sends available_at in whole seconds, so two seconds ahead can
    # be a little over one.
    assert time.monotonic() - started >= 1.0
