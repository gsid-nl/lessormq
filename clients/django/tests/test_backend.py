from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from asgiref.sync import async_to_sync
from django.core.exceptions import ImproperlyConfigured
from django.tasks import TaskResultStatus, default_task_backend, task_backends
from django.tasks.exceptions import InvalidTask, TaskResultDoesNotExist
from django.tasks.signals import task_enqueued

from lessormq_django import LeaseExpired, LessorMQBackend
from tests import tasks
from tests.conftest import backend_settings
from tests.fake import Fake

PUBLISHED = {
    "id": "m1",
    "queue": "shop-default",
    "status": "pending",
    "created_at": "2030-01-01T00:00:00Z",
    "available_at": "2030-01-01T00:00:00Z",
    "duplicate": False,
}


def message(**fields: Any) -> dict[str, Any]:
    """A message as GET /v1/messages/{id} answers it."""
    return {
        "id": "m1",
        "queue": "shop-default",
        "status": "pending",
        "body": {"task": "tests.tasks.add", "args": [1, 2], "kwargs": {}},
        "headers": {},
        "dedup_key": None,
        "attempts": 0,
        "max_attempts": 7,
        "available_at": "2030-01-01T00:00:00Z",
        "started_at": None,
        "lease_until": None,
        "consumer": None,
        "last_error": None,
        "failed_routes": {},
        "created_at": "2030-01-01T00:00:00Z",
        "updated_at": "2030-01-01T00:00:00Z",
        "completed_at": None,
        "failed_at": None,
        **fields,
    }


def test_enqueue_publishes_the_task_as_json_to_the_prefixed_queue(fake: Fake) -> None:
    fake.on("/v1/queues/shop-default/messages", (201, PUBLISHED))
    enqueued = []
    task_enqueued.connect(lambda sender, task_result, **kw: enqueued.append((sender, task_result)), weak=False)
    try:
        result = tasks.add.enqueue(1, b=2)
    finally:
        task_enqueued.receivers.clear()

    sent = fake.calls("/v1/queues/shop-default/messages")
    assert len(sent) == 1
    assert sent[0]["body"] == {"task": "tests.tasks.add", "args": [1], "kwargs": {"b": 2}}
    assert sent[0]["dedup_key"].startswith("lmq-auto-")
    assert "available_at" not in sent[0]
    assert fake.requests[0].headers["Authorization"] == "Bearer k"
    assert fake.requests[0].headers["User-Agent"] == "lessormq-django/1"

    assert result.id == "m1"
    assert result.status == TaskResultStatus.READY
    assert result.enqueued_at == datetime(2030, 1, 1, tzinfo=timezone.utc)
    assert result.args == [1]
    assert result.kwargs == {"b": 2}
    assert result.attempts == 0
    assert enqueued == [(LessorMQBackend, result)]


def test_a_task_goes_to_the_queue_of_its_queue_name(fake: Fake) -> None:
    fake.on("/v1/queues/shop-mail/messages", (201, {**PUBLISHED, "queue": "shop-mail"}))

    tasks.send.enqueue("ada@example.com")
    tasks.add.using(queue_name="mail").enqueue(1, 2)

    assert len(fake.calls("/v1/queues/shop-mail/messages")) == 2


def test_run_after_becomes_available_at(fake: Fake) -> None:
    fake.on("/v1/queues/shop-default/messages", (201, PUBLISHED))
    run_after = datetime(2030, 1, 1, 12, 30, tzinfo=timezone(timedelta(hours=2)))

    tasks.add.using(run_after=run_after).enqueue(1, 2)

    assert fake.calls("/v1/queues/shop-default/messages")[0]["available_at"] == "2030-01-01T10:30:00Z"


def test_a_naive_run_after_is_local_time_without_use_tz(fake: Fake, settings: Any) -> None:
    settings.USE_TZ = False
    settings.TIME_ZONE = "Europe/Amsterdam"
    settings.TASKS = backend_settings(fake)
    fake.on("/v1/queues/shop-default/messages", (201, PUBLISHED))

    result = tasks.add.using(run_after=datetime(2030, 7, 1, 12, 0)).enqueue(1, 2)

    assert fake.calls("/v1/queues/shop-default/messages")[0]["available_at"] == "2030-07-01T10:00:00Z"
    assert result.enqueued_at == datetime(2030, 1, 1, 1, 0)


def test_priority_is_not_supported(fake: Fake) -> None:
    assert default_task_backend.supports_priority is False
    with pytest.raises(InvalidTask, match="priority"):
        tasks.add.using(priority=10)


def test_async_tasks_and_defer_are_supported(fake: Fake) -> None:
    assert default_task_backend.supports_async_task is True
    assert default_task_backend.supports_defer is True


def test_arguments_that_are_not_json_are_refused_before_publishing(fake: Fake) -> None:
    with pytest.raises(TypeError):
        tasks.add.enqueue(object(), 2)
    assert fake.requests == []


def test_aenqueue_publishes_too(fake: Fake) -> None:
    fake.on("/v1/queues/shop-default/messages", (201, PUBLISHED))

    result = async_to_sync(tasks.add.aenqueue)(1, 2)

    assert result.id == "m1"
    assert len(fake.calls("/v1/queues/shop-default/messages")) == 1


def test_a_publish_error_is_raised(fake: Fake) -> None:
    from lessormq import ForbiddenError

    fake.on("/v1/queues/shop-default/messages", (403, {"code": "forbidden", "detail": "no publish scope"}))
    with pytest.raises(ForbiddenError):
        tasks.add.enqueue(1, 2)


def test_results_are_read_with_the_publish_key(fake: Fake) -> None:
    fake.on("/v1/messages/m1", (200, message(status="pending")))

    tasks.add.get_result("m1")

    assert default_task_backend.supports_get_result is True
    assert fake.requests[0].headers["Authorization"] == "Bearer k"


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("pending", TaskResultStatus.READY),
        ("processing", TaskResultStatus.RUNNING),
        ("completed", TaskResultStatus.SUCCESSFUL),
        ("failed", TaskResultStatus.FAILED),
    ],
)
def test_get_result_maps_the_message_status(fake: Fake, status: str, expected: TaskResultStatus) -> None:
    fake.on("/v1/messages/m1", (200, message(status=status)))

    result = tasks.add.get_result("m1")

    assert result.status == expected
    assert fake.requests[0].headers["Authorization"] == "Bearer k"


def test_get_result_reads_attempts_times_and_the_last_error(fake: Fake) -> None:
    fake.on(
        "/v1/messages/m1",
        (
            200,
            message(
                status="failed",
                attempts=3,
                consumer="web-1:42",
                started_at="2030-01-01T00:05:00Z",
                failed_at="2030-01-01T00:05:02Z",
                last_error={
                    "type": "ConnectionError",
                    "message": "upstream down",
                    "details": {"file": "/app/tests/tasks.py", "line": 37},
                    "at": "2030-01-01T00:05:02Z",
                    "attempt": 3,
                    "permanent": False,
                },
            ),
        ),
    )

    result = tasks.add.get_result("m1")

    assert result.status == TaskResultStatus.FAILED
    assert result.attempts == 3
    assert result.worker_ids == ["", "", "web-1:42"]
    assert result.args == [1, 2]
    assert result.task.func is tasks.add.func
    assert result.task.queue_name == "default"
    assert result.enqueued_at == datetime(2030, 1, 1, tzinfo=timezone.utc)
    assert result.started_at == datetime(2030, 1, 1, 0, 5, tzinfo=timezone.utc)
    assert result.last_attempted_at == result.started_at
    assert result.finished_at == datetime(2030, 1, 1, 0, 5, 2, tzinfo=timezone.utc)
    assert len(result.errors) == 1
    assert result.errors[0].exception_class is ConnectionError
    assert result.errors[0].traceback == (
        "Traceback (most recent call last):\n"
        '  File "/app/tests/tasks.py", line 37\n'
        "ConnectionError: upstream down\n"
    )
    with pytest.raises(ValueError, match="failed"):
        result.return_value


def test_an_expired_lease_is_a_lease_expired_error(fake: Fake) -> None:
    fake.on(
        "/v1/messages/m1",
        (200, message(attempts=1, last_error={"message": "Lease expired", "expired": True, "attempt": 1})),
    )

    result = tasks.add.get_result("m1")

    assert result.errors[0].exception_class is LeaseExpired


def test_a_successful_result_has_no_return_value(fake: Fake) -> None:
    fake.on("/v1/messages/m1", (200, message(status="completed", attempts=1, completed_at="2030-01-01T00:00:01Z")))

    result = tasks.add.get_result("m1")

    assert result.status == TaskResultStatus.SUCCESSFUL
    with pytest.raises(NotImplementedError, match="return values"):
        result.return_value


def test_refresh_reads_the_message_again(fake: Fake) -> None:
    fake.on("/v1/queues/shop-default/messages", (201, PUBLISHED))
    fake.on("/v1/messages/m1", (200, message(status="processing", attempts=1, consumer="w")))

    result = tasks.add.enqueue(1, 2)
    result.refresh()

    assert result.status == TaskResultStatus.RUNNING
    assert result.attempts == 1


def test_a_message_that_is_gone_does_not_exist(fake: Fake) -> None:
    fake.on("/v1/messages/m1", (404, {"code": "not_found"}))

    with pytest.raises(TaskResultDoesNotExist):
        tasks.add.get_result("m1")


def test_a_message_that_is_not_a_task_does_not_exist(fake: Fake) -> None:
    fake.on("/v1/messages/m1", (200, message(body={"to": "ada@example.com"})))

    with pytest.raises(TaskResultDoesNotExist):
        default_task_backend.get_result("m1")


def test_get_result_retries_a_server_error(fake: Fake) -> None:
    fake.on("/v1/messages/m%2F1", (503, {"code": "unavailable"}), (200, message(id="m/1")))

    result = tasks.add.get_result("m/1")

    assert result.id == "m/1"
    assert len(fake.requests) == 2


def test_the_checks_name_missing_options_and_bad_queue_names(settings: Any) -> None:
    settings.TASKS = {
        "default": {
            "BACKEND": "lessormq_django.LessorMQBackend",
            "QUEUES": ["default", "no spaces"],
            "OPTIONS": {},
        }
    }
    backend = task_backends["default"]

    assert [error.id for error in backend.check()] == ["lessormq_django.E001", "lessormq_django.E002"]
    with pytest.raises(ImproperlyConfigured):
        backend.client


def test_the_backend_never_shows_its_keys(fake: Fake) -> None:
    assert repr(task_backends["default"]) == "LessorMQBackend('default', 'https://mq.test')"
