from __future__ import annotations

import os
import signal
from io import StringIO
from typing import Any

import pytest
from django.core.management import CommandError, call_command
from django.tasks import TaskResultStatus
from django.tasks.signals import task_finished, task_started

from lessormq_django.worker import run_worker
from tests import tasks
from tests.fake import FAIL_RESULT, Fake

CLAIMS = "/v1/queues/shop-default/claims"


def lease(body: Any, *, attempt: int = 1, queue: str = "shop-default", id: str = "m1") -> dict[str, Any]:
    return {
        "receipt": "r1",
        "lease_until": "2030-01-01T00:05:00Z",
        "message": {
            "id": id,
            "queue": queue,
            "body": body,
            "headers": {},
            "attempt": attempt,
            "max_attempts": 7,
            "created_at": "2030-01-01T00:00:00Z",
        },
    }


def task_body(name: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
    return {"task": f"tests.tasks.{name}", "args": list(args), "kwargs": kwargs}


def run_one(fake: Fake, body: Any, **lease_fields: Any) -> int:
    fake.on(CLAIMS, (200, lease(body, **lease_fields)))
    fake.on("/v1/messages/m1/ack", (204, None)).on("/v1/messages/m1/fail", (200, FAIL_RESULT))
    return run_worker(["default"], consumer="w1", wait_seconds=1, max_tasks=1, handle_signals=False)


@pytest.fixture
def signals() -> Any:
    sent: list[tuple[str, Any]] = []

    def started(sender: Any, task_result: Any, **kwargs: Any) -> None:
        sent.append(("started", task_result.status))

    def finished(sender: Any, task_result: Any, **kwargs: Any) -> None:
        sent.append(("finished", task_result.status, [e.exception_class_path for e in task_result.errors]))

    task_started.connect(started)
    task_finished.connect(finished)
    yield sent
    task_started.disconnect(started)
    task_finished.disconnect(finished)


def test_a_task_that_returns_is_acked(fake: Fake, signals: Any) -> None:
    assert run_one(fake, task_body("add", 1, b=2)) == 1

    assert tasks.seen == [("add", 1, 2)]
    assert fake.calls("/v1/messages/m1/ack") == [{"receipt": "r1"}]
    assert fake.calls(CLAIMS)[0] == {"consumer": "w1", "wait_seconds": 1}
    assert signals == [("started", TaskResultStatus.RUNNING), ("finished", TaskResultStatus.SUCCESSFUL, [])]


def test_the_context_has_the_servers_attempt_and_the_message_id(fake: Fake) -> None:
    run_one(fake, task_body("attempt"), attempt=3)

    assert tasks.seen == [("attempt", 3, "m1")]


def test_a_task_that_raises_is_failed_and_the_queue_decides(fake: Fake, signals: Any) -> None:
    run_one(fake, task_body("flaky", "x"))

    assert fake.calls("/v1/messages/m1/ack") == []
    sent = fake.calls("/v1/messages/m1/fail")
    assert len(sent) == 1
    assert sent[0]["permanent"] is False
    assert sent[0]["error"]["type"] == "ConnectionError"
    assert sent[0]["error"]["message"] == "upstream down (x)"
    assert signals[-1] == ("finished", TaskResultStatus.FAILED, ["builtins.ConnectionError"])


def test_a_permanent_failure_is_reported_as_permanent(fake: Fake) -> None:
    run_one(fake, task_body("unusable", "x"))

    sent = fake.calls("/v1/messages/m1/fail")
    assert sent[0]["permanent"] is True
    assert sent[0]["error"]["type"] == "lessormq._errors.PermanentFailure"


def test_a_message_that_is_not_a_task_fails_permanently(fake: Fake) -> None:
    run_one(fake, {"to": "ada@example.com"})

    sent = fake.calls("/v1/messages/m1/fail")
    assert sent[0]["permanent"] is True
    assert sent[0]["error"]["type"] == "lessormq_django.exceptions.MalformedTask"


@pytest.mark.parametrize("path", ["tests.tasks.gone", "tests.nowhere.task", "tests.tasks.plain"])
def test_a_task_this_worker_does_not_know_is_an_ordinary_failure(fake: Fake, path: str) -> None:
    run_one(fake, {"task": path, "args": [], "kwargs": {}})

    sent = fake.calls("/v1/messages/m1/fail")
    assert sent[0]["permanent"] is False
    assert sent[0]["error"]["type"] == "lessormq_django.exceptions.UnknownTask"


def test_an_async_task_runs(fake: Fake) -> None:
    run_one(fake, task_body("asynchronous", "v"))

    assert tasks.seen == [("asynchronous", "v")]
    assert len(fake.calls("/v1/messages/m1/ack")) == 1


def test_a_return_value_that_is_not_json_fails_as_in_django(fake: Fake) -> None:
    run_one(fake, task_body("not_json"))

    assert fake.calls("/v1/messages/m1/fail")[0]["error"]["type"] == "TypeError"


def test_the_command_runs_every_queue_of_the_backend(fake: Fake) -> None:
    fake.on(CLAIMS, (200, lease(task_body("add", 1, 2))))
    fake.on("/v1/queues/shop-mail/claims", (200, lease(task_body("send", "ada@example.com"), id="m2")))
    fake.on("/v1/messages/m1/ack", (204, None)).on("/v1/messages/m2/ack", (204, None))
    out = StringIO()

    call_command("lessormq_worker", "--max-tasks", "1", "--wait-seconds", "1", stdout=out)

    assert sorted(tasks.seen) == [("add", 1, 2), ("send", "ada@example.com", "welcome")]
    assert "default (shop-default), mail (shop-mail)" in out.getvalue()
    assert "Stopped after 2 task(s)" in out.getvalue()


def test_the_command_runs_the_queues_it_is_given(fake: Fake) -> None:
    fake.on("/v1/queues/shop-mail/claims", (200, lease(task_body("send", "ada@example.com"), id="m2")))
    fake.on("/v1/messages/m2/ack", (204, None))

    call_command("lessormq_worker", "--queue", "mail", "--max-tasks", "1", "--route", "vpn-2", stdout=StringIO())

    assert tasks.seen == [("send", "ada@example.com", "welcome")]
    assert fake.calls(CLAIMS) == []
    assert fake.calls("/v1/queues/shop-mail/claims")[0]["route"] == "vpn-2"


def test_the_command_refuses_a_queue_the_backend_does_not_have(fake: Fake) -> None:
    with pytest.raises(CommandError, match="nope"):
        call_command("lessormq_worker", "--queue", "nope")


def test_the_command_refuses_another_backend(settings: Any) -> None:
    settings.TASKS = {"default": {"BACKEND": "django.tasks.backends.immediate.ImmediateBackend"}}

    with pytest.raises(CommandError, match="not a LessorMQBackend"):
        call_command("lessormq_worker")


def test_sigterm_stops_the_worker(fake: Fake) -> None:
    fake.on(CLAIMS, (204, None))

    handled = run_worker(
        ["default"], wait_seconds=1, on_ready=lambda workers: os.kill(os.getpid(), signal.SIGTERM)
    )

    assert handled == 0
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL
