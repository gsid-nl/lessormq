from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from billiard.einfo import ExceptionInfo
from celery import Celery
from celery.exceptions import Ignore, ImproperlyConfigured, Reject, Retry, WorkerLostError
from celery.utils.serialization import UnpickleableExceptionWrapper

import lessormq_celery
from lessormq_celery import PermanentFailure, ServerRetry
from lessormq_celery.checks import check_worker, problems
from lessormq_celery.request import outcome_of

app = Celery("tests", task_cls=lessormq_celery.Task, broker="lessormq://:k@mq.test", set_as_current=False)


class BadInput(ValueError, PermanentFailure):
    pass


@app.task(bind=True)
def fails(self: Any, error: BaseException) -> None:
    raise error


@app.task(bind=True)
def retries(self: Any, **options: Any) -> None:
    try:
        raise TimeoutError("slow")
    except TimeoutError as e:
        raise self.retry(exc=e, **options)


def delivered(task: Any, attempt: int, max_attempts: int = 3, *args: Any, **kwargs: Any) -> Any:
    """Run ``task`` as the worker runs a delivery of attempt ``attempt``."""
    task.push_request(
        id="t1", called_directly=False, lessormq_attempt=attempt, lessormq_max_attempts=max_attempts, retries=attempt - 1
    )
    try:
        return task(*args, **kwargs)
    finally:
        task.pop_request()


def info(error: BaseException) -> ExceptionInfo:
    try:
        raise error
    except BaseException:
        return ExceptionInfo()


def test_a_failed_attempt_the_queue_will_try_again_is_a_retry() -> None:
    with pytest.raises(ServerRetry) as raised:
        delivered(fails, 1, 3, ConnectionError("down"))

    assert isinstance(raised.value.exc, ConnectionError)
    assert raised.value.humanize() == "after the queue's backoff"


def test_the_last_attempt_and_a_permanent_failure_stay_failures() -> None:
    with pytest.raises(ConnectionError):
        delivered(fails, 3, 3, ConnectionError("down"))
    with pytest.raises(BadInput):
        delivered(fails, 1, 3, BadInput("no"))
    with pytest.raises(Reject):
        delivered(fails, 1, 3, Reject("no", requeue=False))


def test_outside_a_lessormq_delivery_the_task_is_plain_celery() -> None:
    with pytest.raises(ConnectionError):
        fails(ConnectionError("down"))


def test_retry_publishes_nothing_and_ignores_countdown_and_max_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retries, "apply_async", lambda *a, **k: pytest.fail("retry published a message"))

    with pytest.raises(ServerRetry) as raised:
        delivered(retries, 1, 3, countdown=3600, max_retries=0)

    assert isinstance(raised.value.exc, TimeoutError)


def test_retry_on_the_last_attempt_raises_the_error() -> None:
    with pytest.raises(TimeoutError):
        delivered(retries, 3, 3)


def test_outcomes_of_what_a_task_raised() -> None:
    assert outcome_of(info(ServerRetry(exc=ConnectionError("down")))).error.__class__ is ConnectionError
    assert outcome_of(info(Retry("polling", when=1))).action == "ack"
    assert outcome_of(info(Reject("later", requeue=True))).action == "release"
    assert outcome_of(info(Ignore())).action == "ack"

    rejected = outcome_of(info(Reject("bad input", requeue=False)))
    assert (rejected.action, rejected.permanent, rejected.error, rejected.type) == (
        "fail",
        True,
        "bad input",
        "celery.exceptions.Reject",
    )

    permanent = outcome_of(info(BadInput("no")))
    assert (permanent.action, permanent.permanent) == ("fail", True)
    assert "BadInput" in permanent.details["traceback"]

    lost = outcome_of(info(WorkerLostError("gone")))
    assert (lost.action, lost.permanent) == ("fail", False)

    wrapped = outcome_of(info(UnpickleableExceptionWrapper("shop.errors", "Odd", ("x",), "Odd('x')")))
    assert (wrapped.type, wrapped.error) == ("shop.errors.Odd", "Odd('x')")


def test_an_app_with_the_base_task_is_ready() -> None:
    assert problems(app) == []


def test_tasks_that_decide_themselves_are_refused() -> None:
    other = Celery("other", broker="lessormq://:k@mq.test", set_as_current=False)

    @other.task
    def plain() -> None:
        pass

    mixed = Celery("mixed", task_cls=lessormq_celery.Task, set_as_current=False)
    mixed.conf.result_backend = "rpc://"

    @mixed.task(acks_late=False)
    def early() -> None:
        pass

    @mixed.task(retry_on_worker_lost=True)
    def republishing() -> None:
        pass

    assert any("plain does not derive from lessormq_celery.Task" in p for p in problems(other))
    found = problems(mixed)
    assert any("early has acks_late off" in p for p in found)
    assert any("republishing has retry_on_worker_lost on" in p for p in found)
    assert any("rpc://" in p for p in found)

    with pytest.raises(ImproperlyConfigured, match="Not ready for LessorMQ"):
        check_worker(sender=SimpleNamespace(app=other))


def test_the_check_leaves_other_brokers_alone() -> None:
    other = Celery("other", broker="memory://", set_as_current=False)

    @other.task
    def plain() -> None:
        pass

    check_worker(sender=SimpleNamespace(app=other))
