"""Against a running server: a tenant of its own, real Celery workers (prefork) in
subprocesses, tasks published from this process.

    LESSORMQ_URL=http://127.0.0.1:8080 LESSORMQ_OPERATOR_KEY=lmq_... pytest tests/test_end_to_end.py

Skipped without those two variables.
"""

from __future__ import annotations

import json
import os
import secrets
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

URL = os.environ.get("LESSORMQ_URL", "")
OPERATOR_KEY = os.environ.get("LESSORMQ_OPERATOR_KEY", "")
BACKOFF = 2
HERE = Path(__file__).resolve().parent

pytestmark = pytest.mark.skipif(not (URL and OPERATOR_KEY), reason="needs LESSORMQ_URL and LESSORMQ_OPERATOR_KEY")


@pytest.fixture(scope="module")
def env(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, str]]:
    """A tenant of these tests (removed afterwards), and the environment of the app."""
    tenant = "celery-" + secrets.token_hex(4)
    directory = tmp_path_factory.mktemp("e2e")
    (directory / "results").mkdir()
    with httpx.Client(base_url=URL, headers={"Authorization": "Bearer " + OPERATOR_KEY}) as operator:
        created = operator.post("/v1/tenants", json={"id": tenant, "name": "lessormq-celery tests"})
        created.raise_for_status()
        values = {"LESSORMQ_URL": URL, "LESSORMQ_KEY": created.json()["admin_key"]["secret"], "E2E_DIR": str(directory)}
        os.environ.update(values)
        yield values
        operator.patch(f"/v1/tenants/{tenant}", json={"status": "suspended"}).raise_for_status()
        operator.delete(f"/v1/tenants/{tenant}").raise_for_status()


@pytest.fixture(scope="module")
def tasks(env: dict[str, str]) -> Any:
    from tests import e2e_app

    return e2e_app


@pytest.fixture
def queue(env: dict[str, str]) -> str:
    """A queue of this test only: two attempts, a fixed backoff."""
    name = "q-" + secrets.token_hex(4)
    settings = {"max_attempts": 2, "backoff": {"initial_seconds": BACKOFF, "jitter": 0}}
    admin(env, "PATCH", f"/v1/queues/{name}", json=settings)
    return name


@pytest.fixture
def worker(env: dict[str, str], queue: str, tmp_path: Path) -> Iterator[Callable[..., subprocess.Popen[bytes]]]:
    """Starts a Celery worker on the test's queue; stopped (warm) at the end."""
    started: list[subprocess.Popen[bytes]] = []

    def start(concurrency: int = 2, prefetch: int = 1, pool: str = "prefork") -> subprocess.Popen[bytes]:
        log = tmp_path / f"worker-{len(started)}.log"
        process = subprocess.Popen(
            [sys.executable, "-m", "celery", "-A", "tests.e2e_app", "worker", "-P", pool,
             "-c", str(concurrency), "-Q", queue, "-n", f"{queue}-{len(started)}@%h", "-l", "info"],
            cwd=HERE.parent,
            env={**os.environ, **env, "E2E_QUEUE": queue, "E2E_PREFETCH": str(prefetch)},
            stdout=log.open("wb"),
            stderr=subprocess.STDOUT,
        )
        started.append(process)
        until(lambda: " ready." in log.read_text() or process.poll() is not None, 30)
        assert process.poll() is None, log.read_text()
        return process

    yield start
    for process in started:
        stop(process)


def stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def admin(env: dict[str, str], method: str, path: str, **kwargs: Any) -> Any:
    response = httpx.request(method, URL + path, headers={"Authorization": "Bearer " + env["LESSORMQ_KEY"]}, **kwargs)
    response.raise_for_status()
    return response.json() if response.content else None


def message(env: dict[str, str], queue: str, task_id: str) -> dict[str, Any]:
    """The LessorMQ message of a task."""
    items = admin(env, "GET", f"/v1/queues/{queue}/messages", params={"search": task_id})["items"]
    found = [item for item in items if item["headers"].get("task_id") == task_id]
    assert len(found) == 1, items
    return found[0]  # type: ignore[no-any-return]


def notes(env: dict[str, str], name: str, marker: str) -> list[dict[str, Any]]:
    path = Path(env["E2E_DIR"]) / f"{name}.jsonl"
    lines = path.read_text().splitlines() if path.exists() else []
    return [entry for entry in map(json.loads, lines) if entry.get("marker") == marker]


def until(condition: Callable[[], Any], timeout: float = 30) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        value = condition()
        if value:
            return value
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.1)


def test_a_task_is_run_by_the_worker_and_acked(env: dict[str, str], tasks: Any, queue: str, worker: Any) -> None:
    worker()
    result = tasks.add.apply_async((2, 3), queue=queue)

    assert result.get(timeout=30) == 5
    done = until(lambda: (m := message(env, queue, result.id))["status"] == "completed" and m)
    assert done["attempts"] == 1
    assert done["body"]["payload"][0] == [2, 3]


@pytest.mark.parametrize("pool", ["prefork", "threads", "solo"])
def test_a_failing_task_comes_back_after_the_backoff_with_the_servers_attempt_count(
    env: dict[str, str], tasks: Any, queue: str, worker: Any, pool: str
) -> None:
    worker(pool=pool)
    result = tasks.flaky.apply_async((pool,), queue=queue)

    until(lambda: notes(env, "flaky", pool))
    # The queue will try again: Celery's state is RETRY, not FAILURE.
    assert until(lambda: result.state not in ("PENDING", "STARTED") and result.state) == "RETRY"
    runs = until(lambda: len(n := notes(env, "flaky", pool)) == 2 and n)
    failed = until(lambda: (m := message(env, queue, result.id))["status"] == "failed" and m)

    assert [run["attempt"] for run in runs] == [1, 2]
    assert [run["retries"] for run in runs] == [0, 1]
    assert runs[1]["at"] - runs[0]["at"] >= BACKOFF - 0.5
    assert failed["attempts"] == 2
    assert failed["last_error"]["type"] == "ConnectionError"
    assert failed["last_error"]["permanent"] is False
    assert "Traceback" in failed["last_error"]["details"]["traceback"]
    assert until(lambda: result.state == "FAILURE")


def test_retry_leaves_the_delay_and_the_count_to_the_queue(
    env: dict[str, str], tasks: Any, queue: str, worker: Any
) -> None:
    worker()
    result = tasks.retrying.apply_async(("retrying",), queue=queue)

    runs = until(lambda: len(n := notes(env, "retrying", "retrying")) == 2 and n)
    failed = until(lambda: (m := message(env, queue, result.id))["status"] == "failed" and m)

    # countdown=3600 and max_retries=0 in the task: the queue's backoff and attempts.
    assert BACKOFF - 0.5 <= runs[1]["at"] - runs[0]["at"] < 30
    assert failed["attempts"] == 2
    assert failed["last_error"]["type"] == "TimeoutError"


def test_a_permanent_failure_goes_to_the_dead_letter_set_at_once(
    env: dict[str, str], tasks: Any, queue: str, worker: Any
) -> None:
    worker()
    result = tasks.unusable.apply_async(("unusable",), queue=queue)

    failed = until(lambda: (m := message(env, queue, result.id))["status"] == "failed" and m)

    assert failed["attempts"] == 1
    assert failed["last_error"]["permanent"] is True
    assert failed["last_error"]["type"] == "lessormq._errors.PermanentFailure"
    assert until(lambda: result.state == "FAILURE")
    assert len(notes(env, "unusable", "unusable")) == 1


def test_a_child_process_that_dies_is_a_failed_attempt(
    env: dict[str, str], tasks: Any, queue: str, worker: Any
) -> None:
    worker()
    result = tasks.crashing.apply_async(("crashing",), queue=queue)

    failed = until(lambda: (m := message(env, queue, result.id))["status"] == "failed" and m, 60)

    assert failed["attempts"] == 2
    assert failed["last_error"]["type"] == "billiard.exceptions.WorkerLostError"
    assert [run["retries"] for run in notes(env, "crashing", "crashing")] == [0, 1]


def test_a_hard_time_limit_is_a_failed_attempt(env: dict[str, str], tasks: Any, queue: str, worker: Any) -> None:
    worker()
    result = tasks.overtime.apply_async(("overtime",), queue=queue)

    failed = until(lambda: (m := message(env, queue, result.id))["status"] == "failed" and m, 60)

    assert failed["attempts"] == 2
    assert failed["last_error"]["type"] == "billiard.exceptions.TimeLimitExceeded"
    assert [run["retries"] for run in notes(env, "overtime", "overtime")] == [0, 1]


def test_countdown_is_held_back_by_the_server(env: dict[str, str], tasks: Any, queue: str, worker: Any) -> None:
    worker()
    published = time.time()
    result = tasks.add.apply_async((1, 1), countdown=3, queue=queue)

    waiting = message(env, queue, result.id)
    assert waiting["status"] == "pending"
    assert result.get(timeout=30) == 2
    ran = [n for n in (Path(env["E2E_DIR"]) / "add.jsonl").read_text().splitlines() if '"a": 1, "b": 1' in n]
    assert json.loads(ran[-1])["at"] - published >= 2.5


def test_a_warm_shutdown_finishes_the_running_task_and_gives_back_the_rest(
    env: dict[str, str], tasks: Any, queue: str, worker: Any
) -> None:
    process = worker(concurrency=1, prefetch=2)
    first = tasks.limited.apply_async(("first", 3), queue=queue)
    until(lambda: notes(env, "limited", "first"))
    # Held by the worker, waiting for the rate limit: claimed, never started.
    second = tasks.limited.apply_async(("second", 0), queue=queue)
    until(lambda: message(env, queue, second.id)["status"] == "processing")

    stop(process)

    assert message(env, queue, first.id)["status"] == "completed"
    assert [n["phase"] for n in notes(env, "limited", "first")] == ["start", "end"]
    released = message(env, queue, second.id)
    assert (released["status"], released["attempts"]) == ("pending", 0)
    assert notes(env, "limited", "second") == []


def test_a_chord_runs_on_celerys_own_polling(env: dict[str, str], tasks: Any, queue: str, worker: Any) -> None:
    from celery import chord

    worker()
    header = [tasks.add.s(1, 2).set(queue=queue), tasks.sleepy.s(7, 3).set(queue=queue)]
    result = chord(header)(tasks.total.s().set(queue=queue))

    assert result.get(timeout=60) == 10
    # The file result backend has no native chords: celery.chord_unlock polls with
    # Celery's own retry, which publishes its next run, while the header still runs.
    items = admin(env, "GET", f"/v1/queues/{queue}/messages", params={"limit": 100})["items"]
    unlocks = [item for item in items if item["headers"].get("task") == "celery.chord_unlock"]
    assert len(unlocks) >= 2
    assert all(item["status"] == "completed" for item in unlocks)
