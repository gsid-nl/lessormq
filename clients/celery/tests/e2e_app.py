"""The Celery app of the end-to-end tests, configured from the environment the tests
give the worker: LESSORMQ_URL, LESSORMQ_KEY, E2E_QUEUE and E2E_DIR (a directory for the
result backend and the tasks' notes)."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from celery import Celery
from celery.exceptions import Reject

import lessormq_celery

DIR = Path(os.environ.get("E2E_DIR", "/nonexistent"))

app = Celery("e2e", task_cls=lessormq_celery.Task)
app.conf.update(
    broker_url=os.environ.get("LESSORMQ_URL", "http://127.0.0.1:8080").replace("http://", "lessormq+http://", 1),
    broker_transport_options={"api_key": os.environ.get("LESSORMQ_KEY", ""), "wait_seconds": 1},
    result_backend=f"file://{DIR / 'results'}",
    task_default_queue=os.environ.get("E2E_QUEUE", "celery"),
    worker_prefetch_multiplier=int(os.environ.get("E2E_PREFETCH", "1")),
    # How long the worker waits for a child that died to report: short, for the tests.
    worker_lost_wait=1,
    worker_hijack_root_logger=False,
)


def note(name: str, **fields: Any) -> None:
    """What a task saw, one JSON line per run."""
    with open(DIR / f"{name}.jsonl", "a") as f:
        f.write(json.dumps({"at": time.time(), **fields}) + "\n")


@app.task(bind=True)
def add(self: Any, a: int, b: int) -> int:
    note("add", a=a, b=b, retries=self.request.retries)
    return a + b


@app.task(bind=True)
def flaky(self: Any, marker: str) -> None:
    note("flaky", marker=marker, retries=self.request.retries, attempt=self.request.lessormq_attempt)
    raise ConnectionError(f"upstream down ({marker})")


@app.task(bind=True)
def retrying(self: Any, marker: str) -> None:
    note("retrying", marker=marker, retries=self.request.retries)
    try:
        raise TimeoutError("slow upstream")
    except TimeoutError as e:
        # Celery would wait an hour and give up after this run: the queue decides.
        raise self.retry(exc=e, countdown=3600, max_retries=0)


@app.task(bind=True, autoretry_for=(KeyError,), retry_backoff=True)
def autoretrying(self: Any, marker: str) -> None:
    note("autoretrying", marker=marker, retries=self.request.retries)
    raise KeyError(marker)


@app.task(bind=True)
def unusable(self: Any, marker: str) -> None:
    note("unusable", marker=marker)
    raise lessormq_celery.PermanentFailure(f"no such invoice ({marker})")


@app.task(bind=True)
def rejecting(self: Any, marker: str) -> None:
    note("rejecting", marker=marker)
    raise Reject("bad input", requeue=False)


@app.task(bind=True)
def crashing(self: Any, marker: str) -> None:
    note("crashing", marker=marker, retries=self.request.retries)
    os._exit(1)


@app.task(bind=True, rate_limit="1/m")
def limited(self: Any, marker: str, seconds: float) -> str:
    """Runs once a minute: a second message waits in the worker, held but not started."""
    note("limited", marker=marker, phase="start")
    time.sleep(seconds)
    note("limited", marker=marker, phase="end")
    return marker


@app.task
def total(numbers: list[int]) -> int:
    return sum(numbers)


@app.task
def sleepy(value: int, seconds: float) -> int:
    time.sleep(seconds)
    return value


@app.task(bind=True, time_limit=1)
def overtime(self: Any, marker: str) -> None:
    note("overtime", marker=marker, retries=self.request.retries)
    time.sleep(5)
