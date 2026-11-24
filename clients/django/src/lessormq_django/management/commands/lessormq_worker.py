"""``manage.py lessormq_worker``: run the Django tasks of LessorMQ queues."""

from __future__ import annotations

from argparse import ArgumentParser
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, task_backends
from django.tasks.exceptions import InvalidTaskBackend

from lessormq_django.backend import LessorMQBackend
from lessormq_django.worker import run_worker


class Command(BaseCommand):
    help = (
        "Run the tasks of a LessorMQ task backend's queues until SIGTERM or SIGINT. "
        "Retries, backoff and the dead-letter set are the queue's settings on the server."
    )

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--backend",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            help="the task backend in TASKS (default: %(default)s)",
        )
        parser.add_argument(
            "--queue",
            action="append",
            dest="queues",
            metavar="NAME",
            help="a Django queue name, repeatable (default: every queue in the backend's QUEUES)",
        )
        parser.add_argument("--consumer", help="the worker's name on the server (default: host:pid)")
        parser.add_argument("--route", help="the path this worker runs tasks through (a VPN exit, a proxy)")
        parser.add_argument(
            "--wait-seconds",
            type=int,
            default=20,
            help="long-poll wait per claim, at most 20; also the longest a stop waits (default: %(default)s)",
        )
        parser.add_argument("--lease-seconds", type=int, help="lease per claim (default: the queue's)")
        parser.add_argument("--max-tasks", type=int, help="stop each queue's worker after this many tasks")
        parser.add_argument("--max-seconds", type=float, help="stop after running this long")

    def handle(self, *args: Any, **options: Any) -> None:
        alias = options["backend"]
        try:
            backend = task_backends[alias]
        except InvalidTaskBackend as e:
            raise CommandError(str(e)) from e
        if not isinstance(backend, LessorMQBackend):
            raise CommandError(f"Task backend {alias!r} is a {type(backend).__name__}, not a LessorMQBackend")
        queues = options["queues"] or sorted(backend.queues)
        if not queues:
            raise CommandError(f"Task backend {alias!r} allows any queue: name the queues with --queue")
        unknown = [name for name in queues if backend.queues and name not in backend.queues]
        if unknown:
            raise CommandError(f"Not in the QUEUES of task backend {alias!r}: {', '.join(unknown)}")

        self.stdout.write(
            "Running tasks from " + ", ".join(f"{name} ({backend.queue_for(name)})" for name in queues)
        )
        handled = run_worker(
            queues,
            backend=alias,
            consumer=options["consumer"],
            route=options["route"],
            wait_seconds=options["wait_seconds"],
            lease_seconds=options["lease_seconds"],
            max_tasks=options["max_tasks"],
            max_seconds=options["max_seconds"],
        )
        self.stdout.write(f"Stopped after {handled} task(s)")
