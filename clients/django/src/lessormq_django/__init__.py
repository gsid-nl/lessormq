"""LessorMQ for Django: a backend for Django's tasks framework (django.tasks) on
LessorMQ, the message queue over HTTP where the server decides and workers only claim,
run, ack or fail.

    TASKS = {
        "default": {
            "BACKEND": "lessormq_django.LessorMQBackend",
            "OPTIONS": {"URL": "https://mq.example.com", "API_KEY": "lmq_..."},
        }
    }

Run the tasks with ``manage.py lessormq_worker``.
"""

from .backend import LessorMQBackend, LessorMQTaskResult
from .exceptions import LeaseExpired, MalformedTask, PermanentFailure, UnknownTask

__all__ = [
    "LeaseExpired",
    "LessorMQBackend",
    "LessorMQTaskResult",
    "MalformedTask",
    "PermanentFailure",
    "UnknownTask",
]

__version__ = "0.1.0"
