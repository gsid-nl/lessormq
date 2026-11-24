"""LessorMQ for Celery: a kombu transport and a base task that leave retries, backoff
and the dead-letter set to the LessorMQ server.

    import lessormq_celery

    app = Celery("shop", task_cls=lessormq_celery.Task, broker="lessormq://mq.example.com")
    app.conf.broker_transport_options = {"api_key": os.environ["LESSORMQ_KEY"]}

Importing this package registers the ``lessormq`` broker URL scheme with kombu.
"""

from kombu.transport import TRANSPORT_ALIASES

from lessormq import PermanentFailure

from . import checks  # noqa: F401  (connects the worker's startup check)
from .request import Request, ServerRetry
from .task import Task
from .transport import MalformedMessage, Transport

TRANSPORT_ALIASES.setdefault("lessormq", "lessormq_celery.transport:Transport")

__all__ = ["MalformedMessage", "PermanentFailure", "Request", "ServerRetry", "Task", "Transport"]

__version__ = "0.1.0"
