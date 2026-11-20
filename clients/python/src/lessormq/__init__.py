"""LessorMQ for Python: the message queue over HTTP where the server decides and
clients only claim, run, ack or fail.

    from lessormq import Client, Worker

    client = Client("https://mq.example.com", api_key)
    client.publish("mail", {"to": "ada@example.com"})
    Worker(client, "mail", lambda message, job: send(message.body)).run()
"""

from ._client import AsyncClient, Client
from ._errors import (
    ApiError,
    AuthenticationError,
    ConflictError,
    ForbiddenError,
    LeaseLostError,
    LessorMQError,
    NotFoundError,
    PayloadTooLargeError,
    PermanentFailure,
    RateLimitedError,
    ServerError,
    TransportError,
    ValidationError,
)
from ._models import FailResult, Lease, Message, Published, Stats
from ._worker import AsyncJob, AsyncWorker, Job, Worker

__all__ = [
    "ApiError",
    "AsyncClient",
    "AsyncJob",
    "AsyncWorker",
    "AuthenticationError",
    "Client",
    "ConflictError",
    "FailResult",
    "ForbiddenError",
    "Job",
    "Lease",
    "LeaseLostError",
    "LessorMQError",
    "Message",
    "NotFoundError",
    "PayloadTooLargeError",
    "PermanentFailure",
    "Published",
    "RateLimitedError",
    "ServerError",
    "Stats",
    "TransportError",
    "ValidationError",
    "Worker",
]

__version__ = "0.1.0"
