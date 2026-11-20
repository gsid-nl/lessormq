"""What the server hands back: delivered messages, leases, publish and fail results."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def parse_time(value: str) -> datetime:
    """An RFC 3339 time from the API (always UTC, ``Z``) as an aware datetime."""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def format_time(value: datetime) -> str:
    """An aware datetime as the API writes times: UTC, whole seconds, ``Z``."""
    if value.tzinfo is None:
        raise ValueError("pass an aware datetime (with a timezone)")
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class Message:
    """A delivered message: what a handler runs."""

    id: str
    queue: str
    body: Any
    """The JSON value as published."""
    headers: dict[str, str]
    attempt: int
    max_attempts: int
    created_at: datetime

    @property
    def is_last_attempt(self) -> bool:
        """Whether this attempt is the last one the queue allows."""
        return self.attempt >= self.max_attempts

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> Message:
        headers = data.get("headers")
        return cls(
            id=str(data["id"]),
            queue=str(data["queue"]),
            body=data.get("body"),
            headers=headers if isinstance(headers, dict) else {},
            attempt=int(data["attempt"]),
            max_attempts=int(data["max_attempts"]),
            created_at=parse_time(str(data["created_at"])),
        )


@dataclass
class Lease:
    """A claimed message and the proof of holding it.

    Extend, ack, fail and release take the lease; only its receipt is accepted, and only
    until ``lease_until``. ``extend`` moves ``lease_until``.
    """

    receipt: str
    lease_until: datetime
    message: Message

    def seconds_left(self) -> float:
        return self.lease_until.timestamp() - time.time()

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> Lease:
        return cls(
            receipt=str(data["receipt"]),
            lease_until=parse_time(str(data["lease_until"])),
            message=Message.from_api(data["message"]),
        )


@dataclass(frozen=True)
class Published:
    """The answer to a publish: the message, or the open one with the same dedup key."""

    id: str
    queue: str
    status: str
    created_at: datetime
    available_at: datetime
    duplicate: bool

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> Published:
        return cls(
            id=str(data["id"]),
            queue=str(data["queue"]),
            status=str(data["status"]),
            created_at=parse_time(str(data["created_at"])),
            available_at=parse_time(str(data["available_at"])),
            duplicate=bool(data["duplicate"]),
        )


@dataclass(frozen=True)
class FailResult:
    """What the server did with a failed attempt. For logging: a client never acts on it."""

    RETRYING = "retrying"
    DEAD_LETTERED = "dead_lettered"
    DISCARDED = "discarded"

    outcome: str
    available_at: datetime | None
    attempt: int
    max_attempts: int

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> FailResult:
        available_at = data.get("available_at")
        return cls(
            outcome=str(data["outcome"]),
            available_at=parse_time(str(available_at)) if available_at else None,
            attempt=int(data["attempt"]),
            max_attempts=int(data["max_attempts"]),
        )


@dataclass(frozen=True)
class Stats:
    """A queue's counts, for monitoring. Not part of the protocol."""

    ready: int = 0
    delayed: int = 0
    processing: int = 0
    completed: int = 0
    failed: int = 0
    oldest_ready_seconds: int = 0
    oldest_processing_seconds: int = 0
    other: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> Stats:
        known = {name for name in cls.__dataclass_fields__ if name != "other"}
        values = {name: int(value) for name, value in data.items() if isinstance(value, (int, float))}
        return cls(
            **{name: value for name, value in values.items() if name in known},
            other={name: value for name, value in values.items() if name not in known},
        )
