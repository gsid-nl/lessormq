from __future__ import annotations

from typing import Any

import httpx
import pytest

from tests import tasks
from tests.fake import Fake


@pytest.fixture(autouse=True)
def clear_seen() -> None:
    tasks.seen.clear()


@pytest.fixture
def fake(settings: Any) -> Fake:
    """A scripted server behind the default task backend."""
    fake = Fake()
    settings.TASKS = backend_settings(fake)
    return fake


def backend_settings(fake: Fake, **options: Any) -> dict[str, Any]:
    return {
        "default": {
            "BACKEND": "lessormq_django.LessorMQBackend",
            "QUEUES": ["default", "mail"],
            "OPTIONS": {
                "URL": "https://mq.test",
                "API_KEY": "k",
                "QUEUE_PREFIX": "shop-",
                "CLIENT": {"http": httpx.Client(transport=fake.transport()), "retry_delay": 0},
                **options,
            },
        }
    }
