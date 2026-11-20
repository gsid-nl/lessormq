"""A scripted LessorMQ server for the unit tests: answers queued per path, every request
recorded."""

from __future__ import annotations

import json
import threading
from collections import defaultdict, deque
from typing import Any

import httpx

LEASE = {
    "receipt": "r1",
    "lease_until": "2030-01-01T00:00:30Z",
    "message": {
        "id": "m1",
        "queue": "mail",
        "body": {"to": "ada@example.com"},
        "headers": {"type": "Welcome"},
        "attempt": 1,
        "max_attempts": 7,
        "created_at": "2030-01-01T00:00:00Z",
    },
}

FAIL_RESULT = {"outcome": "retrying", "available_at": "2030-01-01T00:00:10Z", "attempt": 1, "max_attempts": 7}


class Fake:
    def __init__(self) -> None:
        self.answers: dict[str, deque[Any]] = defaultdict(deque)
        self.requests: list[httpx.Request] = []
        self._lock = threading.Lock()

    def on(self, path: str, *answers: Any) -> Fake:
        """Answers for a path, in order: an httpx.Response, (status, body[, headers]), or
        an exception to raise. The last one repeats."""
        self.answers[path].extend(answers)
        return self

    def calls(self, path: str) -> list[dict[str, Any]]:
        return [json.loads(r.content) if r.content else {} for r in self.requests if _path(r) == path]

    def handler(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            self.requests.append(request)
            queue = self.answers.get(_path(request))
            if not queue:
                return httpx.Response(404, json={"code": "not_found", "detail": "no answer scripted"})
            answer = queue.popleft() if len(queue) > 1 else queue[0]
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, httpx.Response):
            return answer
        status, body, *rest = answer
        headers = rest[0] if rest else {}
        if body is None:
            return httpx.Response(status, headers=headers)
        return httpx.Response(status, json=body, headers=headers)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    def async_transport(self) -> httpx.MockTransport:
        async def handle(request: httpx.Request) -> httpx.Response:
            await request.aread()
            return self.handler(request)

        return httpx.MockTransport(handle)


def _path(request: httpx.Request) -> str:
    """The path as sent, still encoded (q%2F1, not q/1)."""
    return request.url.raw_path.decode("ascii").split("?", 1)[0]
