"""Everything the library raises, and what a handler raises to report a permanent failure."""

from __future__ import annotations

from typing import Any


class LessorMQError(Exception):
    """Everything this library raises."""


class TransportError(LessorMQError):
    """The server could not be reached, after the configured retries."""


class ApiError(LessorMQError):
    """The server answered with an error (RFC 9457 problem details).

    ``code`` is the stable problem code to branch on; the subclasses cover the ones a
    client acts on.
    """

    def __init__(self, status: int, code: str, detail: str = "", errors: list[dict[str, str]] | None = None) -> None:
        self.status = status
        self.code = code
        self.detail = detail
        self.errors = errors or []
        super().__init__(detail or f"LessorMQ error {status} ({code})")

    @staticmethod
    def from_response(status: int, body: Any, retry_after: int | None = None) -> ApiError:
        problem: dict[str, Any] = body if isinstance(body, dict) else {}
        raw_code = problem.get("code")
        raw_detail = problem.get("detail")
        raw_errors = problem.get("errors")
        code = raw_code if isinstance(raw_code, str) else _code_for(status)
        detail = raw_detail if isinstance(raw_detail, str) else ""
        errors = raw_errors if isinstance(raw_errors, list) else []

        if code == "rate_limited":
            return RateLimitedError(status, code, detail, errors, retry_after)
        cls = _BY_CODE.get(code)
        if cls is None:
            cls = ServerError if status >= 500 else ApiError
        return cls(status, code, detail, errors)


class LeaseLostError(ApiError):
    """The receipt is not the current lease of the message.

    The lease expired, was released, or the message was claimed again. Stop reporting
    on it: the server has already made it available again.
    """


class ValidationError(ApiError):
    """The request was rejected as invalid; ``errors`` names each field."""


class AuthenticationError(ApiError):
    """The API key is missing, unknown or revoked."""


class ForbiddenError(ApiError):
    """The key lacks the scope or the queue, or its tenant is suspended (code ``tenant_suspended``)."""


class NotFoundError(ApiError):
    """No such queue or message for this tenant."""


class ConflictError(ApiError):
    """The message is in a state that does not allow this (``invalid_state``)."""


class PayloadTooLargeError(ApiError):
    """Body and headers exceed the queue's ``max_message_bytes``."""


class RateLimitedError(ApiError):
    """The tenant exceeded a quota or rate limit, after the configured retries."""

    def __init__(
        self,
        status: int,
        code: str,
        detail: str = "",
        errors: list[dict[str, str]] | None = None,
        retry_after: int | None = None,
    ) -> None:
        super().__init__(status, code, detail, errors)
        self.retry_after = retry_after


class ServerError(ApiError):
    """The server failed (5xx), after the configured retries."""


class PermanentFailure(Exception):
    """Raise from a handler when the message itself cannot be processed.

    It cannot be decoded, it fails validation: retrying it would only fail again. Raise
    it directly, or mix it into your own exception (``class BadInput(ValueError,
    PermanentFailure)``). A fact for the server; the queue's ``on_permanent`` decides
    what it means.
    """


_BY_CODE: dict[str, type[ApiError]] = {
    "lease_lost": LeaseLostError,
    "validation_failed": ValidationError,
    "unauthorized": AuthenticationError,
    "forbidden": ForbiddenError,
    "tenant_suspended": ForbiddenError,
    "not_found": NotFoundError,
    "invalid_state": ConflictError,
    "payload_too_large": PayloadTooLargeError,
}


def _code_for(status: int) -> str:
    if status == 401:
        return "unauthorized"
    if status == 403:
        return "forbidden"
    if status == 404:
        return "not_found"
    if status == 413:
        return "payload_too_large"
    if status == 429:
        return "rate_limited"
    if status >= 500:
        return "internal"
    return "validation_failed"
