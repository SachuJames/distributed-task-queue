"""Typed error taxonomy (contract sections 4, 5, 7, 11).

Every error carries a machine-readable ``code`` and an ``http_status`` so the
API layer can translate failures into responses without string matching.
"""

from __future__ import annotations

from typing import Any


class DtqError(Exception):
    """Base class for all engine errors."""

    code: str = "DTQ_ERROR"
    http_status: int = 500

    def __init__(self, message: str = "", *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message or self.code)
        self.message: str = message or self.code
        self.details: dict[str, Any] = details or {}


class InvalidTaskError(DtqError):
    """Submitted task failed validation (400)."""

    code = "INVALID_TASK"
    http_status = 400


class UnknownTaskTypeError(DtqError):
    """task_type is not in the handler registry (400)."""

    code = "UNKNOWN_TASK_TYPE"
    http_status = 400


class TaskTimeoutError(DtqError):
    """Handler execution exceeded timeout_ms (408).

    Not part of the contract's API error list; 408 is the semantically
    correct HTTP status for a timeout. The engine records this on the task
    hash rather than returning it over HTTP.
    """

    code = "TASK_TIMEOUT"
    http_status = 408


class TaskRetryExhaustedError(DtqError):
    """Attempts exhausted for a retryable failure (422).

    Not part of the contract's API error list; 422 marks the task as no
    longer processable. The engine records this on the task hash and routes
    to DLQ/FAILED rather than returning it over HTTP.
    """

    code = "TASK_RETRY_EXHAUSTED"
    http_status = 422


class QueueFullError(DtqError):
    """Admission refused: queue depth reached DTQ_MAX_QUEUE_DEPTH (429)."""

    code = "QUEUE_FULL"
    http_status = 429

    def __init__(
        self,
        message: str = "",
        *,
        retry_after_s: int = 5,
        details: dict[str, Any] | None = None,
    ) -> None:
        merged = {"retry_after_s": retry_after_s, **(details or {})}
        super().__init__(message, details=merged)
        self.retry_after_s = retry_after_s


class RedisUnavailableError(DtqError):
    """Redis is unreachable or not responding (503)."""

    code = "REDIS_UNAVAILABLE"
    http_status = 503


class TaskCancelledError(DtqError):
    """Operation refused because the task was cancelled (409)."""

    code = "TASK_CANCELLED"
    http_status = 409


class WorkerUnavailableError(DtqError):
    """No live worker can take the task (503)."""

    code = "WORKER_UNAVAILABLE"
    http_status = 503


class IdempotencyInProgressError(DtqError):
    """Same idempotency key is already being processed (409)."""

    code = "IDEMPOTENCY_IN_PROGRESS"
    http_status = 409


class PayloadTooLargeError(DtqError):
    """Payload exceeds DTQ_MAX_PAYLOAD_BYTES (413)."""

    code = "PAYLOAD_TOO_LARGE"
    http_status = 413
