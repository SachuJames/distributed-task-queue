"""Adapters over the shared DTQ packages.

Thin integration layer between the API/CLI apps and the shared packages
(dtq_core, dtq_config, dtq_queue, dtq_retry, dtq_idem, dtq_events,
dtq_metrics, dtq_tasks). It re-exports the CONTRACT-described interfaces
used across the apps and adds the small pieces the shared packages do not
provide yet:

- Settings: shared dtq_config.settings.Settings plus DTQ_TEST_REDIS_DB /
  DTQ_TEST_REDIS_URL handling (effective_redis_url) for isolated test runs.
- Errors: shared dtq_core.errors hierarchy plus the API-specific errors the
  shared taxonomy does not define (not-found, terminal, auth).
- Registry: is_task_type_registered() over dtq_tasks.registry.
- Event helpers: re-exports of dtq_events bus/schemas pieces.
"""

from __future__ import annotations

import hmac
from datetime import UTC, datetime
from typing import Any

from dtq_config.settings import Settings as SharedSettings
from dtq_core.errors import (
    DtqError,
    IdempotencyInProgressError,
    InvalidTaskError,
    PayloadTooLargeError,
    QueueFullError,
    RedisUnavailableError,
    TaskCancelledError,
    UnknownTaskTypeError,
)
from dtq_core.models import QUEUE_NAME_RE, TERMINAL_STATUSES, Task, TaskStatus, is_terminal
from dtq_events import bus as events_bus
from dtq_events.schemas import EventType, make_event
from dtq_tasks.registry import get_handler
from pydantic import Field

__all__ = [
    "DTQError",
    "TERMINAL_STATUSES",
    "CancelRequestedError",
    "DLQEntryNotFoundError",
    "DtqError",
    "EventType",
    "ForbiddenError",
    "IdempotencyInProgressError",
    "InvalidTaskError",
    "PayloadTooLargeError",
    "QueueFullError",
    "RedisUnavailableError",
    "Settings",
    "Task",
    "TaskCancelledError",
    "TaskConflictError",
    "TaskNotFoundError",
    "TaskStatus",
    "TaskTerminalError",
    "UnauthorizedError",
    "UnknownTaskTypeError",
    "WorkerNotFoundError",
    "api_key_matches",
    "bus_publish",
    "bus_subscribe",
    "events_bus",
    "is_task_type_registered",
    "is_terminal",
    "is_valid_queue_name",
    "make_event",
    "utcnow_iso",
]

# Backwards-friendly alias: the shared base is named DtqError.
DTQError = DtqError


class TaskNotFoundError(DtqError):
    """Task hash is missing or expired (404)."""

    code = "TASK_NOT_FOUND"
    http_status = 404


class TaskTerminalError(DtqError):
    """Operation refused: the task is already in a terminal state (410)."""

    code = "TASK_TERMINAL"
    http_status = 410


class TaskConflictError(DtqError):
    """Operation conflicts with the task's current state (409)."""

    code = "TASK_CONFLICT"
    http_status = 409


class CancelRequestedError(TaskConflictError):
    """Cancel on a running task: cancellation requested, not yet done (409)."""

    code = "CANCEL_REQUESTED"


class WorkerNotFoundError(DtqError):
    """No worker record with that id (404)."""

    code = "WORKER_NOT_FOUND"
    http_status = 404


class DLQEntryNotFoundError(DtqError):
    """No dead-letter entry for that task id (404)."""

    code = "DLQ_ENTRY_NOT_FOUND"
    http_status = 404


class UnauthorizedError(DtqError):
    """Missing API key on a protected operation (401)."""

    code = "UNAUTHORIZED"
    http_status = 401


class ForbiddenError(DtqError):
    """Wrong API key on a protected operation (403)."""

    code = "FORBIDDEN"
    http_status = 403


class Settings(SharedSettings):
    """Engine settings plus test-database overrides.

    DTQ_TEST_REDIS_DB (and optionally DTQ_TEST_REDIS_URL) select an isolated
    Redis database for tests; effective_redis_url resolves it. In normal
    operation DTQ_REDIS_URL is used unchanged.
    """

    test_redis_url: str | None = Field(default=None)
    test_redis_db: int | None = Field(default=None, ge=0)
    task_modules: str = Field(
        default="",
        description="Comma-separated task handler modules to import at startup "
        "(e.g. 'myapp.tasks'). The API validates task_type against the "
        "handlers registered by these modules.",
    )

    @property
    def effective_redis_url(self) -> str:
        if self.test_redis_db is None:
            return self.redis_url
        base = (self.test_redis_url or "redis://localhost:6379").rstrip("/")
        return f"{base}/{self.test_redis_db}"


def is_task_type_registered(name: str) -> bool:
    """True when dtq_tasks.registry has a handler for this task type."""
    return get_handler(name) is not None


def is_valid_queue_name(name: str) -> bool:
    """Queue name rule, shared with dtq_core.models.QueueNameValidation."""
    return bool(QUEUE_NAME_RE.match(name))


async def bus_publish(redis: Any, event: dict[str, Any]) -> int:
    """Publish a lifecycle event on dtq:events (dtq_events.bus.publish)."""
    return await events_bus.publish(redis, event)


def bus_subscribe(redis: Any) -> Any:
    """Shared event subscriber (dtq_events.bus.subscribe async iterator)."""
    return events_bus.subscribe(redis)


def utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def api_key_matches(provided: str | None, expected: str) -> bool:
    """Constant-time API key comparison. Empty expected means auth disabled."""
    if not expected:
        return True
    if not provided:
        return False
    return hmac.compare_digest(provided, expected)
