"""Core domain model: task statuses, the Task record, and status transitions.

Only :func:`transition` may change a task's status. Every other module must
route status changes through it so illegal moves are rejected in one place.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, ValidationInfo, field_validator

QUEUE_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
MAX_TIMEOUT_MS = 3_600_000
MAX_METADATA_BYTES = 4096


class InvalidTransition(ValueError):
    """Raised when a task status change is not allowed by VALID_TRANSITIONS."""


class TaskStatus(str, Enum):
    """Lifecycle states of a task (contract section 1)."""

    PENDING = "pending"  # accepted, not yet in a stream (delayed)
    QUEUED = "queued"  # entry exists in a dtq:stream:<queue>:p<N> stream
    RUNNING = "running"  # claimed by a worker, handler executing
    SUCCEEDED = "succeeded"  # terminal: handler returned a result
    RETRYING = "retrying"  # terminal attempt recorded, sits in retry schedule
    FAILED = "failed"  # terminal: attempts exhausted, DLQ disabled
    DEAD_LETTERED = "dead_lettered"  # terminal: attempts exhausted, moved to DLQ
    CANCELLED = "cancelled"  # terminal: cancelled before completion
    EXPIRED = "expired"  # terminal: passed metadata.ttl_seconds before running


TERMINAL_STATUSES = frozenset(
    {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.DEAD_LETTERED,
        TaskStatus.CANCELLED,
        TaskStatus.EXPIRED,
    }
)

#: Allowed outgoing transitions per status. Terminal states have none.
#: "Any non-terminal -> CANCELLED" is folded into each non-terminal entry.
VALID_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.QUEUED, TaskStatus.CANCELLED, TaskStatus.EXPIRED}),
    TaskStatus.QUEUED: frozenset({TaskStatus.RUNNING, TaskStatus.CANCELLED, TaskStatus.EXPIRED}),
    TaskStatus.RUNNING: frozenset(
        {
            TaskStatus.SUCCEEDED,
            TaskStatus.RETRYING,
            TaskStatus.FAILED,
            TaskStatus.DEAD_LETTERED,
            TaskStatus.CANCELLED,
        }
    ),
    TaskStatus.RETRYING: frozenset({TaskStatus.QUEUED, TaskStatus.CANCELLED}),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.DEAD_LETTERED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
    TaskStatus.EXPIRED: frozenset(),
}


def is_terminal(status: TaskStatus) -> bool:
    """Return True when the status has no legal outgoing transition."""
    return status in TERMINAL_STATUSES


def transition(task: Task, new_status: TaskStatus) -> Task:
    """Move ``task`` to ``new_status``, enforcing VALID_TRANSITIONS.

    Raises:
        InvalidTransition: if the move is not in VALID_TRANSITIONS.
    """
    allowed = VALID_TRANSITIONS[task.status]
    if new_status not in allowed:
        raise InvalidTransition(
            f"Cannot transition task {task.task_id} from {task.status.value} to {new_status.value}"
        )
    task.status = new_status
    return task


def _require_tz_aware(value: datetime | None, field_name: str) -> datetime | None:
    if value is not None and value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware (UTC)")
    return value


class Task(BaseModel):
    """A unit of work tracked by the engine (contract section 1)."""

    task_id: str
    idempotency_key: str | None = None
    task_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    available_at: datetime
    attempt: int = Field(default=1, ge=1)
    max_attempts: int = Field(default=5, ge=1)
    priority: int = Field(default=5, ge=0, le=9)
    timeout_ms: int = Field(default=30000, ge=1, le=MAX_TIMEOUT_MS)
    status: TaskStatus = TaskStatus.PENDING
    queue: str = "default"
    metadata: dict[str, Any] = Field(default_factory=dict)

    # Execution metadata recorded on the task hash per attempt.
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_ms: int | None = None
    worker_id: str | None = None
    error_class: str | None = None
    error_message: str | None = None
    retryable: bool | None = None
    retry_delay_ms: int | None = None

    @field_validator("queue")
    @classmethod
    def _validate_queue(cls, value: str) -> str:
        if not QUEUE_NAME_RE.match(value):
            raise ValueError("queue must match ^[a-z0-9][a-z0-9_-]{0,63}$")
        return value

    @field_validator("task_type")
    @classmethod
    def _validate_task_type(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("task_type must be a non-empty string")
        return value

    @field_validator("created_at", "available_at", "started_at", "finished_at")
    @classmethod
    def _validate_tz_aware(cls, value: datetime | None, info: ValidationInfo) -> datetime | None:
        return _require_tz_aware(value, info.field_name or "datetime field")

    def payload_json(self) -> str:
        """Serialize the payload the way it is stored in Redis and streams."""
        return json.dumps(self.payload, separators=(",", ":"))

    def metadata_json(self) -> str:
        """Serialize the metadata the way it is stored in the task hash."""
        return json.dumps(self.metadata, separators=(",", ":"))
