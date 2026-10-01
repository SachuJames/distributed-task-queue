"""Lifecycle event schema (contract section 8).

Events are JSON published on ``dtq:events`` and fanned out to WebSocket
clients: {event_id, ts, type, task_id, queue, task_type, attempt, worker_id,
metadata?}.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import Any


class EventType(str, Enum):
    """All event types emitted by the engine."""

    TASK_QUEUED = "TASK_QUEUED"
    TASK_STARTED = "TASK_STARTED"
    TASK_SUCCEEDED = "TASK_SUCCEEDED"
    TASK_FAILED = "TASK_FAILED"
    TASK_RETRY_SCHEDULED = "TASK_RETRY_SCHEDULED"
    TASK_RETRIED = "TASK_RETRIED"
    TASK_DEAD_LETTERED = "TASK_DEAD_LETTERED"
    TASK_CANCELLED = "TASK_CANCELLED"
    WORKER_STARTED = "WORKER_STARTED"
    WORKER_STOPPED = "WORKER_STOPPED"
    WORKER_STALE = "WORKER_STALE"
    CONFIGURATION_CHANGED = "CONFIGURATION_CHANGED"
    # Worker-side extensions (emitted by the worker service):
    TASK_EXPIRED = "TASK_EXPIRED"
    TASK_DUPLICATE_ABSORBED = "TASK_DUPLICATE_ABSORBED"


def make_event(
    event_type: EventType,
    *,
    task_id: str | None = None,
    queue: str | None = None,
    task_type: str | None = None,
    attempt: int | None = None,
    worker_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an event dict with a uuid event_id and an ISO UTC timestamp."""
    event: dict[str, Any] = {
        "event_id": uuid.uuid4().hex,
        "ts": datetime.now(UTC).isoformat(),
        "type": event_type.value,
    }
    if task_id is not None:
        event["task_id"] = task_id
    if queue is not None:
        event["queue"] = queue
    if task_type is not None:
        event["task_type"] = task_type
    if attempt is not None:
        event["attempt"] = attempt
    if worker_id is not None:
        event["worker_id"] = worker_id
    if metadata is not None:
        event["metadata"] = metadata
    return event
