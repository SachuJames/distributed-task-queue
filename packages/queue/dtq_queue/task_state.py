"""Task hash persistence (contract section 2).

Tasks live in ``dtq:task:<id>`` hashes with every field from the domain model
plus per-attempt execution metadata. All status changes go through
:func:`dtq_core.models.transition`; this module is the only place that calls
it outside tests.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from dtq_core.keys import task_key
from dtq_core.models import Task, TaskStatus, transition
from redis.asyncio import Redis

#: Sanitized error messages are capped at 500 chars (contract section 1).
ERROR_MESSAGE_MAX_CHARS = 500


class InvalidTransitionForRedelivery(ValueError):
    """record_redelivery called on a task that is not RUNNING."""


_EMPTY = ""


def _opt_str(value: str | None) -> str:
    return value if value is not None else _EMPTY


def _opt_dt(value: datetime | None) -> str:
    return value.isoformat() if value is not None else _EMPTY


def _none_if_empty(value: str) -> str | None:
    return value if value != _EMPTY else None


def _opt_int(value: str) -> int | None:
    return int(value) if value != _EMPTY else None


def _opt_bool(value: str) -> bool | None:
    if value == _EMPTY:
        return None
    return value == "1"


def to_hash(task: Task) -> dict[str, str]:
    """Serialize a Task to its Redis hash field map (all values strings)."""
    return {
        "task_id": task.task_id,
        "idempotency_key": _opt_str(task.idempotency_key),
        "task_type": task.task_type,
        "payload": task.payload_json(),
        "created_at": task.created_at.isoformat(),
        "available_at": task.available_at.isoformat(),
        "attempt": str(task.attempt),
        "max_attempts": str(task.max_attempts),
        "priority": str(task.priority),
        "timeout_ms": str(task.timeout_ms),
        "status": task.status.value,
        "queue": task.queue,
        "metadata": task.metadata_json(),
        "started_at": _opt_dt(task.started_at),
        "finished_at": _opt_dt(task.finished_at),
        "duration_ms": str(task.duration_ms) if task.duration_ms is not None else _EMPTY,
        "worker_id": _opt_str(task.worker_id),
        "error_class": _opt_str(task.error_class),
        "error_message": _opt_str(task.error_message),
        "retryable": _EMPTY if task.retryable is None else ("1" if task.retryable else "0"),
        "retry_delay_ms": (str(task.retry_delay_ms) if task.retry_delay_ms is not None else _EMPTY),
    }


def from_hash(data: Mapping[str, str]) -> Task:
    """Rebuild a Task from its Redis hash field map."""
    payload: dict[str, Any] = json.loads(data["payload"])
    metadata: dict[str, Any] = json.loads(data["metadata"])
    return Task(
        task_id=data["task_id"],
        idempotency_key=_none_if_empty(data.get("idempotency_key", _EMPTY)),
        task_type=data["task_type"],
        payload=payload,
        created_at=datetime.fromisoformat(data["created_at"]),
        available_at=datetime.fromisoformat(data["available_at"]),
        attempt=int(data["attempt"]),
        max_attempts=int(data["max_attempts"]),
        priority=int(data["priority"]),
        timeout_ms=int(data["timeout_ms"]),
        status=TaskStatus(data["status"]),
        queue=data["queue"],
        metadata=metadata,
        started_at=(datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None),
        finished_at=(
            datetime.fromisoformat(data["finished_at"]) if data.get("finished_at") else None
        ),
        duration_ms=_opt_int(data.get("duration_ms", _EMPTY)),
        worker_id=_none_if_empty(data.get("worker_id", _EMPTY)),
        error_class=_none_if_empty(data.get("error_class", _EMPTY)),
        error_message=_none_if_empty(data.get("error_message", _EMPTY)),
        retryable=_opt_bool(data.get("retryable", _EMPTY)),
        retry_delay_ms=_opt_int(data.get("retry_delay_ms", _EMPTY)),
    )


def sanitize_error_message(message: str) -> str:
    """Strip newlines (log-injection safety) and cap at 500 chars."""
    flat = message.replace("\r", " ").replace("\n", " ")
    return flat[:ERROR_MESSAGE_MAX_CHARS]


async def get_task(redis: Redis, task_id: str) -> Task | None:
    """Load a task by id; None when the hash is missing or expired."""
    # redis-py 6.x types this as Union[Awaitable[T], T]; bind through Any first.
    hgetall_call: Any = redis.hgetall(task_key(task_id))
    data: Any = await hgetall_call
    if not data:
        return None
    return from_hash({str(k): str(v) for k, v in dict(data).items()})


async def save_task(redis: Redis, task: Task, retention_s: int) -> None:
    """Write the task hash and refresh its retention TTL."""
    key = task_key(task.task_id)
    # redis-py 6.x types these as Union[Awaitable[T], T]; awaiting the union
    # directly is a type error, so bind through Any first.
    hset_call: Any = redis.hset(key, mapping=to_hash(task))
    await hset_call
    expire_call: Any = redis.expire(key, retention_s)
    await expire_call


async def _require_task(redis: Redis, task_id: str) -> Task:
    task = await get_task(redis, task_id)
    if task is None:
        raise KeyError(f"unknown task_id: {task_id}")
    return task


async def update_task(redis: Redis, task_id: str, new_status: TaskStatus, retention_s: int) -> Task:
    """Move a task to ``new_status`` via models.transition and persist it."""
    task = await _require_task(redis, task_id)
    transition(task, new_status)
    await save_task(redis, task, retention_s)
    return task


async def record_attempt_start(
    redis: Redis, task_id: str, worker_id: str, retention_s: int
) -> Task:
    """Record the start of a fresh execution: QUEUED -> RUNNING.

    ``attempt`` already holds the upcoming attempt number (assigned at ingest
    or when the scheduler requeued the task), so it is not incremented here.
    """
    task = await _require_task(redis, task_id)
    transition(task, TaskStatus.RUNNING)
    task.started_at = datetime.now(UTC)
    task.finished_at = None
    task.duration_ms = None
    task.worker_id = worker_id
    task.error_class = None
    task.error_message = None
    task.retryable = None
    task.retry_delay_ms = None
    await save_task(redis, task, retention_s)
    return task


async def record_redelivery(redis: Redis, task_id: str, worker_id: str, retention_s: int) -> Task:
    """Record a crash redelivery of an already-RUNNING task.

    Redelivery counts as a new execution, so ``attempt`` increments. Status
    stays RUNNING (no transition), only execution metadata is refreshed.
    """
    task = await _require_task(redis, task_id)
    if task.status is not TaskStatus.RUNNING:
        raise InvalidTransitionForRedelivery(task_id, task.status)
    task.attempt += 1
    task.started_at = datetime.now(UTC)
    task.worker_id = worker_id
    await save_task(redis, task, retention_s)
    return task


async def record_attempt_end(
    redis: Redis,
    task_id: str,
    retention_s: int,
    *,
    error_class: str | None = None,
    error_message: str | None = None,
    retryable: bool | None = None,
    retry_delay_ms: int | None = None,
) -> Task:
    """Write per-attempt execution metadata after the handler finished.

    Status is not changed here; the caller moves it with :func:`update_task`.
    """
    task = await _require_task(redis, task_id)
    now = datetime.now(UTC)
    task.finished_at = now
    if task.started_at is not None:
        task.duration_ms = max(0, int((now - task.started_at).total_seconds() * 1000))
    task.error_class = error_class
    task.error_message = sanitize_error_message(error_message) if error_message else None
    task.retryable = retryable
    task.retry_delay_ms = retry_delay_ms
    await save_task(redis, task, retention_s)
    return task
