"""Task ingestion: validation, idempotency, backpressure, persistence, routing.

Submit order:
  1. validate every field (typed errors, no Redis writes yet)
  2. idempotent replay: a completed/failed key returns the stored task
  3. backpressure: queue depth >= DTQ_MAX_QUEUE_DEPTH -> 429 QUEUE_FULL
  4. claim the idempotency record (SET NX); a racing submit sees 409
  5. persist the task hash, index in the recent zset
  6. route: delayed tasks wait in the retry schedule (status PENDING),
     everything else is XADDed to its priority stream (status QUEUED)
  7. publish TASK_QUEUED
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from dtq_config.settings import Settings
from dtq_core.errors import (
    IdempotencyInProgressError,
    InvalidTaskError,
    PayloadTooLargeError,
    QueueFullError,
    UnknownTaskTypeError,
)
from dtq_core.keys import (
    RECENT_ZSET,
    RECENT_ZSET_MAXLEN,
    RETRY_SCHEDULE,
    idem_key,
    stream_name,
)
from dtq_core.models import MAX_METADATA_BYTES, QUEUE_NAME_RE, Task, TaskStatus
from dtq_events import bus as events_bus
from dtq_events import schemas as event_schemas
from dtq_idem import store as idem_store
from redis.asyncio import Redis

from dtq_queue.streams import queue_depth, stream_entry_fields, xadd_task
from dtq_queue.task_state import get_task, save_task

MAX_TIMEOUT_MS = 3_600_000


def _payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))


def _conflict_error(stored_id: str) -> IdempotencyInProgressError:
    return IdempotencyInProgressError(
        "idempotency key is already being processed",
        details={"task_id": stored_id},
    )


async def submit_task(
    redis: Redis,
    *,
    task_type: str,
    payload: dict[str, Any],
    queue: str = "default",
    idempotency_key: str | None = None,
    max_attempts: int = 5,
    priority: int = 5,
    timeout_ms: int | None = None,
    delay_seconds: float = 0,
    metadata: dict[str, Any] | None = None,
    settings: Settings,
    is_registered: Callable[[str], bool] | None = None,
) -> tuple[Task, bool]:
    """Submit a task. Returns (task, is_duplicate).

    Raises:
        InvalidTaskError: bad queue name, payload, attempts, timeout,
            priority, metadata, or delay.
        UnknownTaskTypeError: task_type not registered (only when
            ``is_registered`` is provided).
        PayloadTooLargeError: payload over DTQ_MAX_PAYLOAD_BYTES.
        QueueFullError: queue depth at DTQ_MAX_QUEUE_DEPTH.
        IdempotencyInProgressError: same key already being processed.
    """
    # 1. validation (no Redis writes yet)
    if not QUEUE_NAME_RE.match(queue):
        raise InvalidTaskError(f"invalid queue name: {queue!r}")
    if not task_type or not task_type.strip():
        raise InvalidTaskError("task_type must be a non-empty string")
    if is_registered is not None and not is_registered(task_type):
        raise UnknownTaskTypeError(f"unknown task_type: {task_type!r}")
    if not isinstance(payload, dict):
        raise InvalidTaskError("payload must be a JSON object")
    if _payload_size_bytes(payload) > settings.max_payload_bytes:
        raise PayloadTooLargeError(
            f"payload exceeds {settings.max_payload_bytes} bytes",
            details={"max_payload_bytes": settings.max_payload_bytes},
        )
    if not 1 <= max_attempts <= settings.max_attempts:
        raise InvalidTaskError(
            f"max_attempts must be 1..{settings.max_attempts}, got {max_attempts}"
        )
    if not 0 <= priority <= 9:
        raise InvalidTaskError(f"priority must be 0..9, got {priority}")
    timeout = settings.task_timeout_ms if timeout_ms is None else timeout_ms
    if not 1 <= timeout <= MAX_TIMEOUT_MS:
        raise InvalidTaskError(f"timeout_ms must be 1..{MAX_TIMEOUT_MS}, got {timeout}")
    meta: dict[str, Any] = {} if metadata is None else metadata
    if not isinstance(meta, dict):
        raise InvalidTaskError("metadata must be a JSON object")
    if len(json.dumps(meta, separators=(",", ":")).encode("utf-8")) > MAX_METADATA_BYTES:
        raise InvalidTaskError(f"metadata exceeds {MAX_METADATA_BYTES} bytes serialized")
    if delay_seconds < 0:
        raise InvalidTaskError("delay_seconds must be >= 0")

    # 2. idempotent replay (read-only: costs nothing, never blocked by depth)
    if idempotency_key:
        peeked = await idem_store.peek(redis, queue, idempotency_key)
        if peeked is not None:
            peek_state, peek_id = peeked
            if peek_state == "processing":
                raise _conflict_error(peek_id)
            duplicate = await get_task(redis, peek_id)
            if duplicate is not None:
                return duplicate, True
            # Stale record (task hash expired); fall through and replace it.

    # 3. backpressure: approximate under concurrent ingest (contract section 7)
    depth = await queue_depth(redis, queue)
    if depth >= settings.max_queue_depth:
        raise QueueFullError(
            f"queue {queue!r} depth {depth} >= limit {settings.max_queue_depth}",
            retry_after_s=5,
            details={"queue": queue, "depth": depth, "limit": settings.max_queue_depth},
        )

    # 4. claim the idempotency record; a racing submit loses here (409)
    task_id = uuid4().hex
    if idempotency_key:
        state, stored_id = await idem_store.acquire(
            redis, queue, idempotency_key, task_id, settings.idem_ttl_s
        )
        if state == "processing":
            raise _conflict_error(stored_id)
        if state in ("completed", "failed"):
            duplicate = await get_task(redis, stored_id)
            if duplicate is not None:
                return duplicate, True
            # Stale record; replace it and retry the claim once.
            await redis.delete(idem_key(queue, idempotency_key))
            state, stored_id = await idem_store.acquire(
                redis, queue, idempotency_key, task_id, settings.idem_ttl_s
            )
            if state == "processing":
                raise _conflict_error(stored_id)
            if state != "created":
                duplicate = await get_task(redis, stored_id)
                if duplicate is not None:
                    return duplicate, True
                raise InvalidTaskError("idempotency record references a missing task")

    # 5. persist the task hash and index it in the recent zset
    now = datetime.now(UTC)
    available_at = now + timedelta(seconds=delay_seconds)
    delayed = delay_seconds > 0
    task = Task(
        task_id=task_id,
        idempotency_key=idempotency_key,
        task_type=task_type,
        payload=payload,
        created_at=now,
        available_at=available_at,
        attempt=1,
        max_attempts=max_attempts,
        priority=priority,
        timeout_ms=timeout,
        status=TaskStatus.PENDING if delayed else TaskStatus.QUEUED,
        queue=queue,
        metadata=meta,
    )
    await save_task(redis, task, settings.task_retention_s)
    created_ms = int(now.timestamp() * 1000)
    await redis.zadd(RECENT_ZSET, {task_id: created_ms})
    await redis.zremrangebyrank(RECENT_ZSET, 0, -(RECENT_ZSET_MAXLEN + 1))

    # 6. route: delayed tasks wait in the retry schedule, else the stream
    if delayed:
        await redis.zadd(RETRY_SCHEDULE, {task_id: int(available_at.timestamp() * 1000)})
    else:
        await xadd_task(redis, stream_name(queue, priority), stream_entry_fields(task))

    # 7. lifecycle event
    await events_bus.publish(
        redis,
        event_schemas.make_event(
            event_schemas.EventType.TASK_QUEUED,
            task_id=task_id,
            queue=queue,
            task_type=task_type,
            attempt=task.attempt,
        ),
    )

    return task, False
