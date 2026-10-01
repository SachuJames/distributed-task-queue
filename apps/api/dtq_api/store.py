"""Redis data-access layer for the DTQ API and CLI.

Implements the CONTRACT-described shared interfaces used by the apps
(dtq_queue.ingest.submit_task, dtq_queue.task_state.get_task, dtq_queue.streams
helpers, dtq_events.bus, dtq_idem.store) against the contract's exact Redis
key schema (dtq_core.keys, section 2).

Every function takes an explicit redis.asyncio client so the API (request
scoped, shared pool) and the CLI (process scoped) share one implementation.

Notes on deviations, all deliberate and documented:
- Task attempt starts at 1 (shared dtq_core.models.Task requires ge=1), so
  requeue resets attempt to 1 rather than the contract text's 0.
- DEAD_LETTERED -> QUEUED on requeue bypasses dtq_core.models.transition
  (admin resurrection, outside the worker lifecycle).
- cancel_requested is stored as an extra hash field (the shared Task model
  has no such field; from_hash ignores it).
- Cancelled tasks mark their idempotency record "failed" (shared idem store
  only allows processing/completed/failed); resubmission replays the
  cancelled task as a duplicate instead of blocking for the TTL.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import redis.asyncio
from dtq_core import keys
from dtq_core.models import Task, TaskStatus, is_terminal
from dtq_events.schemas import EventType, make_event
from dtq_idem import store as idem_store
from dtq_queue import ingest as ingest_mod
from dtq_queue import streams, task_state
from dtq_tasks.registry import TASK_REGISTRY
from redis.exceptions import ResponseError

from dtq_api import metrics as metrics_mod
from dtq_api.compat import (
    CancelRequestedError,  # noqa: F401  (re-exported for routes)
    DLQEntryNotFoundError,
    PayloadTooLargeError,
    QueueFullError,
    Settings,
    TaskConflictError,
    TaskNotFoundError,
    TaskTerminalError,
    bus_publish,
    is_task_type_registered,
)

log = logging.getLogger(__name__)

RedisClient = redis.asyncio.Redis  # always constructed with decode_responses=True


# ---------------------------------------------------------------------------
# Records (app-level views; tasks themselves are dtq_core.models.Task)
# ---------------------------------------------------------------------------


@dataclass
class DlqEntry:
    entry_id: str
    task_id: str
    queue: str
    task_type: str
    attempt: int
    max_attempts: int
    priority: int
    timeout_ms: int
    failed_at: datetime | None
    attempts_made: int
    last_error: str
    last_error_class: str
    retryable: bool
    worker_id: str
    original_queue: str

    @classmethod
    def from_entry(cls, entry_id: str, fields: dict[str, str]) -> DlqEntry:
        failed_at_ms = fields.get("failed_at_ms", "")
        failed_at = (
            datetime.fromtimestamp(int(failed_at_ms) / 1000, tz=UTC) if failed_at_ms else None
        )
        return cls(
            entry_id=entry_id,
            task_id=fields.get("task_id", ""),
            queue=fields.get("queue", ""),
            task_type=fields.get("task_type", ""),
            attempt=int(fields.get("attempt", "0") or "0"),
            max_attempts=int(fields.get("max_attempts", "5") or "5"),
            priority=int(fields.get("priority", "5") or "5"),
            timeout_ms=int(fields.get("timeout_ms", "30000") or "30000"),
            failed_at=failed_at,
            attempts_made=int(fields.get("attempts_made", "0") or "0"),
            last_error=fields.get("last_error", ""),
            last_error_class=fields.get("last_error_class", ""),
            retryable=fields.get("retryable", "0") == "1",
            worker_id=fields.get("worker_id", ""),
            original_queue=fields.get("original_queue", ""),
        )


@dataclass
class WorkerRecord:
    worker_id: str
    hostname: str
    status: str
    started_at: float | None
    last_heartbeat: float | None
    active_tasks: int
    concurrency: int
    tasks_processed: int
    tasks_failed: int
    tasks_retried: int

    @classmethod
    def from_hash(cls, worker_id: str, data: dict[str, str]) -> WorkerRecord:
        def opt_float(value: str) -> float | None:
            try:
                return float(value) if value else None
            except ValueError:
                return None

        def opt_int(value: str) -> int:
            try:
                return int(value) if value else 0
            except ValueError:
                return 0

        return cls(
            worker_id=worker_id,
            hostname=data.get("hostname", ""),
            status=data.get("status", ""),
            started_at=opt_float(data.get("started_at", "")),
            last_heartbeat=opt_float(data.get("last_heartbeat", "")),
            active_tasks=opt_int(data.get("active_tasks", "")),
            concurrency=opt_int(data.get("concurrency", "")),
            tasks_processed=opt_int(data.get("tasks_processed", "")),
            tasks_failed=opt_int(data.get("tasks_failed", "")),
            tasks_retried=opt_int(data.get("tasks_retried", "")),
        )

    def is_live(self, now: float | None = None, interval_s: float = 5.0) -> bool:
        """Liveness from heartbeat age only; never from record existence."""
        if self.last_heartbeat is None:
            return False
        current = now if now is not None else datetime.now(UTC).timestamp()
        return (current - self.last_heartbeat) <= 3 * interval_s


@dataclass
class SubmitOutcome:
    task: Task
    created: bool  # False when an idempotent replay returned the stored task


@dataclass
class CancelOutcome:
    task: Task
    cancel_requested: bool  # True when running: cancellation requested, not done


# ---------------------------------------------------------------------------
# Ingest (dtq_queue.ingest.submit_task)
# ---------------------------------------------------------------------------


async def submit_task(
    redis: RedisClient,
    settings: Settings,
    *,
    task_type: str,
    payload: dict[str, Any],
    queue: str = "default",
    idempotency_key: str | None = None,
    max_attempts: int = 5,
    priority: int = 5,
    timeout_ms: int | None = None,
    delay_seconds: float = 0.0,
    metadata: dict[str, Any] | None = None,
) -> SubmitOutcome:
    """Validate, dedupe, and persist a task via the shared ingest pipeline."""
    # Unknown-type validation only applies when this process knows the
    # handler set (task modules loaded). With an empty registry the check
    # cannot run; the worker still dead-letters unknown types without
    # executing anything.
    check_registered = is_task_type_registered if TASK_REGISTRY else None
    try:
        task, is_duplicate = await ingest_mod.submit_task(
            redis,
            task_type=task_type,
            payload=payload,
            queue=queue,
            idempotency_key=idempotency_key,
            max_attempts=max_attempts,
            priority=priority,
            timeout_ms=timeout_ms,
            delay_seconds=delay_seconds,
            metadata=metadata,
            settings=settings,
            is_registered=check_registered,
        )
    except QueueFullError:
        metrics_mod.get_metrics().ingest_rejected_total.labels(
            queue=queue, reason="queue_full"
        ).inc()
        raise
    except PayloadTooLargeError:
        metrics_mod.get_metrics().ingest_rejected_total.labels(
            queue=queue, reason="payload_too_large"
        ).inc()
        raise
    metrics_mod.get_metrics().tasks_submitted_total.labels(queue=queue, task_type=task_type).inc()
    return SubmitOutcome(task=task, created=not is_duplicate)


# ---------------------------------------------------------------------------
# Task state (dtq_queue.task_state.get_task)
# ---------------------------------------------------------------------------


async def get_task(redis: RedisClient, task_id: str) -> Task | None:
    return await task_state.get_task(redis, task_id)


async def list_recent_tasks(
    redis: RedisClient,
    *,
    queue: str | None = None,
    status: TaskStatus | None = None,
    limit: int = 50,
) -> list[Task]:
    """Recent tasks first, filtered by queue/status. Exact filtering."""
    results: list[Task] = []
    offset = 0
    batch = 200
    scanned = 0
    while len(results) < limit and scanned < keys.RECENT_ZSET_MAXLEN:
        ids = await redis.zrevrange(keys.RECENT_ZSET, offset, offset + batch - 1)
        if not ids:
            break
        scanned += len(ids)
        offset += batch
        async with redis.pipeline() as pipe:
            for task_id in ids:
                pipe.hgetall(keys.task_key(task_id))
            hashes = await pipe.execute()
        for _task_id, data in zip(ids, hashes, strict=False):
            if not data:
                continue
            try:
                record = task_state.from_hash({str(k): str(v) for k, v in dict(data).items()})
            except (ValueError, KeyError) as exc:
                log.debug("skipping unparsable task hash: %s", exc)
                continue
            if queue is not None and record.queue != queue:
                continue
            if status is not None and record.status != status:
                continue
            results.append(record)
            if len(results) >= limit:
                break
    return results


# ---------------------------------------------------------------------------
# Queue introspection (dtq_queue.streams helpers)
# ---------------------------------------------------------------------------


async def ensure_consumer_groups(redis: RedisClient, queues: list[str] | None = None) -> None:
    discovered = queues if queues is not None else await discover_queues(redis)
    await streams.ensure_consumer_groups(redis, discovered)


async def streams_missing_group(redis: RedisClient) -> list[str]:
    """Stream keys whose consumer group is absent. Used by /ready."""
    missing: list[str] = []
    for queue in await discover_queues(redis):
        for stream in keys.priority_streams(queue):
            try:
                groups = await redis.xinfo_groups(stream)
            except ResponseError as exc:
                log.debug("xinfo_groups failed for %s: %s", stream, exc)
                continue
            names = [g.get("name") for g in groups] if isinstance(groups, list) else []
            if keys.CONSUMER_GROUP not in names:
                missing.append(stream)
    return missing


async def queue_depth(redis: RedisClient, queue: str) -> int:
    """Sum of XLEN across the queue's priority streams. Exact."""
    return await streams.queue_depth(redis, queue)


async def queue_pending(redis: RedisClient, queue: str) -> int:
    """Sum of XPENDING across the queue's priority streams. Exact."""
    total = 0
    for stream in keys.priority_streams(queue):
        try:
            total += await streams.xpending_count(redis, stream, keys.CONSUMER_GROUP)
        except ResponseError as exc:
            # no stream or no group yet
            log.debug("xpending failed for %s: %s", stream, exc)
            continue
    return total


async def discover_queues(redis: RedisClient) -> list[str]:
    """Queue names seen in stream or paused keys. Best effort discovery."""
    queues: set[str] = set()
    async for key in redis.scan_iter(match="dtq:stream:*:p[0-9]", count=500):
        parts = key.split(":")
        if len(parts) == 4 and parts[0] == "dtq" and parts[1] == "stream":
            queues.add(parts[2])
    async for key in redis.scan_iter(match="dtq:queue:*:paused", count=500):
        parts = key.split(":")
        if len(parts) == 4 and parts[0] == "dtq" and parts[1] == "queue":
            queues.add(parts[2])
    return sorted(queues)


async def retry_scheduled_by_queue(redis: RedisClient) -> dict[str, int]:
    """Count of retry/delay schedule members per queue. Exact point-in-time."""
    members = await redis.zrange(keys.RETRY_SCHEDULE, 0, -1)
    if not members:
        return {}
    async with redis.pipeline() as pipe:
        for member in members:
            pipe.hget(keys.task_key(member), "queue")
        found = await pipe.execute()
    counts: dict[str, int] = {}
    for queue in found:
        if queue:
            counts[queue] = counts.get(queue, 0) + 1
    return counts


async def dlq_depth(redis: RedisClient) -> int:
    return int(await redis.xlen(keys.DLQ_STREAM))


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------


async def cancel_task(redis: RedisClient, settings: Settings, task_id: str) -> CancelOutcome:
    task = await task_state.get_task(redis, task_id)
    if task is None:
        raise TaskNotFoundError(f"task {task_id} not found")
    if is_terminal(task.status):
        raise TaskTerminalError(f"task {task_id} is already terminal ({task.status.value})")
    if task.status == TaskStatus.RUNNING:
        # Best effort: the worker watches the cancel_requested hash field.
        key = keys.task_key(task_id)
        async with redis.pipeline() as pipe:
            pipe.hset(key, "cancel_requested", "1")
            pipe.expire(key, settings.task_retention_s)
            await pipe.execute()
        await bus_publish(
            redis,
            make_event(
                EventType.TASK_CANCELLED,
                task_id=task_id,
                queue=task.queue,
                task_type=task.task_type,
                attempt=task.attempt,
                metadata={"cancel_requested": True},
            ),
        )
        updated = await task_state.get_task(redis, task_id)
        if updated is None:  # pragma: no cover - raced with expiry
            raise TaskNotFoundError(f"task {task_id} not found")
        return CancelOutcome(task=updated, cancel_requested=True)

    # QUEUED / PENDING / RETRYING: mark cancelled now; workers skip the entry
    # when they see the hash status (and the cancelled set for queued tasks).
    updated = await task_state.update_task(
        redis, task_id, TaskStatus.CANCELLED, settings.task_retention_s
    )
    async with redis.pipeline() as pipe:
        pipe.sadd(keys.CANCELLED_SET, task_id)
        pipe.zrem(keys.RETRY_SCHEDULE, task_id)
        pipe.zrem(keys.RETRY_CLAIMED, task_id)
        await pipe.execute()
    if task.idempotency_key:
        # Shared idem states are processing/completed/failed; a cancelled task
        # replays as a duplicate instead of blocking for the TTL.
        await idem_store.complete(redis, task.queue, task.idempotency_key, "failed")
    await bus_publish(
        redis,
        make_event(
            EventType.TASK_CANCELLED,
            task_id=task_id,
            queue=task.queue,
            task_type=task.task_type,
            attempt=task.attempt,
        ),
    )
    metrics_mod.get_metrics().tasks_completed_total.labels(
        queue=task.queue, task_type=task.task_type, status="cancelled"
    ).inc()
    return CancelOutcome(task=updated, cancel_requested=False)


# ---------------------------------------------------------------------------
# Dead-letter queue
# ---------------------------------------------------------------------------


async def dlq_add(
    redis: RedisClient,
    settings: Settings,
    task_id: str,
    *,
    last_error: str = "",
    last_error_class: str = "",
    retryable: bool = False,
    worker_id: str = "",
) -> str:
    """Move a task to the DLQ stream. Simulates the worker failure path.

    Used by tests and the CLI; production DLQ routing lives in the worker.
    """
    task = await task_state.get_task(redis, task_id)
    if task is None:
        raise TaskNotFoundError(f"task {task_id} not found")
    now = datetime.now(UTC)
    fields = streams.stream_entry_fields(task)
    fields.update(
        {
            "failed_at_ms": str(int(now.timestamp() * 1000)),
            "attempts_made": str(task.attempt),
            "last_error": task_state.sanitize_error_message(last_error),
            "last_error_class": last_error_class,
            "retryable": "1" if retryable else "0",
            "worker_id": worker_id,
            "original_queue": task.queue,
        }
    )
    entry_id = await streams.dlq_xadd(redis, fields, maxlen=settings.dlq_maxlen)
    task.status = TaskStatus.DEAD_LETTERED
    task.finished_at = now
    task.error_class = last_error_class or None
    task.error_message = task_state.sanitize_error_message(last_error) or None
    task.retryable = retryable
    await task_state.save_task(redis, task, settings.task_retention_s)
    if task.idempotency_key:
        await idem_store.complete(redis, task.queue, task.idempotency_key, "failed")
    await bus_publish(
        redis,
        make_event(
            EventType.TASK_DEAD_LETTERED,
            task_id=task_id,
            queue=task.queue,
            task_type=task.task_type,
            attempt=task.attempt,
            worker_id=worker_id or None,
            metadata={"last_error": task_state.sanitize_error_message(last_error)},
        ),
    )
    metrics = metrics_mod.get_metrics()
    metrics.tasks_dead_lettered_total.labels(queue=task.queue, task_type=task.task_type).inc()
    metrics.tasks_completed_total.labels(
        queue=task.queue, task_type=task.task_type, status="dead_lettered"
    ).inc()
    return entry_id


async def dlq_list(redis: RedisClient, limit: int = 50) -> list[DlqEntry]:
    entries = await streams.dlq_range(redis, count=limit)
    return [DlqEntry.from_entry(entry_id, fields) for entry_id, fields in entries]


async def dlq_find(redis: RedisClient, task_id: str, scan_limit: int = 10000) -> DlqEntry | None:
    """Find a DLQ entry by task_id, scanning newest first."""
    last = "+"
    seen = 0
    batch = 500
    while seen < scan_limit:
        raw = await redis.xrevrange(keys.DLQ_STREAM, max=last, count=batch)
        if not raw:
            break
        for entry_id, fields in raw:
            seen += 1
            if fields.get("task_id") == task_id:
                return DlqEntry.from_entry(
                    str(entry_id), {str(k): str(v) for k, v in dict(fields).items()}
                )
        last = "(" + str(raw[-1][0])
    return None


async def dlq_requeue(redis: RedisClient, settings: Settings, task_id: str) -> Task:
    """Requeue a dead-lettered task.

    Attempt resets to 1 (the shared Task model requires ge=1; this is the
    "not yet executed" value the ingest path uses), and the previous attempt
    count is preserved in metadata.previous_attempts. The status move
    DEAD_LETTERED -> QUEUED is an admin resurrection outside the worker
    lifecycle, so it bypasses dtq_core.models.transition.
    """
    entry = await dlq_find(redis, task_id)
    if entry is None:
        raise DLQEntryNotFoundError(f"no DLQ entry for task {task_id}")
    task = await task_state.get_task(redis, task_id)
    if task is None:
        raise TaskNotFoundError(f"task {task_id} not found")
    if task.status != TaskStatus.DEAD_LETTERED:
        raise TaskConflictError(f"task {task_id} is not dead-lettered")
    previous = task.metadata.get("previous_attempts")
    previous_attempts = list(previous) if isinstance(previous, list) else []
    previous_attempts.append(task.attempt)
    metadata = dict(task.metadata)
    metadata["previous_attempts"] = previous_attempts
    metadata.pop("discarded", None)
    now = datetime.now(UTC)
    task.status = TaskStatus.QUEUED
    task.attempt = 1
    task.available_at = now
    task.metadata = metadata
    task.started_at = None
    task.finished_at = None
    task.duration_ms = None
    task.worker_id = None
    task.error_class = None
    task.error_message = None
    task.retryable = None
    task.retry_delay_ms = None
    await task_state.save_task(redis, task, settings.task_retention_s)
    await streams.ensure_consumer_groups(redis, [task.queue])
    await streams.xadd_task(
        redis, keys.stream_name(task.queue, task.priority), streams.stream_entry_fields(task)
    )
    await streams.dlq_xdel(redis, entry.entry_id)
    # redis-py types some commands as Awaitable[T] | T; cast through Any.
    await cast(Any, redis.hdel(keys.task_key(task_id), "cancel_requested"))
    if task.idempotency_key:
        # Reset to processing so a resubmission 409s instead of replaying.
        await redis.set(
            keys.idem_key(task.queue, task.idempotency_key),
            json.dumps(
                {
                    "task_id": task_id,
                    "state": "processing",
                    "updated_at": now.isoformat(),
                }
            ),
            ex=settings.idem_ttl_s,
        )
    await bus_publish(
        redis,
        make_event(
            EventType.TASK_RETRIED,
            task_id=task_id,
            queue=task.queue,
            task_type=task.task_type,
            attempt=task.attempt,
            metadata={"from_dlq": True, "previous_attempts": len(previous_attempts)},
        ),
    )
    metrics_mod.get_metrics().tasks_retried_total.labels(
        queue=task.queue, task_type=task.task_type
    ).inc()
    return task


async def dlq_discard(redis: RedisClient, settings: Settings, task_id: str) -> DlqEntry:
    """Remove a DLQ entry without requeueing. The task hash keeps status
    DEAD_LETTERED and records metadata.discarded=true. Never silent."""
    entry = await dlq_find(redis, task_id)
    if entry is None:
        raise DLQEntryNotFoundError(f"no DLQ entry for task {task_id}")
    task = await task_state.get_task(redis, task_id)
    if task is None:
        raise TaskNotFoundError(f"task {task_id} not found")
    await streams.dlq_xdel(redis, entry.entry_id)
    metadata = dict(task.metadata)
    metadata["discarded"] = True
    task.metadata = metadata
    await task_state.save_task(redis, task, settings.task_retention_s)
    return entry


async def dlq_purge(redis: RedisClient) -> int:
    """Remove all DLQ entries. Returns the number removed."""
    return await streams.trim_stream(redis, keys.DLQ_STREAM, 0)


# ---------------------------------------------------------------------------
# Queue admin
# ---------------------------------------------------------------------------


async def pause_queue(redis: RedisClient, queue: str) -> None:
    await redis.set(keys.pause_key(queue), "1")


async def resume_queue(redis: RedisClient, queue: str) -> None:
    await redis.delete(keys.pause_key(queue))


async def is_paused(redis: RedisClient, queue: str) -> bool:
    exists: Any = await redis.exists(keys.pause_key(queue))
    return bool(exists == 1)


async def purge_queue(redis: RedisClient, queue: str) -> dict[str, int]:
    """Trim a queue's streams and drop its retry-schedule entries. CLI helper."""
    removed_streams = 0
    for stream in keys.priority_streams(queue):
        removed_streams += await streams.trim_stream(redis, stream, 0)
    members = await redis.zrange(keys.RETRY_SCHEDULE, 0, -1)
    removed_schedule = 0
    if members:
        async with redis.pipeline() as pipe:
            for member in members:
                pipe.hget(keys.task_key(member), "queue")
            found = await pipe.execute()
        doomed = [m for m, q in zip(members, found, strict=False) if q == queue]
        if doomed:
            # redis-py types some commands as Awaitable[T] | T; cast through Any.
            zrem_result: Any = await cast(Any, redis.zrem(keys.RETRY_SCHEDULE, *doomed))
            removed_schedule = int(zrem_result)
    return {
        "stream_entries_removed": removed_streams,
        "schedule_entries_removed": removed_schedule,
    }


# ---------------------------------------------------------------------------
# Workers (no shared worker registry yet; heartbeat records read directly)
# ---------------------------------------------------------------------------


async def list_workers(redis: RedisClient) -> list[WorkerRecord]:
    # redis-py types some commands as Awaitable[T] | T; cast through Any.
    raw_ids: Any = await cast(Any, redis.smembers(keys.WORKERS_SET))
    ids = {str(i) for i in raw_ids}
    if not ids:
        return []
    async with redis.pipeline() as pipe:
        for worker_id in ids:
            pipe.hgetall(keys.worker_key(worker_id))
        hashes = await pipe.execute()
    records = []
    for worker_id, data in zip(ids, hashes, strict=False):
        if data:
            records.append(
                WorkerRecord.from_hash(
                    str(worker_id), {str(k): str(v) for k, v in dict(data).items()}
                )
            )
    return records


async def get_worker(redis: RedisClient, worker_id: str) -> WorkerRecord | None:
    # redis-py types some commands as Awaitable[T] | T; cast through Any.
    raw_data: Any = await cast(Any, redis.hgetall(keys.worker_key(worker_id)))
    if not raw_data:
        return None
    data = {str(k): str(v) for k, v in dict(raw_data).items()}
    return WorkerRecord.from_hash(worker_id, data)


__all__ = [
    "CancelOutcome",
    "DlqEntry",
    "RedisClient",
    "SubmitOutcome",
    "WorkerRecord",
    "cancel_task",
    "discover_queues",
    "dlq_add",
    "dlq_depth",
    "dlq_discard",
    "dlq_find",
    "dlq_list",
    "dlq_purge",
    "dlq_requeue",
    "ensure_consumer_groups",
    "get_task",
    "get_worker",
    "is_paused",
    "list_recent_tasks",
    "list_workers",
    "pause_queue",
    "purge_queue",
    "queue_depth",
    "queue_pending",
    "resume_queue",
    "retry_scheduled_by_queue",
    "streams_missing_group",
    "submit_task",
]
