"""Async Redis Streams helpers: priority streams, consumer groups, DLQ."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from dtq_core.keys import (
    CONSUMER_GROUP,
    DLQ_STREAM,
    PRIORITY_BANDS,
    priority_streams,
    stream_name,
)
from dtq_core.models import Task
from redis.asyncio import Redis
from redis.exceptions import ResponseError

#: (stream, entry_id, fields) for one stream message.
StreamMessage = tuple[str, str, dict[str, str]]


def _field_map(fields: Mapping[str, str]) -> dict[Any, Any]:
    """Copy stream fields into the concrete dict type redis-py expects."""
    return {str(k): str(v) for k, v in fields.items()}


def stream_entry_fields(task: Task) -> dict[str, str]:
    """Build the XADD field map for a task (contract section 2)."""
    return {
        "task_id": task.task_id,
        "queue": task.queue,
        "task_type": task.task_type,
        "payload": task.payload_json(),
        "attempt": str(task.attempt),
        "max_attempts": str(task.max_attempts),
        "priority": str(task.priority),
        "timeout_ms": str(task.timeout_ms),
        "idempotency_key": task.idempotency_key or "",
    }


async def ensure_consumer_groups(
    redis: Redis, queues: Iterable[str], group: str = CONSUMER_GROUP
) -> None:
    """Create consumer group ``group`` on all priority streams of each queue.

    Uses MKSTREAM so missing streams are created. Existing groups are left
    alone (BUSYGROUP is ignored).
    """
    for queue in queues:
        for stream in priority_streams(queue):
            try:
                await redis.xgroup_create(stream, group, id="0", mkstream=True)
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise


async def xadd_task(
    redis: Redis,
    stream: str,
    fields: Mapping[str, str],
    maxlen: int | None = None,
) -> str:
    """Append a task entry to a stream, returning the entry id."""
    entry_id: Any = await redis.xadd(stream, _field_map(fields), maxlen=maxlen, approximate=True)
    return str(entry_id)


async def xreadgroup_priority(
    redis: Redis,
    queue: str,
    group: str,
    consumer: str,
    count: int,
    block_ms: int | None = 500,
) -> list[StreamMessage]:
    """Drain priority streams p9 -> p0 with one blocking XREADGROUP.

    A single XREADGROUP call watches all ten priority streams at once, so
    the BLOCK applies to every band: a task arriving on any band wakes the
    reader immediately. Redis returns one element per stream that has data,
    in the order the streams were requested, so iterating the response in
    order yields entries in strict p9 -> p0 priority.

    ``block_ms=None`` (or 0) means "do not block"; any positive value is the
    BLOCK timeout in milliseconds. NOTE: passing block=0 to Redis would block
    indefinitely, so 0 is normalized to None here.
    """
    if block_ms == 0:
        block_ms = None
    # One blocking read across all ten bands, requested p9 -> p0 so the
    # response groups entries in strict priority order. (Inline: mypy
    # context-types the dict from the xreadgroup signature.)
    raw: Any = await redis.xreadgroup(
        group,
        consumer,
        {stream_name(queue, p): ">" for p in range(PRIORITY_BANDS - 1, -1, -1)},
        count=count,
        block=block_ms,
    )
    collected: list[StreamMessage] = []
    if not raw:
        return collected
    for stream_blob, entries in raw:
        for entry_id_blob, fields_blob in entries:
            fields = {str(k): str(v) for k, v in dict(fields_blob).items()}
            collected.append((str(stream_blob), str(entry_id_blob), fields))
    return collected


async def xack(redis: Redis, stream: str, group: str, *entry_ids: str) -> int:
    """Acknowledge entries; returns the number acknowledged."""
    if not entry_ids:
        return 0
    acked: Any = await redis.xack(stream, group, *entry_ids)
    return int(acked)


async def xpending_count(redis: Redis, stream: str, group: str) -> int:
    """Return the total pending (unacked) count for a stream's group."""
    summary: Any = await redis.xpending(stream, group)
    return int(dict(summary).get("pending", 0))


async def xautoclaim(
    redis: Redis,
    stream: str,
    group: str,
    consumer: str,
    min_idle_ms: int,
    count: int = 100,
) -> list[tuple[str, dict[str, str]]]:
    """Reclaim idle pending entries to ``consumer``.

    Returns [(entry_id, fields)] for entries idle longer than ``min_idle_ms``.
    """
    raw: Any = await redis.xautoclaim(
        stream, group, consumer, min_idle_ms, start_id="0-0", count=count
    )
    entries: list[tuple[str, dict[str, str]]] = []
    items = list(raw)[1] if isinstance(raw, list | tuple) and len(raw) > 1 else []
    for entry_id_blob, fields_blob in items:
        fields = {str(k): str(v) for k, v in dict(fields_blob).items()}
        entries.append((str(entry_id_blob), fields))
    return entries


async def queue_depth(redis: Redis, queue: str) -> int:
    """Sum XLEN across the queue's priority streams (approximate under load)."""
    pipe = redis.pipeline()
    for stream in priority_streams(queue):
        pipe.xlen(stream)
    lengths: Any = await pipe.execute()
    return sum(int(n) for n in lengths)


async def dlq_xadd(redis: Redis, fields: Mapping[str, str], maxlen: int = 10000) -> str:
    """Append to the DLQ stream, bounded with MAXLEN ~maxlen."""
    entry_id: Any = await redis.xadd(
        DLQ_STREAM, _field_map(fields), maxlen=maxlen, approximate=True
    )
    return str(entry_id)


async def dlq_range(redis: Redis, count: int = 50) -> list[tuple[str, dict[str, str]]]:
    """Return up to ``count`` DLQ entries as (entry_id, fields), newest first."""
    raw: Any = await redis.xrevrange(DLQ_STREAM, max="+", min="-", count=count)
    out: list[tuple[str, dict[str, str]]] = []
    for entry_id_blob, fields_blob in raw or []:
        fields = {str(k): str(v) for k, v in dict(fields_blob).items()}
        out.append((str(entry_id_blob), fields))
    return out


async def dlq_xdel(redis: Redis, *entry_ids: str) -> int:
    """Remove entries from the DLQ stream; returns the number removed."""
    if not entry_ids:
        return 0
    removed: Any = await redis.xdel(DLQ_STREAM, *entry_ids)
    return int(removed)


#: KEYS[1] = stream, ARGV = [group, entry_id]. ACKs the entry for the
#: consumer group and deletes it, atomically. XACK alone does not remove
#: entries from a stream; without the XDEL, processed entries would
#: accumulate forever and inflate XLEN-based depth accounting.
_ACKDEL_LUA = """
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
return redis.call('XDEL', KEYS[1], ARGV[2])
"""


async def xackdel(redis: Redis, stream: str, group: str, entry_id: str) -> int:
    """ACK an entry and delete it from the stream, atomically.

    Returns 1 when the entry was deleted, 0 when it was already gone.
    """
    # redis-py 6.x types eval as Union[Awaitable, T]; bind to Any first.
    call: Any = redis.eval(_ACKDEL_LUA, 1, stream, group, entry_id)
    removed: Any = await call
    return int(removed)


async def trim_stream(redis: Redis, stream: str, maxlen: int) -> int:
    """Trim a stream to ~maxlen entries; returns the number removed."""
    removed: Any = await redis.xtrim(stream, maxlen=maxlen, approximate=True)
    return int(removed)
