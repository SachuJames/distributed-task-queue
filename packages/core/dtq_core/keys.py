"""Exact Redis key names for the dtq schema (contract section 2).

Every key the engine touches is built here, so renames stay in one place.
"""

from __future__ import annotations

#: Number of strict-priority bands per queue (p0 lowest, p9 highest).
PRIORITY_BANDS = 10

#: Consumer group name on every priority stream.
CONSUMER_GROUP = "workers"

#: DLQ stream. Bounded via MAXLEN on XADD.
DLQ_STREAM = "dtq:stream:dead-letter"

#: Sorted set of retries and delayed tasks: member = task_id, score = due epoch ms.
RETRY_SCHEDULE = "dtq:retry:schedule"

#: Sorted set of claimed tasks: member = task_id, score = claim epoch ms.
RETRY_CLAIMED = "dtq:retry:claimed"

#: Set of known worker ids (best effort; liveness comes from heartbeat age).
WORKERS_SET = "dtq:workers"

#: Set of task ids cancelled while queued.
CANCELLED_SET = "dtq:cancelled"

#: Zset of task_id by created epoch ms, trimmed to 10000 entries.
RECENT_ZSET = "dtq:tasks:recent"

#: Scheduler leader lock (SET NX PX 10000).
SCHEDULER_LOCK = "dtq:lock:scheduler"

#: Stream of admin actions, MAXLEN bounded.
AUDIT_STREAM = "dtq:stream:audit"

#: Pub/sub channel for lifecycle events fanned to WebSocket clients.
EVENTS_CHANNEL = "dtq:events"

#: Cap on entries kept in the recent-tasks zset.
RECENT_ZSET_MAXLEN = 10000


def stream_name(queue: str, priority: int) -> str:
    """Return the stream key for a queue's priority band."""
    if not 0 <= priority < PRIORITY_BANDS:
        raise ValueError(f"priority must be 0..{PRIORITY_BANDS - 1}, got {priority}")
    return f"dtq:stream:{queue}:p{priority}"


def priority_streams(queue: str) -> list[str]:
    """Return all priority stream keys for a queue, p0 first."""
    return [stream_name(queue, p) for p in range(PRIORITY_BANDS)]


def task_key(task_id: str) -> str:
    """Return the hash key holding a task's fields and execution metadata."""
    return f"dtq:task:{task_id}"


def idem_key(queue: str, key: str) -> str:
    """Return the idempotency record key for a (queue, key) scope."""
    return f"dtq:idempotency:{queue}:{key}"


def worker_key(worker_id: str) -> str:
    """Return the hash key holding a worker's heartbeat record."""
    return f"dtq:worker:{worker_id}"


def pause_key(queue: str) -> str:
    """Return the key whose presence ("1") marks a queue paused."""
    return f"dtq:queue:{queue}:paused"
