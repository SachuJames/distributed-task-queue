"""dtq_queue: Redis Streams ingest, claim, and task-state primitives."""

from dtq_queue.ingest import submit_task
from dtq_queue.lua import claim_due as lua_claim_due
from dtq_queue.lua import requeue_stale as lua_requeue_stale
from dtq_queue.streams import (
    queue_depth,
    stream_entry_fields,
    trim_stream,
    xack,
    xackdel,
    xadd_task,
    xautoclaim,
    xpending_count,
    xreadgroup_priority,
)
from dtq_queue.task_state import (
    InvalidTransitionForRedelivery,
    from_hash,
    get_task,
    record_attempt_end,
    record_attempt_start,
    record_redelivery,
    save_task,
    to_hash,
    update_task,
)

__all__ = [
    "InvalidTransitionForRedelivery",
    "from_hash",
    "get_task",
    "lua_claim_due",
    "lua_requeue_stale",
    "queue_depth",
    "record_attempt_end",
    "record_attempt_start",
    "record_redelivery",
    "save_task",
    "stream_entry_fields",
    "submit_task",
    "to_hash",
    "trim_stream",
    "update_task",
    "xack",
    "xackdel",
    "xadd_task",
    "xautoclaim",
    "xpending_count",
    "xreadgroup_priority",
]
