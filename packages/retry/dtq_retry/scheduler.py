"""Retry/delayed-task scheduler primitives (contract section 4).

The scheduler claims due members of ``dtq:retry:schedule`` with an atomic Lua
script (see dtq_queue.lua for why atomicity matters), XADDs each claimed task
to its priority stream, and removes it from the claimed set. A reaper returns
claims older than 60s to the schedule so a crashed scheduler never strands a
task.
"""

from __future__ import annotations

import time
from collections.abc import Mapping

from dtq_core.keys import RETRY_CLAIMED, RETRY_SCHEDULE
from dtq_core.models import TaskStatus
from dtq_core.models import transition as transition_status
from dtq_queue import lua as lua_scripts
from dtq_queue.streams import stream_entry_fields, xadd_task
from dtq_queue.task_state import get_task, save_task
from redis.asyncio import Redis

#: Scheduler batch cap (contract section 7: retry pressure).
SCHEDULER_BATCH_CAP = 100

#: Claims older than this are considered stale and returned to the schedule.
STALE_CLAIM_AFTER_S = 60.0


def _now_ms() -> int:
    return int(time.time() * 1000)


async def schedule_retry(redis: Redis, task_id: str, delay_s: float) -> None:
    """Put a task in the retry schedule, due ``delay_s`` seconds from now."""
    due_ms = _now_ms() + int(delay_s * 1000)
    await redis.zadd(RETRY_SCHEDULE, {task_id: due_ms})


async def claim_due(redis: Redis, limit: int = SCHEDULER_BATCH_CAP) -> list[str]:
    """Atomically move due task ids schedule -> claimed (batch capped at 100)."""
    return await lua_scripts.claim_due(redis, _now_ms(), min(limit, SCHEDULER_BATCH_CAP))


async def requeue_claimed(
    redis: Redis,
    task_ids: list[str],
    streams: Mapping[str, str],
    retention_s: int = 604800,
) -> list[str]:
    """XADD each claimed task to its priority stream and ZREM it from claimed.

    ``streams`` maps task_id -> priority stream name. Each task moves
    RETRYING -> QUEUED and its attempt increments to the upcoming attempt
    number. Tasks whose hash is gone (or with no stream mapping) are dropped
    from the claimed set so they cannot strand the scheduler.
    """
    requeued: list[str] = []
    for task_id in task_ids:
        stream = streams.get(task_id)
        task = await get_task(redis, task_id)
        if task is None or stream is None:
            await redis.zrem(RETRY_CLAIMED, task_id)
            continue
        task.attempt += 1
        transition_status(task, TaskStatus.QUEUED)
        await save_task(redis, task, retention_s)
        await xadd_task(redis, stream, stream_entry_fields(task))
        await redis.zrem(RETRY_CLAIMED, task_id)
        requeued.append(task_id)
    return requeued


async def requeue_stale_claims(
    redis: Redis,
    stale_after_s: float = STALE_CLAIM_AFTER_S,
    limit: int = 1000,
) -> list[str]:
    """Return claims older than ``stale_after_s`` to the schedule, due now."""
    now_ms = _now_ms()
    cutoff_ms = now_ms - int(stale_after_s * 1000)
    return await lua_scripts.requeue_stale(redis, cutoff_ms, now_ms, limit)
