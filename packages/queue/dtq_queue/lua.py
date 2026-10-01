"""Lua scripts for atomic retry-schedule operations.

Why atomicity is required
--------------------------
The scheduler loop and the claim reaper run concurrently, and during a leader
failover two scheduler instances may briefly overlap. Each operation below is
a read-then-move across two sorted sets:

* ``CLAIM_DUE_RETRIES``: ZRANGEBYSCORE the due tasks, then move each
  ``dtq:retry:schedule`` -> ``dtq:retry:claimed``.
* ``REQUEUE_STALE_CLAIMS``: ZRANGEBYSCORE claims older than 60s, then move
  each ``dtq:retry:claimed`` -> ``dtq:retry:schedule``.

If the read and the move were separate round trips, two schedulers could read
the same due task and both XADD it into a priority stream, producing a
duplicate execution; or the reaper could read a claim and move it back while
the scheduler is XADDing it, losing the claim or double-enqueueing the task.
Running the whole read-and-move inside one Lua script makes it indivisible:
Redis executes scripts atomically with respect to other clients, so a task
can only ever be claimed once per pass.
"""

from __future__ import annotations

from typing import Any

from dtq_core.keys import RETRY_CLAIMED, RETRY_SCHEDULE
from redis.asyncio import Redis

#: Move due members schedule -> claimed.
#: KEYS[1] = dtq:retry:schedule, KEYS[2] = dtq:retry:claimed.
#: ARGV[1] = now_ms (upper score bound), ARGV[2] = limit,
#: ARGV[3] = claim timestamp ms (score written to the claimed set).
CLAIM_DUE_RETRIES = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '0', ARGV[1], 'LIMIT', '0', ARGV[2])
for i, member in ipairs(due) do
    redis.call('ZADD', KEYS[2], ARGV[3], member)
    redis.call('ZREM', KEYS[1], member)
end
return due
"""

#: Move stale claims claimed -> schedule.
#: KEYS[1] = dtq:retry:claimed, KEYS[2] = dtq:retry:schedule.
#: ARGV[1] = cutoff_ms (claims with score <= cutoff are stale),
#: ARGV[2] = limit, ARGV[3] = now_ms (new due score, so the task is retried
#: promptly instead of waiting on its original due time).
REQUEUE_STALE_CLAIMS = """
local stale = redis.call('ZRANGEBYSCORE', KEYS[1], '0', ARGV[1], 'LIMIT', '0', ARGV[2])
for i, member in ipairs(stale) do
    redis.call('ZADD', KEYS[2], ARGV[3], member)
    redis.call('ZREM', KEYS[1], member)
end
return stale
"""


def _members(raw: Any) -> list[str]:
    if not raw:
        return []
    return [str(m) for m in raw]


async def _eval_script(redis: Redis, script: str, keys: list[str], args: list[int]) -> Any:
    """Run a Lua script; redis-py 6.x types eval as Union[Awaitable, T]."""
    call: Any = redis.eval(script, len(keys), *keys, *[str(a) for a in args])
    return await call


async def claim_due(redis: Redis, now_ms: int, limit: int) -> list[str]:
    """Atomically move up to ``limit`` due task ids schedule -> claimed."""
    raw = await _eval_script(
        redis, CLAIM_DUE_RETRIES, [RETRY_SCHEDULE, RETRY_CLAIMED], [now_ms, limit, now_ms]
    )
    return _members(raw)


async def requeue_stale(redis: Redis, cutoff_ms: int, now_ms: int, limit: int) -> list[str]:
    """Atomically move claims older than ``cutoff_ms`` back to the schedule."""
    raw = await _eval_script(
        redis,
        REQUEUE_STALE_CLAIMS,
        [RETRY_CLAIMED, RETRY_SCHEDULE],
        [cutoff_ms, limit, now_ms],
    )
    return _members(raw)
