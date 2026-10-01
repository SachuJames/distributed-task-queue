"""slow_task: long async sleep (timeout and crash-recovery demo).

Unlike sleep_task (capped at 30s), this one allows up to 120s so tests can
kill a worker mid-execution and watch a peer reclaim the entry.
"""

from __future__ import annotations

import asyncio
from typing import Any

from dtq_worker.registry import PermanentError, TaskContext, task

#: Hard cap so a demo task can never sleep unboundedly.
MAX_SECONDS = 120.0


@task("slow_task")
async def slow_task(payload: dict[str, Any], ctx: TaskContext) -> dict[str, Any]:
    """Sleep ``payload["seconds"]`` seconds (default 30, max 120)."""
    try:
        seconds = float(payload.get("seconds", 30.0))
    except (TypeError, ValueError) as exc:
        raise PermanentError(f"seconds must be a number: {exc}") from exc
    seconds = max(0.0, min(seconds, MAX_SECONDS))
    await asyncio.sleep(seconds)
    return {"slept_seconds": seconds, "attempt": ctx.attempt}


if __name__ == "__main__":
    ctx = TaskContext(
        task_id="demo",
        idempotency_key=None,
        attempt=1,
        worker_id="demo-worker",
        queue="default",
        deadline=0.0,
    )
    print(asyncio.run(slow_task({"seconds": 0.1}, ctx)))
