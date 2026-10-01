"""sleep_task: async sleep demo (cancellable, used for timeout tests)."""

from __future__ import annotations

import asyncio
from typing import Any

from dtq_worker.registry import PermanentError, TaskContext, task

#: Hard cap so a demo task can never sleep unboundedly.
MAX_SECONDS = 30.0


@task("sleep_task")
async def sleep_task(payload: dict[str, Any], ctx: TaskContext) -> dict[str, Any]:
    """Sleep ``payload["seconds"]`` seconds, clamped to 30.

    Async, so the worker can cancel it promptly on timeout or cancel
    requests.
    """
    try:
        seconds = float(payload.get("seconds", 1.0))
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
    print(asyncio.run(sleep_task({"seconds": 0.1}, ctx)))
