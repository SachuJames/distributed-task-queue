"""counter_task: atomic Redis INCR demo (synthetic).

Increments ``dtq:example:counter:<name>`` and returns the new count.
Deliberately NOT idempotent: a redelivered execution increments again,
which is exactly what at-least-once delivery means for side effects.
"""

from __future__ import annotations

from typing import Any

from dtq_worker.registry import TaskContext, task

from ._redis import sync_redis


@task("counter_task")
def counter_task(payload: dict[str, Any], ctx: TaskContext) -> dict[str, Any]:
    """Atomically increment a synthetic counter key; return the count."""
    name = str(payload.get("counter", "default"))
    key = f"dtq:example:counter:{name}"
    raw_count: Any = sync_redis().incr(key)
    count = int(raw_count)
    return {"counter": key, "count": count, "attempt": ctx.attempt}


if __name__ == "__main__":
    ctx = TaskContext(
        task_id="demo",
        idempotency_key=None,
        attempt=1,
        worker_id="demo-worker",
        queue="default",
        deadline=0.0,
    )
    print(counter_task({"counter": "demo"}, ctx))
