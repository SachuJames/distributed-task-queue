"""echo_task: return the payload back (synthetic connectivity check)."""

from __future__ import annotations

from typing import Any

from dtq_worker.registry import TaskContext, task


@task("echo_task")
def echo_task(payload: dict[str, Any], ctx: TaskContext) -> dict[str, Any]:
    """Echo the payload with the attempt number attached."""
    return {"echo": payload, "attempt": ctx.attempt}


if __name__ == "__main__":
    ctx = TaskContext(
        task_id="demo",
        idempotency_key=None,
        attempt=1,
        worker_id="demo-worker",
        queue="default",
        deadline=0.0,
    )
    print(echo_task({"hello": "world"}, ctx))
