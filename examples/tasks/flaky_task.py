"""flaky_task: fail ``fail_times`` times, then succeed (retry demo).

The remaining failure budget comes from the payload; the attempt number
comes from the task context, so no extra state is needed.
"""

from __future__ import annotations

from typing import Any

from dtq_worker.registry import PermanentError, RetryableError, TaskContext, task


@task("flaky_task")
def flaky_task(payload: dict[str, Any], ctx: TaskContext) -> dict[str, Any]:
    """Raise RetryableError while ``ctx.attempt <= fail_times``."""
    try:
        fail_times = int(payload.get("fail_times", 2))
    except (TypeError, ValueError) as exc:
        raise PermanentError(f"fail_times must be an integer: {exc}") from exc
    if fail_times < 0:
        raise PermanentError("fail_times must be >= 0")
    if ctx.attempt <= fail_times:
        raise RetryableError(
            f"synthetic flaky failure {ctx.attempt}/{fail_times}: transient dependency unavailable"
        )
    return {"succeeded_on_attempt": ctx.attempt, "fail_times": fail_times}


if __name__ == "__main__":
    ctx = TaskContext(
        task_id="demo",
        idempotency_key=None,
        attempt=3,
        worker_id="demo-worker",
        queue="default",
        deadline=0.0,
    )
    print(flaky_task({"fail_times": 2}, ctx))
