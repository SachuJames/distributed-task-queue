"""always_fail_task: always raise RetryableError (DLQ demo).

With max_attempts=N it fails N times, then the engine moves it to the
dead-letter stream (or FAILED when the DLQ is disabled).
"""

from __future__ import annotations

from typing import Any

from dtq_worker.registry import RetryableError, TaskContext, task


@task("always_fail_task")
def always_fail_task(payload: dict[str, Any], ctx: TaskContext) -> dict[str, Any]:
    """Never succeeds; used to exercise retries-to-exhaustion and the DLQ."""
    raise RetryableError(
        f"synthetic always-fail demo (attempt {ctx.attempt}): this task never succeeds"
    )


if __name__ == "__main__":
    ctx = TaskContext(
        task_id="demo",
        idempotency_key=None,
        attempt=1,
        worker_id="demo-worker",
        queue="default",
        deadline=0.0,
    )
    try:
        always_fail_task({}, ctx)
    except RetryableError as exc:
        print(f"raised as expected: {exc}")
