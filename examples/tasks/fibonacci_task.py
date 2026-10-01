"""fibonacci_task: CPU-bound demo (documents the GIL).

This handler is synchronous, so the worker runs it in a thread via
``asyncio.to_thread``. It is GIL-bound while it computes: it occupies one
worker thread but does not block the event loop, so other tasks keep
flowing. True parallelism for CPU-bound work scales with the number of
worker processes, not the concurrency setting.
"""

from __future__ import annotations

from typing import Any

from dtq_worker.registry import PermanentError, TaskContext, task

#: Upper bound for n: keeps the demo fast and the result JSON small.
MAX_N = 100_000


@task("fibonacci_task")
def fibonacci_task(payload: dict[str, Any], ctx: TaskContext) -> dict[str, Any]:
    """Compute fib(n) iteratively. ``payload["n"]`` must be 0..100000."""
    n = payload.get("n", 10)
    if isinstance(n, bool) or not isinstance(n, int):
        raise PermanentError("n must be an integer")
    if not 0 <= n <= MAX_N:
        raise PermanentError(f"n must be between 0 and {MAX_N}")
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return {"n": n, "fib": a, "attempt": ctx.attempt}


if __name__ == "__main__":
    ctx = TaskContext(
        task_id="demo",
        idempotency_key=None,
        attempt=1,
        worker_id="demo-worker",
        queue="default",
        deadline=0.0,
    )
    print(fibonacci_task({"n": 20}, ctx))
