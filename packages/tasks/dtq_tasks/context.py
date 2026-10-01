"""TaskContext passed to every handler (contract section 15)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TaskContext:
    """Execution context for one handler invocation."""

    task_id: str
    idempotency_key: str | None
    attempt: int
    worker_id: str
    queue: str
    deadline: float  # monotonic seconds (time.monotonic()); finish before this
