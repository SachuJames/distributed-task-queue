"""Handler registry (contract section 15).

Workers execute ONLY handlers registered here via @task("name"). Payloads are
never evaluated, imported, or shelled out: an unknown task_type takes the
permanent failure path instead of executing anything.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dtq_tasks.context import TaskContext

#: fn(payload: dict, ctx: TaskContext) -> JSON-serializable. May be sync or async.
Handler = Callable[[dict[str, Any], TaskContext], Any]

TASK_REGISTRY: dict[str, Handler] = {}


def task(name: str) -> Callable[[Handler], Handler]:
    """Register ``fn`` as the handler for ``name``. Returns ``fn`` unchanged."""

    def decorator(fn: Handler) -> Handler:
        if name in TASK_REGISTRY:
            raise ValueError(f"task {name!r} is already registered")
        TASK_REGISTRY[name] = fn
        return fn

    return decorator


def get_handler(name: str) -> Handler | None:
    """Return the handler for ``name``, or None when it is not registered."""
    return TASK_REGISTRY.get(name)
