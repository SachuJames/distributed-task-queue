"""Handler registry (contract section 15).

Thin layer over the shared :mod:`dtq_tasks` registry, kept so worker code
and example handlers have one import location.

Security: handlers run only when registered here via :func:`task`. The
executor never imports, evals, or execs anything derived from a task payload
or a ``task_type`` string; an unregistered ``task_type`` takes the permanent
failure path and is never executed.
"""

from __future__ import annotations

from dtq_tasks import (
    TASK_REGISTRY,
    Handler,
    PermanentError,
    RetryableError,
    TaskContext,
    get_handler,
    idempotent_operation,
    task,
)


def registered_names() -> list[str]:
    """Return all registered task type names, sorted."""
    return sorted(TASK_REGISTRY)


__all__ = [
    "Handler",
    "PermanentError",
    "RetryableError",
    "TASK_REGISTRY",
    "TaskContext",
    "get_handler",
    "idempotent_operation",
    "registered_names",
    "task",
]
