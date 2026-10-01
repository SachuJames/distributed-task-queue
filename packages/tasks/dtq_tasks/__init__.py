"""dtq_tasks: the handler registry. Workers execute ONLY @task handlers."""

from dtq_tasks.context import TaskContext
from dtq_tasks.errors import PermanentError, RetryableError
from dtq_tasks.helpers import idempotent_operation
from dtq_tasks.registry import TASK_REGISTRY, Handler, get_handler, task

__all__ = [
    "TASK_REGISTRY",
    "Handler",
    "PermanentError",
    "RetryableError",
    "TaskContext",
    "get_handler",
    "idempotent_operation",
    "task",
]
