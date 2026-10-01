"""DTQ worker: claim stream entries and execute registered handlers."""

from .executor import (
    OUTCOME_ABSORBED,
    OUTCOME_CANCELLED,
    OUTCOME_DEAD_LETTERED,
    OUTCOME_EXPIRED,
    OUTCOME_FAILED,
    OUTCOME_RETRY_SCHEDULED,
    OUTCOME_SKIPPED,
    OUTCOME_SUCCEEDED,
    ExecContext,
    StreamEntry,
    execute_entry,
)
from .main import run_worker
from .registry import (
    Handler,
    PermanentError,
    RetryableError,
    TaskContext,
    get_handler,
    registered_names,
    task,
)
from .worker import Worker

__all__ = [
    "ExecContext",
    "Handler",
    "OUTCOME_ABSORBED",
    "OUTCOME_CANCELLED",
    "OUTCOME_DEAD_LETTERED",
    "OUTCOME_EXPIRED",
    "OUTCOME_FAILED",
    "OUTCOME_RETRY_SCHEDULED",
    "OUTCOME_SKIPPED",
    "OUTCOME_SUCCEEDED",
    "PermanentError",
    "RetryableError",
    "StreamEntry",
    "TaskContext",
    "Worker",
    "execute_entry",
    "get_handler",
    "registered_names",
    "run_worker",
    "task",
]
