"""Synthetic demo task handlers (contract section 14).

Importing this package registers every task type with the worker registry.
No shell, no network, no real data.
"""

from . import (
    always_fail_task,
    counter_task,
    echo_task,
    fibonacci_task,
    flaky_task,
    sleep_task,
    slow_task,
)

__all__ = [
    "always_fail_task",
    "counter_task",
    "echo_task",
    "fibonacci_task",
    "flaky_task",
    "sleep_task",
    "slow_task",
]
