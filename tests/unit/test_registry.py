"""Unit tests for the handler registry."""

from __future__ import annotations

from typing import Any

import pytest
from dtq_tasks.context import TaskContext
from dtq_tasks.registry import TASK_REGISTRY, get_handler, task


def _ctx() -> TaskContext:
    import time

    return TaskContext(
        task_id="t1",
        idempotency_key=None,
        attempt=1,
        worker_id="worker-abc",
        queue="default",
        deadline=time.monotonic() + 60,
    )


def test_decorator_registers_and_returns_fn() -> None:
    async def my_handler(payload: dict[str, Any], ctx: TaskContext) -> dict[str, Any]:
        return {"echo": payload}

    registered = task("test_echo_unique")(my_handler)
    assert registered is my_handler
    assert get_handler("test_echo_unique") is my_handler
    assert TASK_REGISTRY["test_echo_unique"] is my_handler


def test_sync_handler_supported() -> None:
    def sync_handler(payload: dict[str, Any], ctx: TaskContext) -> str:
        return "ok"

    task("test_sync_unique")(sync_handler)
    handler = get_handler("test_sync_unique")
    assert handler is not None
    assert handler({"a": 1}, _ctx()) == "ok"


def test_unknown_name_returns_none() -> None:
    assert get_handler("no_such_task_type_xyz") is None


def test_duplicate_registration_rejected() -> None:
    def h1(payload: dict[str, Any], ctx: TaskContext) -> None:
        return None

    def h2(payload: dict[str, Any], ctx: TaskContext) -> None:
        return None

    task("test_dup_unique")(h1)
    with pytest.raises(ValueError, match="already registered"):
        task("test_dup_unique")(h2)
    assert get_handler("test_dup_unique") is h1


def test_decorator_syntax() -> None:
    @task("test_deco_syntax_unique")
    def fib(payload: dict[str, Any], ctx: TaskContext) -> int:
        return int(payload["n"])

    assert get_handler("test_deco_syntax_unique") is fib
