"""Unit tests for the domain model and status transitions."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from dtq_core.models import (
    VALID_TRANSITIONS,
    InvalidTransition,
    Task,
    TaskStatus,
    is_terminal,
    transition,
)
from pydantic import ValidationError


def make_task(status: TaskStatus = TaskStatus.PENDING, **overrides: Any) -> Task:
    now = datetime.now(UTC)
    fields: dict[str, Any] = {
        "task_id": "9f3a" * 8,
        "task_type": "echo_task",
        "payload": {"message": "hello"},
        "created_at": now,
        "available_at": now,
        "status": status,
    }
    fields.update(overrides)
    return Task(**fields)


VALID_CASES = [
    (TaskStatus.PENDING, TaskStatus.QUEUED),
    (TaskStatus.PENDING, TaskStatus.CANCELLED),
    (TaskStatus.PENDING, TaskStatus.EXPIRED),
    (TaskStatus.QUEUED, TaskStatus.RUNNING),
    (TaskStatus.QUEUED, TaskStatus.CANCELLED),
    (TaskStatus.QUEUED, TaskStatus.EXPIRED),
    (TaskStatus.RUNNING, TaskStatus.SUCCEEDED),
    (TaskStatus.RUNNING, TaskStatus.RETRYING),
    (TaskStatus.RUNNING, TaskStatus.FAILED),
    (TaskStatus.RUNNING, TaskStatus.DEAD_LETTERED),
    (TaskStatus.RUNNING, TaskStatus.CANCELLED),
    (TaskStatus.RETRYING, TaskStatus.QUEUED),
    (TaskStatus.RETRYING, TaskStatus.CANCELLED),
]


@pytest.mark.parametrize(("from_status", "to_status"), VALID_CASES)
def test_valid_transitions(from_status: TaskStatus, to_status: TaskStatus) -> None:
    task = make_task(from_status)
    result = transition(task, to_status)
    assert result is task
    assert task.status is to_status


@pytest.mark.parametrize(
    ("from_status", "to_status"),
    [
        # skipping states
        (TaskStatus.PENDING, TaskStatus.RUNNING),
        (TaskStatus.PENDING, TaskStatus.SUCCEEDED),
        (TaskStatus.QUEUED, TaskStatus.SUCCEEDED),
        (TaskStatus.QUEUED, TaskStatus.RETRYING),
        (TaskStatus.RUNNING, TaskStatus.QUEUED),
        (TaskStatus.RUNNING, TaskStatus.PENDING),
        (TaskStatus.RETRYING, TaskStatus.RUNNING),
        (TaskStatus.RETRYING, TaskStatus.SUCCEEDED),
        (TaskStatus.RETRYING, TaskStatus.EXPIRED),
        # terminal states have no outgoing transitions
        (TaskStatus.SUCCEEDED, TaskStatus.QUEUED),
        (TaskStatus.SUCCEEDED, TaskStatus.CANCELLED),
        (TaskStatus.FAILED, TaskStatus.RETRYING),
        (TaskStatus.DEAD_LETTERED, TaskStatus.QUEUED),
        (TaskStatus.CANCELLED, TaskStatus.QUEUED),
        (TaskStatus.EXPIRED, TaskStatus.QUEUED),
        # self transitions are illegal
        (TaskStatus.QUEUED, TaskStatus.QUEUED),
        (TaskStatus.RUNNING, TaskStatus.RUNNING),
    ],
)
def test_invalid_transitions_raise(from_status: TaskStatus, to_status: TaskStatus) -> None:
    task = make_task(from_status)
    with pytest.raises(InvalidTransition):
        transition(task, to_status)
    assert task.status is from_status


def test_valid_transitions_map_covers_every_status() -> None:
    assert set(VALID_TRANSITIONS) == set(TaskStatus)


def test_any_non_terminal_can_cancel() -> None:
    for status in TaskStatus:
        if not is_terminal(status):
            assert TaskStatus.CANCELLED in VALID_TRANSITIONS[status]


def test_terminal_states_have_no_outgoing() -> None:
    for status in (
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.DEAD_LETTERED,
        TaskStatus.CANCELLED,
        TaskStatus.EXPIRED,
    ):
        assert is_terminal(status)
        assert VALID_TRANSITIONS[status] == frozenset()


def test_is_terminal_false_for_live_states() -> None:
    for status in (
        TaskStatus.PENDING,
        TaskStatus.QUEUED,
        TaskStatus.RUNNING,
        TaskStatus.RETRYING,
    ):
        assert not is_terminal(status)


@pytest.mark.parametrize("queue", ["default", "q1", "my-queue_2", "a", "x" * 64])
def test_valid_queue_names(queue: str) -> None:
    assert make_task(queue=queue).queue == queue


@pytest.mark.parametrize("queue", ["", "Upper", "-lead", "_lead", "has space", "q!" * 40, "x" * 65])
def test_invalid_queue_names_rejected(queue: str) -> None:
    with pytest.raises(ValidationError):
        make_task(queue=queue)


def test_naive_datetimes_rejected() -> None:
    with pytest.raises(ValidationError):
        make_task(created_at=datetime.now())


def test_priority_bounds_enforced() -> None:
    with pytest.raises(ValidationError):
        make_task(priority=10)
    with pytest.raises(ValidationError):
        make_task(priority=-1)
    assert make_task(priority=0).priority == 0
    assert make_task(priority=9).priority == 9


def test_timeout_ms_upper_bound() -> None:
    with pytest.raises(ValidationError):
        make_task(timeout_ms=3_600_001)


def test_all_contract_fields_present() -> None:
    task = make_task()
    data = task.model_dump()
    for field in (
        "task_id",
        "idempotency_key",
        "task_type",
        "payload",
        "created_at",
        "available_at",
        "attempt",
        "max_attempts",
        "priority",
        "timeout_ms",
        "status",
        "queue",
        "metadata",
        "started_at",
        "finished_at",
        "duration_ms",
        "worker_id",
        "error_class",
        "error_message",
        "retryable",
        "retry_delay_ms",
    ):
        assert field in data
