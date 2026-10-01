"""Unit tests for event schemas."""

from __future__ import annotations

from datetime import datetime

from dtq_events.schemas import EventType, make_event


def test_all_contract_event_types_present() -> None:
    assert {e.value for e in EventType} == {
        "TASK_QUEUED",
        "TASK_STARTED",
        "TASK_SUCCEEDED",
        "TASK_FAILED",
        "TASK_RETRY_SCHEDULED",
        "TASK_RETRIED",
        "TASK_DEAD_LETTERED",
        "TASK_CANCELLED",
        "WORKER_STARTED",
        "WORKER_STOPPED",
        "WORKER_STALE",
        "CONFIGURATION_CHANGED",
        # Worker-side extensions (documented in dtq_events.schemas).
        "TASK_EXPIRED",
        "TASK_DUPLICATE_ABSORBED",
    }


def test_make_event_shape() -> None:
    event = make_event(
        EventType.TASK_QUEUED,
        task_id="t1",
        queue="default",
        task_type="echo_task",
        attempt=1,
        worker_id="worker-x",
        metadata={"note": "hi"},
    )
    assert event["type"] == "TASK_QUEUED"
    assert event["task_id"] == "t1"
    assert event["queue"] == "default"
    assert event["task_type"] == "echo_task"
    assert event["attempt"] == 1
    assert event["worker_id"] == "worker-x"
    assert event["metadata"] == {"note": "hi"}
    assert len(event["event_id"]) == 32
    # ISO UTC timestamp, parseable
    datetime.fromisoformat(event["ts"])


def test_make_event_omits_unset_fields() -> None:
    event = make_event(EventType.WORKER_STARTED, worker_id="w1")
    assert set(event) == {"event_id", "ts", "type", "worker_id"}


def test_event_ids_unique() -> None:
    ids = {make_event(EventType.TASK_QUEUED)["event_id"] for _ in range(100)}
    assert len(ids) == 100
