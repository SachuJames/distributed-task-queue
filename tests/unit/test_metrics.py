"""Unit tests for metrics construction and label discipline."""

from __future__ import annotations

import pytest
from dtq_metrics.metrics import DtqMetrics, build_metrics
from prometheus_client import CollectorRegistry


def test_build_metrics_twice_is_safe() -> None:
    m1 = build_metrics(CollectorRegistry())
    m2 = build_metrics(CollectorRegistry())
    assert isinstance(m1, DtqMetrics)
    assert isinstance(m2, DtqMetrics)
    m1.tasks_submitted_total.labels(queue="q", task_type="t").inc()
    assert m2.tasks_submitted_total.labels(queue="q", task_type="t")._value.get() == 0


def test_all_contract_metrics_present() -> None:
    m = build_metrics(CollectorRegistry())
    for name in (
        "tasks_submitted_total",
        "tasks_started_total",
        "tasks_succeeded_total",
        "tasks_failed_total",
        "tasks_retried_total",
        "tasks_dead_lettered_total",
        "tasks_completed_total",
        "task_attempts_total",
        "queue_depth",
        "queue_pending",
        "retry_scheduled",
        "dlq_depth",
        "workers_active",
        "worker_inflight",
        "worker_tasks_total",
        "task_duration_seconds",
        "task_execution_seconds",
        "task_wait_seconds",
        "retry_delay_seconds",
        "redis_operations_total",
        "ingest_rejected_total",
    ):
        assert hasattr(m, name), name


def test_metric_names_match_contract() -> None:
    m = build_metrics(CollectorRegistry())
    # Counter._name drops the _total suffix; exposition restores it.
    assert m.tasks_submitted_total._name == "dtq_tasks_submitted"
    m.tasks_submitted_total.labels(queue="q", task_type="t").inc()
    samples = {s.name for metric in m.tasks_submitted_total.collect() for s in metric.samples}
    assert "dtq_tasks_submitted_total" in samples
    assert m.queue_depth._name == "dtq_queue_depth"
    assert m.retry_scheduled._name == "dtq_retry_scheduled"
    assert m.task_duration_seconds._name == "dtq_task_duration_seconds"
    assert m.redis_operations_total._name == "dtq_redis_operations"


def test_label_sets_match_contract() -> None:
    m = build_metrics(CollectorRegistry())
    assert list(m.tasks_submitted_total._labelnames) == ["queue", "task_type"]
    assert list(m.tasks_completed_total._labelnames) == ["queue", "task_type", "status"]
    assert list(m.task_attempts_total._labelnames) == ["queue", "task_type", "result"]
    assert list(m.worker_inflight._labelnames) == ["worker_id"]
    assert list(m.worker_tasks_total._labelnames) == ["worker_id", "task_type", "result"]
    assert list(m.retry_scheduled._labelnames) == []
    assert list(m.ingest_rejected_total._labelnames) == ["queue", "reason"]


def test_duplicate_registration_raises_without_fresh_registry() -> None:
    reg = CollectorRegistry()
    build_metrics(reg)
    with pytest.raises(ValueError):
        build_metrics(reg)
