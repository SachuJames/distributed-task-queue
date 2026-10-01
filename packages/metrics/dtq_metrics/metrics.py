"""Prometheus metrics (contract section 9).

Labels are limited to task_type, queue, status (plus worker_id only where the
contract allows it). Never task_id, user id, request id, exception text, or
payload.
"""

from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


@dataclass
class DtqMetrics:
    """Every metric from contract section 9, built against one registry."""

    tasks_submitted_total: Counter
    tasks_started_total: Counter
    tasks_succeeded_total: Counter
    tasks_failed_total: Counter
    tasks_retried_total: Counter
    tasks_dead_lettered_total: Counter
    tasks_completed_total: Counter
    task_attempts_total: Counter
    queue_depth: Gauge
    queue_pending: Gauge
    retry_scheduled: Gauge
    dlq_depth: Gauge
    workers_active: Gauge
    worker_inflight: Gauge
    worker_tasks_total: Gauge
    task_duration_seconds: Histogram
    task_execution_seconds: Histogram
    task_wait_seconds: Histogram
    retry_delay_seconds: Histogram
    redis_operations_total: Counter
    ingest_rejected_total: Counter


def build_metrics(registry: CollectorRegistry | None = None) -> DtqMetrics:
    """Build all metrics. Pass a CollectorRegistry to stay test-safe.

    Calling this twice against the default registry raises a duplicate
    registration error, so tests must pass their own CollectorRegistry().
    """
    reg = registry if registry is not None else CollectorRegistry()
    return DtqMetrics(
        tasks_submitted_total=Counter(
            "dtq_tasks_submitted_total",
            "Tasks accepted by the API",
            ["queue", "task_type"],
            registry=reg,
        ),
        tasks_started_total=Counter(
            "dtq_tasks_started_total",
            "Task executions started by workers",
            ["queue", "task_type"],
            registry=reg,
        ),
        tasks_succeeded_total=Counter(
            "dtq_tasks_succeeded_total",
            "Task executions that returned a result",
            ["queue", "task_type"],
            registry=reg,
        ),
        tasks_failed_total=Counter(
            "dtq_tasks_failed_total",
            "Task executions that failed",
            ["queue", "task_type"],
            registry=reg,
        ),
        tasks_retried_total=Counter(
            "dtq_tasks_retried_total",
            "Attempts scheduled for retry",
            ["queue", "task_type"],
            registry=reg,
        ),
        tasks_dead_lettered_total=Counter(
            "dtq_tasks_dead_lettered_total",
            "Tasks moved to the dead-letter stream",
            ["queue", "task_type"],
            registry=reg,
        ),
        tasks_completed_total=Counter(
            "dtq_tasks_completed_total",
            "Tasks reaching a terminal state",
            ["queue", "task_type", "status"],
            registry=reg,
        ),
        task_attempts_total=Counter(
            "dtq_task_attempts_total",
            "Attempts by result",
            ["queue", "task_type", "result"],
            registry=reg,
        ),
        queue_depth=Gauge(
            "dtq_queue_depth",
            "Sum of XLEN across a queue's priority streams",
            ["queue"],
            registry=reg,
        ),
        queue_pending=Gauge(
            "dtq_queue_pending",
            "Total XPENDING entries across a queue's streams",
            ["queue"],
            registry=reg,
        ),
        retry_scheduled=Gauge(
            "dtq_retry_scheduled",
            "Tasks waiting in the retry schedule",
            registry=reg,
        ),
        dlq_depth=Gauge(
            "dtq_dlq_depth",
            "Entries in the dead-letter stream",
            registry=reg,
        ),
        workers_active=Gauge(
            "dtq_workers_active",
            "Workers with a fresh heartbeat",
            registry=reg,
        ),
        worker_inflight=Gauge(
            "dtq_worker_inflight",
            "In-flight executions per worker",
            ["worker_id"],
            registry=reg,
        ),
        worker_tasks_total=Gauge(
            "dtq_worker_tasks_total",
            "Tasks handled per worker",
            ["worker_id", "task_type", "result"],
            registry=reg,
        ),
        task_duration_seconds=Histogram(
            "dtq_task_duration_seconds",
            "End-to-end task time (created to terminal)",
            ["queue", "task_type"],
            registry=reg,
        ),
        task_execution_seconds=Histogram(
            "dtq_task_execution_seconds",
            "Handler-only execution time",
            ["queue", "task_type"],
            registry=reg,
        ),
        task_wait_seconds=Histogram(
            "dtq_task_wait_seconds",
            "Time from created to first execution start",
            ["queue", "task_type"],
            registry=reg,
        ),
        retry_delay_seconds=Histogram(
            "dtq_retry_delay_seconds",
            "Computed retry delay per attempt",
            ["queue", "task_type"],
            registry=reg,
        ),
        redis_operations_total=Counter(
            "dtq_redis_operations_total",
            "Redis operations by operation and result",
            ["operation", "result"],
            registry=reg,
        ),
        ingest_rejected_total=Counter(
            "dtq_ingest_rejected_total",
            "Ingest rejections by queue and reason",
            ["queue", "reason"],
            registry=reg,
        ),
    )
