"""Prometheus metrics owned by the worker (contract section 9).

All metric names and label sets follow the contract exactly: labels are only
``task_type``, ``queue`` and ``status``/``result``, plus the explicitly
allowed ``worker_id`` on the per-worker gauges/counters (one label value per
process, so cardinality stays bounded).

The metrics live in a private :data:`REGISTRY` (not the global default) so
the worker can be imported alongside other engine components without
duplicate-timeseries registration errors. Wire ``REGISTRY`` to whatever
exposition the deployment uses.

Ownership note: the worker updates the task-lifecycle and worker-liveness
metrics below. ``dtq_queue_depth``, ``dtq_queue_pending``,
``dtq_retry_scheduled``, ``dtq_dlq_depth``, ``dtq_tasks_submitted_total``,
``dtq_redis_operations_total`` and ``dtq_ingest_rejected_total`` are defined
here for schema completeness but are updated by the API/observer side.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

REGISTRY = CollectorRegistry()

tasks_submitted_total = Counter(
    "dtq_tasks_submitted_total",
    "Tasks accepted at ingest.",
    ["queue", "task_type"],
    registry=REGISTRY,
)
tasks_started_total = Counter(
    "dtq_tasks_started_total",
    "Task executions started by workers.",
    ["queue", "task_type"],
    registry=REGISTRY,
)
tasks_succeeded_total = Counter(
    "dtq_tasks_succeeded_total",
    "Task executions that returned a result.",
    ["queue", "task_type"],
    registry=REGISTRY,
)
tasks_failed_total = Counter(
    "dtq_tasks_failed_total",
    "Tasks that reached FAILED (attempts exhausted, DLQ disabled).",
    ["queue", "task_type"],
    registry=REGISTRY,
)
tasks_retried_total = Counter(
    "dtq_tasks_retried_total",
    "Attempts that were scheduled for retry.",
    ["queue", "task_type"],
    registry=REGISTRY,
)
tasks_dead_lettered_total = Counter(
    "dtq_tasks_dead_lettered_total",
    "Tasks moved to the dead-letter stream.",
    ["queue", "task_type"],
    registry=REGISTRY,
)
tasks_completed_total = Counter(
    "dtq_tasks_completed_total",
    "Tasks that reached a terminal status.",
    ["queue", "task_type", "status"],
    registry=REGISTRY,
)
task_attempts_total = Counter(
    "dtq_task_attempts_total",
    "Handler attempts by outcome.",
    ["queue", "task_type", "result"],
    registry=REGISTRY,
)
queue_depth = Gauge(
    "dtq_queue_depth",
    "Sum of XLEN across a queue's priority streams.",
    ["queue"],
    registry=REGISTRY,
)
queue_pending = Gauge(
    "dtq_queue_pending",
    "Total XPENDING entries across a queue's priority streams.",
    ["queue"],
    registry=REGISTRY,
)
retry_scheduled = Gauge(
    "dtq_retry_scheduled",
    "Tasks currently sitting in the retry/delay schedule.",
    registry=REGISTRY,
)
dlq_depth = Gauge(
    "dtq_dlq_depth",
    "Approximate length of the dead-letter stream.",
    registry=REGISTRY,
)
workers_active = Gauge(
    "dtq_workers_active",
    "Workers currently running in this process (0 or 1).",
    registry=REGISTRY,
)
worker_inflight = Gauge(
    "dtq_worker_inflight",
    "Executions currently owned by this worker.",
    ["worker_id"],
    registry=REGISTRY,
)
worker_tasks_total = Counter(
    "dtq_worker_tasks_total",
    "Executions finished by this worker, by outcome.",
    ["worker_id", "task_type", "result"],
    registry=REGISTRY,
)
task_duration_seconds = Histogram(
    "dtq_task_duration_seconds",
    "End-to-end task time: created_at to finished_at.",
    ["queue", "task_type"],
    registry=REGISTRY,
)
task_execution_seconds = Histogram(
    "dtq_task_execution_seconds",
    "Handler-only execution time.",
    ["queue", "task_type"],
    registry=REGISTRY,
)
task_wait_seconds = Histogram(
    "dtq_task_wait_seconds",
    "Time from created_at to execution start.",
    ["queue", "task_type"],
    registry=REGISTRY,
)
retry_delay_seconds = Histogram(
    "dtq_task_retry_delay_seconds",
    "Scheduled delay before the next attempt.",
    ["queue", "task_type"],
    registry=REGISTRY,
)
redis_operations_total = Counter(
    "dtq_redis_operations_total",
    "Redis operations issued by the engine.",
    ["operation", "result"],
    registry=REGISTRY,
)
ingest_rejected_total = Counter(
    "dtq_ingest_rejected_total",
    "Ingest requests rejected at admission.",
    ["queue", "reason"],
    registry=REGISTRY,
)
