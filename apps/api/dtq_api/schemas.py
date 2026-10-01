"""Pydantic request/response models for the DTQ HTTP API."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from dtq_api.compat import Task
from dtq_api.store import DlqEntry, WorkerRecord


class TaskSubmitRequest(BaseModel):
    """Body for POST /api/v1/tasks."""

    task_type: str = Field(
        ...,
        description="Registered handler name, e.g. echo_task. Must exist in the handler registry.",
        examples=["echo_task"],
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="JSON object passed to the handler. Never executable code.",
        examples=[{"message": "hello"}],
    )
    queue: str = Field(
        default="default",
        description="Queue name, must match ^[a-z0-9][a-z0-9_-]{0,63}$.",
        examples=["default"],
    )
    idempotency_key: str | None = Field(
        default=None,
        description=(
            "Client-supplied idempotency key. The Idempotency-Key header wins over this field."
        ),
        examples=["order-1234"],
    )
    max_attempts: int = Field(
        default=5,
        ge=1,
        description="Max execution attempts, 1..DTQ_MAX_ATTEMPTS.",
        examples=[5],
    )
    priority: int = Field(
        default=5,
        ge=0,
        le=9,
        description="Strict priority band 0..9. Workers drain p9 down to p0.",
        examples=[5],
    )
    timeout_ms: int | None = Field(
        default=None,
        description="Handler timeout in ms. Defaults to DTQ_TASK_TIMEOUT_MS, max 3600000.",
        examples=[30000],
    )
    delay_seconds: float = Field(
        default=0.0,
        ge=0.0,
        description=(
            "Delay before the task becomes eligible. Delayed tasks wait in the retry schedule."
        ),
        examples=[0.0],
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="JSON object, max 4KB serialized. Recognized key: ttl_seconds.",
        examples=[{"ttl_seconds": 3600}],
    )


class ExecutionMetadata(BaseModel):
    """Per-attempt execution metadata recorded on the task."""

    started_at: datetime | None = Field(default=None, examples=["2026-10-01T09:00:00+00:00"])
    finished_at: datetime | None = Field(default=None, examples=["2026-10-01T09:00:01+00:00"])
    duration_ms: int | None = Field(default=None, examples=[1200])
    worker_id: str | None = Field(default=None, examples=["worker-1a2b3c4d"])
    error_class: str | None = Field(default=None, examples=["TimeoutError"])
    error_message: str | None = Field(
        default=None, description="Sanitized, max 500 chars.", examples=["handler timed out"]
    )
    retryable: bool | None = Field(default=None, examples=[True])
    retry_delay_ms: int | None = Field(default=None, examples=[2000])


class TaskResponse(BaseModel):
    """Full task representation, including execution metadata."""

    task_id: str = Field(examples=["9f3c2a1b4d5e6f7890abcdef12345678"])
    idempotency_key: str | None = Field(default=None, examples=["order-1234"])
    task_type: str = Field(examples=["echo_task"])
    payload: dict[str, Any] = Field(examples=[{"message": "hello"}])
    queue: str = Field(examples=["default"])
    status: str = Field(examples=["queued"])
    attempt: int = Field(examples=[0])
    max_attempts: int = Field(examples=[5])
    priority: int = Field(examples=[5])
    timeout_ms: int = Field(examples=[30000])
    created_at: datetime = Field(examples=["2026-10-01T09:00:00+00:00"])
    available_at: datetime = Field(examples=["2026-10-01T09:00:00+00:00"])
    metadata: dict[str, Any] = Field(examples=[{"ttl_seconds": 3600}])
    execution: ExecutionMetadata = Field(default_factory=ExecutionMetadata)
    duplicate: bool = Field(
        default=False,
        description=(
            "True when this response replays an earlier submission with the same idempotency key."
        ),
        examples=[False],
    )

    @classmethod
    def from_record(cls, record: Task, *, duplicate: bool = False) -> TaskResponse:
        return cls(
            task_id=record.task_id,
            idempotency_key=record.idempotency_key,
            task_type=record.task_type,
            payload=record.payload,
            queue=record.queue,
            status=record.status.value,
            attempt=record.attempt,
            max_attempts=record.max_attempts,
            priority=record.priority,
            timeout_ms=record.timeout_ms,
            created_at=record.created_at,
            available_at=record.available_at,
            metadata=record.metadata,
            execution=ExecutionMetadata(
                started_at=record.started_at,
                finished_at=record.finished_at,
                duration_ms=record.duration_ms,
                worker_id=record.worker_id,
                error_class=record.error_class,
                error_message=record.error_message,
                retryable=record.retryable,
                retry_delay_ms=record.retry_delay_ms,
            ),
            duplicate=duplicate,
        )


class TaskListResponse(BaseModel):
    """Response for GET /api/v1/tasks. Tasks are ordered recent first."""

    tasks: list[TaskResponse] = Field(examples=[[]])
    count: int = Field(description="Number of tasks in this response.", examples=[3])


class QueueStatsResponse(BaseModel):
    """Per-queue statistics. Each stat is labeled exact or approximate."""

    queue: str = Field(examples=["default"])
    depth: int = Field(
        description="Sum of XLEN across the queue's priority streams. Exact.",
        examples=[12],
    )
    pending: int = Field(
        description="Sum of XPENDING across the queue's priority streams. Exact.",
        examples=[2],
    )
    paused: bool = Field(description="Whether the queue is paused. Exact.", examples=[False])
    retry_scheduled: int = Field(
        description="Tasks in the retry/delay schedule for this queue. Exact, point-in-time.",
        examples=[1],
    )
    dlq_depth: int | None = Field(
        default=None,
        description=(
            "Entries in the global dead-letter stream. Exact. Only on the single-queue view."
        ),
        examples=[0],
    )
    workers_active: int | None = Field(
        default=None,
        description=(
            "Workers with a fresh heartbeat. Approximate "
            "(liveness is time-based). Only on the single-queue view."
        ),
        examples=[2],
    )


class QueueListResponse(BaseModel):
    queues: list[QueueStatsResponse] = Field(examples=[[]])


class WorkerResponse(BaseModel):
    """Worker record. Liveness is observer-computed from heartbeat age."""

    worker_id: str = Field(examples=["worker-1a2b3c4d"])
    hostname: str = Field(examples=["worker-host-01"])
    status: str = Field(
        description=(
            "Last reported status, or 'stale' when the heartbeat is older than 3x the interval."
        ),
        examples=["ready"],
    )
    live: bool = Field(
        description=(
            "True when the last heartbeat is within 3x the heartbeat "
            "interval. Never inferred from record existence alone."
        ),
        examples=[True],
    )
    last_heartbeat_age_s: float | None = Field(
        default=None,
        description="Seconds since the last heartbeat. Approximate.",
        examples=[2.5],
    )
    active_tasks: int = Field(examples=[1])
    concurrency: int = Field(examples=[4])
    tasks_processed: int = Field(examples=[120])
    tasks_failed: int = Field(examples=[3])
    tasks_retried: int = Field(examples=[5])

    @classmethod
    def from_record(
        cls, record: WorkerRecord, *, interval_s: float, now_s: float
    ) -> WorkerResponse:
        live = record.is_live(now_s, interval_s)
        age = (now_s - record.last_heartbeat) if record.last_heartbeat is not None else None
        return cls(
            worker_id=record.worker_id,
            hostname=record.hostname,
            status=record.status if live else "stale",
            live=live,
            last_heartbeat_age_s=age,
            active_tasks=record.active_tasks,
            concurrency=record.concurrency,
            tasks_processed=record.tasks_processed,
            tasks_failed=record.tasks_failed,
            tasks_retried=record.tasks_retried,
        )


class WorkerListResponse(BaseModel):
    workers: list[WorkerResponse] = Field(examples=[[]])


class DlqEntryResponse(BaseModel):
    """One dead-letter stream entry."""

    entry_id: str = Field(examples=["1727779200000-0"])
    task_id: str = Field(examples=["9f3c2a1b4d5e6f7890abcdef12345678"])
    queue: str = Field(examples=["default"])
    task_type: str = Field(examples=["echo_task"])
    attempt: int = Field(examples=[3])
    max_attempts: int = Field(examples=[5])
    priority: int = Field(examples=[5])
    timeout_ms: int = Field(examples=[30000])
    failed_at: datetime | None = Field(examples=["2026-10-01T09:05:00+00:00"])
    attempts_made: int = Field(examples=[3])
    last_error: str = Field(examples=["connection refused"])
    last_error_class: str = Field(examples=["ConnectionError"])
    retryable: bool = Field(examples=[True])
    worker_id: str = Field(examples=["worker-1a2b3c4d"])
    original_queue: str = Field(examples=["default"])

    @classmethod
    def from_entry(cls, entry: DlqEntry) -> DlqEntryResponse:
        return cls(
            entry_id=entry.entry_id,
            task_id=entry.task_id,
            queue=entry.queue,
            task_type=entry.task_type,
            attempt=entry.attempt,
            max_attempts=entry.max_attempts,
            priority=entry.priority,
            timeout_ms=entry.timeout_ms,
            failed_at=entry.failed_at,
            attempts_made=entry.attempts_made,
            last_error=entry.last_error,
            last_error_class=entry.last_error_class,
            retryable=entry.retryable,
            worker_id=entry.worker_id,
            original_queue=entry.original_queue,
        )


class DlqListResponse(BaseModel):
    entries: list[DlqEntryResponse] = Field(examples=[[]])
    count: int = Field(description="Number of entries in this response.", examples=[2])


class CancelResponse(BaseModel):
    task_id: str = Field(examples=["9f3c2a1b4d5e6f7890abcdef12345678"])
    status: str = Field(examples=["cancelled"])


class ErrorResponse(BaseModel):
    """Consistent error body for every DTQ failure."""

    code: str = Field(
        description="Stable machine-readable code, e.g. TASK_NOT_FOUND.",
        examples=["TASK_NOT_FOUND"],
    )
    message: str = Field(examples=["task 9f3c not found"])
    request_id: str = Field(examples=["b3c4d5e6f7890abcdef1234567890abcd"])
