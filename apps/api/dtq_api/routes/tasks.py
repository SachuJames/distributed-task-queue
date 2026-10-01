"""Task routes: submit, fetch, cancel, list."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse

from dtq_api import audit, store
from dtq_api.compat import (
    CancelRequestedError,
    InvalidTaskError,
    Settings,
    TaskNotFoundError,
    TaskStatus,
)
from dtq_api.deps import get_redis, get_settings, require_admin
from dtq_api.schemas import (
    CancelResponse,
    TaskListResponse,
    TaskResponse,
    TaskSubmitRequest,
)

router = APIRouter(tags=["tasks"])


@router.post(
    "/tasks",
    summary="Submit a task",
    response_description=(
        "202 for a new task, 200 with duplicate:true for a replayed idempotency key."
    ),
)
async def create_task(
    body: TaskSubmitRequest,
    request: Request,
    redis: store.RedisClient = Depends(get_redis),
    settings: Settings = Depends(get_settings),
) -> JSONResponse:
    """Submit a task. The Idempotency-Key header wins over the body field."""
    idempotency_key = request.headers.get("Idempotency-Key") or body.idempotency_key
    outcome = await store.submit_task(
        redis,
        settings,
        task_type=body.task_type,
        payload=body.payload,
        queue=body.queue,
        idempotency_key=idempotency_key,
        max_attempts=body.max_attempts,
        priority=body.priority,
        timeout_ms=body.timeout_ms,
        delay_seconds=body.delay_seconds,
        metadata=body.metadata,
    )
    content = TaskResponse.from_record(outcome.task, duplicate=not outcome.created).model_dump(
        mode="json"
    )
    return JSONResponse(status_code=202 if outcome.created else 200, content=content)


@router.get("/tasks/{task_id}", response_model=TaskResponse, summary="Get a task by id")
async def get_task_by_id(
    task_id: str,
    redis: store.RedisClient = Depends(get_redis),
) -> TaskResponse:
    record = await store.get_task(redis, task_id)
    if record is None:
        raise TaskNotFoundError(f"task {task_id} not found")
    return TaskResponse.from_record(record)


@router.post(
    "/tasks/{task_id}/cancel",
    summary="Cancel a task (admin when DTQ_API_KEY is set)",
    response_description="202 cancelled, 409 cancel requested on a running task.",
)
async def cancel_task_by_id(
    task_id: str,
    request: Request,
    redis: store.RedisClient = Depends(get_redis),
    settings: Settings = Depends(get_settings),
    actor: str = Depends(require_admin),
) -> JSONResponse:
    outcome = await store.cancel_task(redis, settings, task_id)
    request_id: str = getattr(request.state, "request_id", "")
    if outcome.cancel_requested:
        await audit.log_admin_action(
            redis,
            actor=actor,
            action="cancel_requested",
            task_id=task_id,
            request_id=request_id,
        )
        raise CancelRequestedError(f"task {task_id} is running; cancellation has been requested")
    await audit.log_admin_action(
        redis, actor=actor, action="cancel", task_id=task_id, request_id=request_id
    )
    content = CancelResponse(task_id=task_id, status="cancelled").model_dump(mode="json")
    return JSONResponse(status_code=202, content=content)


@router.get("/tasks", response_model=TaskListResponse, summary="List recent tasks")
async def list_tasks(
    redis: store.RedisClient = Depends(get_redis),
    queue: str | None = Query(default=None, description="Filter by queue name."),
    status: TaskStatus | None = Query(default=None, description="Filter by task status."),
    limit: int = Query(default=50, ge=1, le=500, description="Max tasks, recent first."),
) -> TaskListResponse:
    if queue is not None and not queue:
        raise InvalidTaskError("queue filter must not be empty")
    records = await store.list_recent_tasks(redis, queue=queue, status=status, limit=limit)
    tasks = [TaskResponse.from_record(r) for r in records]
    return TaskListResponse(tasks=tasks, count=len(tasks))
