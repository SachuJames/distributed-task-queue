"""Dead-letter queue routes: list, inspect, requeue, discard, purge."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request

from dtq_api import audit, store
from dtq_api.compat import DLQEntryNotFoundError, Settings
from dtq_api.deps import get_redis, get_settings, require_admin
from dtq_api.schemas import DlqEntryResponse, DlqListResponse, TaskResponse

router = APIRouter(tags=["dlq"])


@router.get("/dlq", response_model=DlqListResponse, summary="List dead-letter entries")
async def list_dlq(
    redis: store.RedisClient = Depends(get_redis),
    limit: int = Query(default=50, ge=1, le=500),
) -> DlqListResponse:
    entries = await store.dlq_list(redis, limit=limit)
    responses = [DlqEntryResponse.from_entry(e) for e in entries]
    return DlqListResponse(entries=responses, count=len(responses))


@router.get("/dlq/{task_id}", response_model=DlqEntryResponse, summary="Get a dead-letter entry")
async def get_dlq_entry(
    task_id: str,
    redis: store.RedisClient = Depends(get_redis),
) -> DlqEntryResponse:
    entry = await store.dlq_find(redis, task_id)
    if entry is None:
        raise DLQEntryNotFoundError(f"no DLQ entry for task {task_id}")
    return DlqEntryResponse.from_entry(entry)


@router.post(
    "/dlq/{task_id}/requeue",
    response_model=TaskResponse,
    summary="Requeue a dead-lettered task (admin when DTQ_API_KEY is set)",
)
async def requeue_dlq_entry(
    task_id: str,
    request: Request,
    redis: store.RedisClient = Depends(get_redis),
    settings: Settings = Depends(get_settings),
    actor: str = Depends(require_admin),
) -> TaskResponse:
    record = await store.dlq_requeue(redis, settings, task_id)
    await audit.log_admin_action(
        redis,
        actor=actor,
        action="dlq_requeue",
        task_id=task_id,
        request_id=getattr(request.state, "request_id", ""),
    )
    return TaskResponse.from_record(record)


@router.post(
    "/dlq/{task_id}/discard",
    response_model=DlqEntryResponse,
    summary="Discard a dead-letter entry (admin when DTQ_API_KEY is set)",
)
async def discard_dlq_entry(
    task_id: str,
    request: Request,
    redis: store.RedisClient = Depends(get_redis),
    settings: Settings = Depends(get_settings),
    actor: str = Depends(require_admin),
) -> DlqEntryResponse:
    entry = await store.dlq_discard(redis, settings, task_id)
    await audit.log_admin_action(
        redis,
        actor=actor,
        action="dlq_discard",
        task_id=task_id,
        request_id=getattr(request.state, "request_id", ""),
    )
    return DlqEntryResponse.from_entry(entry)


@router.post("/dlq/purge", summary="Purge the dead-letter stream (admin when DTQ_API_KEY is set)")
async def purge_dlq(
    request: Request,
    redis: store.RedisClient = Depends(get_redis),
    actor: str = Depends(require_admin),
) -> dict[str, object]:
    removed = await store.dlq_purge(redis)
    await audit.log_admin_action(
        redis,
        actor=actor,
        action="dlq_purge",
        task_id=None,
        request_id=getattr(request.state, "request_id", ""),
    )
    return {"purged": removed}
