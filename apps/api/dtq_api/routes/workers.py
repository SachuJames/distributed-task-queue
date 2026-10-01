"""Worker routes: list and detail with observer-computed liveness."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends

from dtq_api import store
from dtq_api.compat import Settings, WorkerNotFoundError
from dtq_api.deps import get_redis, get_settings
from dtq_api.schemas import WorkerListResponse, WorkerResponse

router = APIRouter(tags=["workers"])


@router.get("/workers", response_model=WorkerListResponse, summary="List workers")
async def list_workers(
    redis: store.RedisClient = Depends(get_redis),
    settings: Settings = Depends(get_settings),
) -> WorkerListResponse:
    now_s = datetime.now(UTC).timestamp()
    records = await store.list_workers(redis)
    workers = [
        WorkerResponse.from_record(r, interval_s=settings.heartbeat_interval_s, now_s=now_s)
        for r in records
    ]
    return WorkerListResponse(workers=workers)


@router.get(
    "/workers/{worker_id}",
    response_model=WorkerResponse,
    summary="Get a worker by id",
)
async def get_worker(
    worker_id: str,
    redis: store.RedisClient = Depends(get_redis),
    settings: Settings = Depends(get_settings),
) -> WorkerResponse:
    record = await store.get_worker(redis, worker_id)
    if record is None:
        raise WorkerNotFoundError(f"worker {worker_id} not found")
    now_s = datetime.now(UTC).timestamp()
    return WorkerResponse.from_record(record, interval_s=settings.heartbeat_interval_s, now_s=now_s)
