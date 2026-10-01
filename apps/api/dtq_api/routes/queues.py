"""Queue routes: stats, pause, resume."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, Request

from dtq_api import audit, store
from dtq_api.compat import (
    EventType,
    InvalidTaskError,
    Settings,
    bus_publish,
    is_valid_queue_name,
    utcnow_iso,
)
from dtq_api.deps import get_redis, get_settings, require_admin
from dtq_api.schemas import QueueListResponse, QueueStatsResponse

router = APIRouter(tags=["queues"])


def _config_event(queue: str, *, paused: bool) -> dict[str, Any]:
    """CONFIGURATION_CHANGED event; config changes have no task_id."""
    return {
        "type": EventType.CONFIGURATION_CHANGED.value,
        "task_id": "",
        "queue": queue,
        "ts": utcnow_iso(),
        "metadata": {"paused": paused},
    }


def _check_queue_name(queue: str) -> None:
    if not is_valid_queue_name(queue):
        raise InvalidTaskError(f"invalid queue name {queue!r}")


async def _queue_stats(
    redis: store.RedisClient,
    settings: Settings,
    queue: str,
    *,
    detail: bool,
) -> QueueStatsResponse:
    retry_by_queue = await store.retry_scheduled_by_queue(redis)
    stats = QueueStatsResponse(
        queue=queue,
        depth=await store.queue_depth(redis, queue),
        pending=await store.queue_pending(redis, queue),
        paused=await store.is_paused(redis, queue),
        retry_scheduled=retry_by_queue.get(queue, 0),
    )
    if detail:
        stats.dlq_depth = await store.dlq_depth(redis)
        now_s = datetime.now(UTC).timestamp()
        workers = await store.list_workers(redis)
        stats.workers_active = sum(
            1 for w in workers if w.is_live(now_s, settings.heartbeat_interval_s)
        )
    return stats


@router.get("/queues", response_model=QueueListResponse, summary="List queue stats")
async def list_queues(
    redis: store.RedisClient = Depends(get_redis),
    settings: Settings = Depends(get_settings),
) -> QueueListResponse:
    queues = await store.discover_queues(redis)
    stats = [await _queue_stats(redis, settings, q, detail=False) for q in queues]
    return QueueListResponse(queues=stats)


@router.get("/queues/{queue}", response_model=QueueStatsResponse, summary="Get one queue's stats")
async def get_queue(
    queue: str,
    redis: store.RedisClient = Depends(get_redis),
    settings: Settings = Depends(get_settings),
) -> QueueStatsResponse:
    _check_queue_name(queue)
    return await _queue_stats(redis, settings, queue, detail=True)


@router.post("/queue/{queue}/pause", summary="Pause a queue (admin when DTQ_API_KEY is set)")
async def pause_queue(
    queue: str,
    request: Request,
    redis: store.RedisClient = Depends(get_redis),
    actor: str = Depends(require_admin),
) -> dict[str, object]:
    _check_queue_name(queue)
    await store.pause_queue(redis, queue)
    await bus_publish(redis, _config_event(queue, paused=True))
    await audit.log_admin_action(
        redis,
        actor=actor,
        action="queue_pause",
        task_id=None,
        request_id=getattr(request.state, "request_id", ""),
    )
    return {"queue": queue, "paused": True}


@router.post("/queue/{queue}/resume", summary="Resume a queue (admin when DTQ_API_KEY is set)")
async def resume_queue(
    queue: str,
    request: Request,
    redis: store.RedisClient = Depends(get_redis),
    actor: str = Depends(require_admin),
) -> dict[str, object]:
    _check_queue_name(queue)
    await store.resume_queue(redis, queue)
    await bus_publish(redis, _config_event(queue, paused=False))
    await audit.log_admin_action(
        redis,
        actor=actor,
        action="queue_resume",
        task_id=None,
        request_id=getattr(request.state, "request_id", ""),
    )
    return {"queue": queue, "paused": False}
