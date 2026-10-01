"""FastAPI application factory for the DTQ API service."""

from __future__ import annotations

import asyncio
import importlib
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import redis.asyncio
from dtq_core.keys import CONSUMER_GROUP
from fastapi import Depends, FastAPI, Request, Response

from dtq_api import metrics as metrics_mod
from dtq_api import store
from dtq_api import websocket as ws_mod
from dtq_api.compat import RedisUnavailableError, Settings
from dtq_api.deps import get_redis
from dtq_api.errors import register_error_handlers
from dtq_api.logging_config import setup_logging
from dtq_api.routes import dlq, queues, tasks, workers

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Shared redis.asyncio pool, consumer groups, metrics, WS fanout.

    Graceful shutdown: uvicorn stops accepting new connections, in-flight
    requests drain, then the fanout task is cancelled and the pool closed.
    """
    settings: Settings = app.state.settings
    for module_name in [m.strip() for m in settings.task_modules.split(",") if m.strip()]:
        importlib.import_module(module_name)
        log.info("loaded task module %s", module_name)
    client: store.RedisClient = redis.asyncio.Redis.from_url(
        settings.effective_redis_url, decode_responses=True
    )
    app.state.redis = client
    await client.ping()  # fail fast when Redis is unreachable
    await store.ensure_consumer_groups(client)
    stop = asyncio.Event()
    fanout = asyncio.create_task(ws_mod.fanout_loop(client, app, stop), name="dtq-ws-fanout")
    log.info("dtq api started")
    try:
        yield
    finally:
        stop.set()
        fanout.cancel()
        try:
            await fanout
        except asyncio.CancelledError:
            log.debug("ws fanout task cancelled on shutdown")
        await client.aclose()
        log.info("dtq api stopped")


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the FastAPI application."""
    cfg = settings or Settings()
    setup_logging(cfg)
    app = FastAPI(
        title="DTQ API",
        description=(
            "HTTP API for the Distributed Task Queue Engine: task ingest with "
            "idempotency and backpressure, queue/worker/DLQ observability, and "
            "a real-time WebSocket event stream."
        ),
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.settings = cfg
    app.state.ws_clients = set()
    register_error_handlers(app)

    @app.middleware("http")
    async def request_id_middleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    app.include_router(tasks.router, prefix="/api/v1")
    app.include_router(queues.router, prefix="/api/v1")
    app.include_router(workers.router, prefix="/api/v1")
    app.include_router(dlq.router, prefix="/api/v1")
    app.websocket(cfg.ws_path)(ws_mod.websocket_endpoint)

    @app.get("/health", summary="Liveness probe", tags=["ops"])
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "dtq-api"}

    @app.get("/ready", summary="Readiness probe", tags=["ops"])
    async def ready(
        redis: store.RedisClient = Depends(get_redis),
    ) -> dict[str, str]:
        try:
            pong = await redis.ping()
        except Exception as exc:
            raise RedisUnavailableError(f"redis unavailable: {exc}") from exc
        if not pong:
            raise RedisUnavailableError("redis ping failed")
        missing = await store.streams_missing_group(redis)
        if missing:
            raise RedisUnavailableError(
                f"consumer group '{CONSUMER_GROUP}' missing on {len(missing)} stream(s)"
            )
        return {"status": "ready"}

    @app.get("/metrics", summary="Prometheus exposition", tags=["ops"])
    async def metrics(
        redis: store.RedisClient = Depends(get_redis),
    ) -> Response:
        await metrics_mod.refresh_gauges(redis)
        body, content_type = metrics_mod.exposition()
        return Response(content=body, media_type=content_type)

    return app


def main() -> None:
    """Run the API with uvicorn (used for local dev)."""
    import uvicorn

    cfg = Settings()
    uvicorn.run(create_app(cfg), host=cfg.api_host, port=cfg.api_port)
