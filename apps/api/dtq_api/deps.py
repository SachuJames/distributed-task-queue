"""Request-scoped FastAPI dependencies."""

from __future__ import annotations

from fastapi import Request

from dtq_api import store
from dtq_api.auth import check_api_key
from dtq_api.compat import Settings


def get_settings(request: Request) -> Settings:
    """Process settings, built once in create_app and stored on app.state."""
    return request.app.state.settings  # type: ignore[no-any-return]


def get_redis(request: Request) -> store.RedisClient:
    """Shared redis.asyncio pool, created once in the lifespan."""
    return request.app.state.redis  # type: ignore[no-any-return]


async def require_admin(request: Request) -> str:
    """Dependency for destructive ops. Returns the actor name when authorized.

    See dtq_api.auth for the rules: when DTQ_API_KEY is set, the X-API-Key
    header is required (constant-time compare); missing -> 401, wrong ->
    403. When unset, everything is open and the actor is "local".
    """
    settings = get_settings(request)
    return await check_api_key(request, settings)
