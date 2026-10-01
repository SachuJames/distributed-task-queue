"""Redis client construction for the worker.

Honors ``DTQ_TEST_REDIS_DB`` so the test suite can point the worker at an
isolated database without changing ``DTQ_REDIS_URL``.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse, urlunparse

from dtq_config.settings import Settings
from redis.asyncio import Redis


def test_aware_url(url: str) -> str:
    """Rewrite the DB index of ``url`` when ``DTQ_TEST_REDIS_DB`` is set."""
    override = os.environ.get("DTQ_TEST_REDIS_DB")
    if not override:
        return url
    parts = urlparse(url)
    return urlunparse(parts._replace(path=f"/{override}"))


def make_redis(settings: Settings) -> Redis:
    """Build a decode_responses async Redis client from settings."""
    client: Redis = Redis.from_url(
        test_aware_url(settings.redis_url),
        decode_responses=True,
        socket_timeout=5,
    )
    return client


async def await_redis(call: Any) -> Any:
    """Await a redis-py call typed as a sync/async union.

    redis-py shares command implementations between its sync and async
    clients, so several commands are typed ``X | Awaitable[X]``. On the
    async client the result is always awaitable; this helper resolves it
    without a per-call cast.
    """
    return await call
