"""Sync Redis client for example handlers (honors DTQ_TEST_REDIS_DB).

Handlers run in worker threads without an event loop, so the examples use
the synchronous client. Cached per process.
"""

from __future__ import annotations

import os

import redis
from dtq_worker.redis_client import test_aware_url

_client: redis.Redis | None = None


def sync_redis() -> redis.Redis:
    """Return a cached sync client pointed at the engine's Redis."""
    global _client
    if _client is None:
        url = test_aware_url(os.environ.get("DTQ_REDIS_URL", "redis://localhost:6379/0"))
        _client = redis.Redis.from_url(url, decode_responses=True, socket_timeout=5)
    return _client
