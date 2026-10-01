"""Handler helpers (contract section 15)."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from redis.asyncio import Redis


async def idempotent_operation(
    redis: Redis,
    key: str,
    ttl_seconds: int,
    fn: Callable[[], Any],
) -> tuple[bool, Any]:
    """Run ``fn`` once per ``key`` via SET NX.

    The first caller executes ``fn`` (sync or async) and gets (True, result);
    later callers within the TTL get (False, None) without running it.
    Use this inside handlers for dedup of side effects that must not repeat.
    """
    acquired: Any = await redis.set(key, "1", nx=True, ex=ttl_seconds)
    if not acquired:
        return False, None
    result = fn()
    if inspect.isawaitable(result):
        result = await result
    return True, result
