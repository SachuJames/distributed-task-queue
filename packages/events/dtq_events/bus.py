"""Publish/subscribe event bus over Redis pub/sub (contract section 8)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from dtq_core.keys import EVENTS_CHANNEL
from redis.asyncio import Redis


async def publish(redis: Redis, event: dict[str, Any]) -> int:
    """Publish an event dict to dtq:events. Returns the receiver count."""
    receivers: Any = await redis.publish(EVENTS_CHANNEL, json.dumps(event))
    return int(receivers)


async def subscribe(redis: Redis) -> AsyncIterator[dict[str, Any]]:
    """Yield decoded events from dtq:events until cancelled.

    Intended for the WebSocket endpoint: one subscriber task per client,
    cancelled when the client disconnects.
    """
    pubsub = redis.pubsub()
    await pubsub.subscribe(EVENTS_CHANNEL)
    try:
        async for message in pubsub.listen():
            if not isinstance(message, dict) or message.get("type") != "message":
                continue
            data = message.get("data")
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            if isinstance(data, str):
                payload: dict[str, Any] = json.loads(data)
                yield payload
    finally:
        await pubsub.unsubscribe(EVENTS_CHANNEL)
        await pubsub.aclose()  # type: ignore[no-untyped-call]
