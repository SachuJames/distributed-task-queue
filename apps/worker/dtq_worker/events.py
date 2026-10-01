"""Lifecycle event publishing (contract section 8).

Events are JSON objects published on the ``dtq:events`` pub/sub channel via
the shared :mod:`dtq_events` bus. The API fans them out to WebSocket clients.
Two event types extend the contract's list and are documented here:

* ``TASK_EXPIRED``: a task passed its ``metadata.ttl_seconds`` before running.
* ``TASK_DUPLICATE_ABSORBED``: a duplicate stream entry was ACKed without
  re-executing the handler.
"""

from __future__ import annotations

from typing import Any

from dtq_events.bus import publish
from dtq_events.schemas import EventType, make_event
from redis.asyncio import Redis

#: Contract section 8 event types (re-exported for worker code).
TASK_QUEUED = EventType.TASK_QUEUED.value
TASK_STARTED = EventType.TASK_STARTED.value
TASK_SUCCEEDED = EventType.TASK_SUCCEEDED.value
TASK_FAILED = EventType.TASK_FAILED.value
TASK_RETRY_SCHEDULED = EventType.TASK_RETRY_SCHEDULED.value
TASK_RETRIED = EventType.TASK_RETRIED.value
TASK_DEAD_LETTERED = EventType.TASK_DEAD_LETTERED.value
TASK_CANCELLED = EventType.TASK_CANCELLED.value
WORKER_STARTED = EventType.WORKER_STARTED.value
WORKER_STOPPED = EventType.WORKER_STOPPED.value
WORKER_STALE = EventType.WORKER_STALE.value
CONFIGURATION_CHANGED = EventType.CONFIGURATION_CHANGED.value

#: Worker-side extensions (see module docstring).
TASK_EXPIRED = EventType.TASK_EXPIRED.value
TASK_DUPLICATE_ABSORBED = EventType.TASK_DUPLICATE_ABSORBED.value


async def publish_event(
    redis: Redis,
    event_type: str,
    *,
    task_id: str | None = None,
    queue: str | None = None,
    task_type: str | None = None,
    attempt: int | None = None,
    worker_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> int:
    """Publish one lifecycle event to the ``dtq:events`` channel.

    Returns the number of subscribers that received the event.
    """
    event = make_event(
        EventType(event_type),
        task_id=task_id,
        queue=queue,
        task_type=task_type,
        attempt=attempt,
        worker_id=worker_id,
        metadata=metadata,
    )
    return await publish(redis, event)
