"""Shared helpers for the worker test suites.

Raw ingest here mirrors the API contract (task hash + stream entry, or the
retry schedule for delayed tasks) so worker tests do not depend on the API
builder's ingest path.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from dtq_config.settings import Settings
from dtq_core.keys import EVENTS_CHANNEL, RETRY_SCHEDULE, stream_name
from dtq_core.models import Task, TaskStatus
from dtq_queue.streams import stream_entry_fields, xadd_task, xpending_count
from dtq_queue.task_state import save_task
from dtq_worker.events import TASK_QUEUED, publish_event
from dtq_worker.worker import Worker
from redis.asyncio import Redis

TEST_DB = int(os.environ.get("DTQ_TEST_REDIS_DB", "3"))

log = logging.getLogger("tests.fixtures.worker_helpers")


def make_test_settings(**overrides: Any) -> Settings:
    """Settings tuned for fast, deterministic tests."""
    base: dict[str, Any] = {
        "redis_url": f"redis://localhost:6379/{TEST_DB}",
        "worker_concurrency": 4,
        "visibility_timeout_s": 30,
        "heartbeat_interval_s": 5,
        "retry_base_delay": 0.05,
        "retry_max_delay": 0.5,
        "retry_jitter": 0.0,
        "drain_timeout_s": 5,
        "log_level": "WARNING",
    }
    base.update(overrides)
    return Settings(**base)


async def submit_task(
    redis: Redis,
    *,
    task_type: str,
    payload: dict[str, Any],
    queue: str = "default",
    task_id: str | None = None,
    max_attempts: int = 5,
    priority: int = 5,
    timeout_ms: int = 30000,
    metadata: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
    delay_seconds: float = 0,
    retention_s: int = 604800,
) -> str:
    """Ingest a task the way the API would: hash + stream entry (or the
    retry schedule when delay_seconds > 0). Returns the task id."""
    now = datetime.now(UTC)
    tid = task_id or uuid.uuid4().hex
    status = TaskStatus.PENDING if delay_seconds > 0 else TaskStatus.QUEUED
    task = Task(
        task_id=tid,
        idempotency_key=idempotency_key,
        task_type=task_type,
        payload=payload,
        created_at=now,
        available_at=now + timedelta(seconds=delay_seconds),
        attempt=1,
        max_attempts=max_attempts,
        priority=priority,
        timeout_ms=timeout_ms,
        status=status,
        queue=queue,
        metadata=metadata or {},
    )
    await save_task(redis, task, retention_s)
    if delay_seconds > 0:
        due_ms = int((now + timedelta(seconds=delay_seconds)).timestamp() * 1000)
        await redis.zadd(RETRY_SCHEDULE, {tid: due_ms})
    else:
        await xadd_task(redis, stream_name(queue, priority), stream_entry_fields(task))
    # The API publishes TASK_QUEUED on ingest; mirror it so the event
    # lifecycle in tests matches production.
    await publish_event(
        redis,
        TASK_QUEUED,
        task_id=tid,
        queue=queue,
        task_type=task_type,
        attempt=1,
        metadata={"priority": priority, "delayed": delay_seconds > 0},
    )
    return tid


class EventCollector:
    """Buffers lifecycle events published on the ``dtq:events`` channel.

    Pub/sub has no history, so the collector must be subscribed *before* the
    events it should capture are published. Query with :meth:`of_type` or
    :meth:`all`; both return events newest-first as parsed dicts.
    """

    def __init__(self) -> None:
        self._events: list[dict[str, Any]] = []

    def _record(self, event: dict[str, Any]) -> None:
        self._events.insert(0, event)

    def of_type(self, event_type: str, task_id: str | None = None) -> list[dict[str, Any]]:
        """Events of one type (optionally for one task), newest first."""
        return [
            e
            for e in self._events
            if e.get("type") == event_type and (task_id is None or e.get("task_id") == task_id)
        ]

    def all(self, task_id: str | None = None) -> list[dict[str, Any]]:
        """All captured events (optionally for one task), newest first."""
        if task_id is None:
            return list(self._events)
        return [e for e in self._events if e.get("task_id") == task_id]


@asynccontextmanager
async def event_collector(redis: Redis) -> AsyncIterator[EventCollector]:
    """Subscribe to ``dtq:events`` and buffer every event until exit."""
    collector = EventCollector()
    pubsub = redis.pubsub()
    await pubsub.subscribe(EVENTS_CHANNEL)
    stop = asyncio.Event()

    async def _drain() -> None:
        try:
            while not stop.is_set():
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.05)
                if message is None:
                    continue
                data = message.get("data")
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                if isinstance(data, str):
                    try:
                        event = json.loads(data)
                    except json.JSONDecodeError:
                        log.debug("dropping undecodable event payload")
                        continue
                    if isinstance(event, dict):
                        collector._record(event)
        except asyncio.CancelledError:
            log.debug("event drain cancelled")
            raise

    drain_task = asyncio.create_task(_drain(), name="test-event-drain")
    try:
        # redis-py's subscribe() returns only after the server confirms the
        # subscription, so no event published after this point is missed.
        yield collector
    finally:
        stop.set()
        drain_task.cancel()
        try:
            await drain_task
        except asyncio.CancelledError:
            log.debug("drain task did not stop cleanly")
        await pubsub.unsubscribe(EVENTS_CHANNEL)
        await pubsub.aclose()  # type: ignore[no-untyped-call]


async def wait_for(
    check: Callable[[], Awaitable[Any]],
    timeout: float = 30.0,  # noqa: ASYNC109 - polling helper; asyncio.timeout does not apply
    interval: float = 0.1,
    what: str = "condition",
) -> Any:
    """Poll an async check until it returns truthy; raise on timeout."""
    deadline = time.monotonic() + timeout
    last: Any = None
    while True:
        last = await check()
        if last:
            return last
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out waiting for: {what}")
        await asyncio.sleep(interval)


async def task_status(redis: Redis, task_id: str) -> str | None:
    """Return the task hash status string, or None if the hash is missing."""
    pending: Any = redis.hget(f"dtq:task:{task_id}", "status")
    raw = await pending
    return str(raw) if raw is not None else None


def status_is(redis: Redis, task_id: str, status: str) -> Callable[[], Awaitable[bool]]:
    """Build a wait_for check that polls a task's hash status."""

    async def _check() -> bool:
        return await task_status(redis, task_id) == status

    return _check


async def total_pending(redis: Redis, queue: str = "default") -> int:
    """Sum XPENDING across a queue's priority streams."""
    total = 0
    for priority in range(10):
        total += await xpending_count(redis, stream_name(queue, priority), "workers")
    return total


@asynccontextmanager
async def background_worker(
    redis_client: Redis,
    settings: Settings | None = None,
    queues: tuple[str, ...] = ("default",),
) -> AsyncIterator[Worker]:
    """Run a worker in the background; shut it down gracefully on exit."""
    worker = Worker(
        settings or make_test_settings(),
        queues=list(queues),
        redis_client=redis_client,
    )
    run_task = asyncio.create_task(worker.run(), name=f"test-worker-{worker.worker_id}")
    try:
        yield worker
    finally:
        worker.initiate_shutdown()
        await asyncio.wait_for(run_task, timeout=30)
