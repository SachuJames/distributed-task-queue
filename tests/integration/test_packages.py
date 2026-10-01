"""Integration tests for the shared packages (real Redis).

Run with DTQ_TEST_REDIS_DB set to a dedicated db index, e.g.
  DTQ_TEST_REDIS_DB=1 .venv/bin/pytest tests/integration/test_packages.py -q
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import UTC, datetime
from typing import Any

import pytest
from dtq_config.settings import Settings
from dtq_core.errors import (
    IdempotencyInProgressError,
    InvalidTaskError,
    PayloadTooLargeError,
    QueueFullError,
    UnknownTaskTypeError,
)
from dtq_core.keys import (
    RETRY_CLAIMED,
    RETRY_SCHEDULE,
    stream_name,
    task_key,
)
from dtq_core.models import InvalidTransition, Task, TaskStatus
from dtq_events import bus as events_bus
from dtq_events.schemas import EventType, make_event
from dtq_idem import store as idem_store
from dtq_queue import ingest as ingest_mod
from dtq_queue import lua as lua_scripts
from dtq_queue import task_state
from dtq_queue.streams import (
    dlq_range,
    dlq_xadd,
    dlq_xdel,
    ensure_consumer_groups,
    queue_depth,
    stream_entry_fields,
    trim_stream,
    xack,
    xadd_task,
    xautoclaim,
    xpending_count,
    xreadgroup_priority,
)
from dtq_retry import scheduler
from dtq_tasks.helpers import idempotent_operation

pytest_plugins = "tests.fixtures.redis"


def make_task(**overrides: Any) -> Task:
    now = datetime.now(UTC)
    fields: dict[str, Any] = {
        "task_id": "task-" + "ab12",
        "task_type": "echo_task",
        "payload": {"message": "hello"},
        "created_at": now,
        "available_at": now,
        "status": TaskStatus.QUEUED,
    }
    fields.update(overrides)
    return Task(**fields)


# ---------------------------------------------------------------- streams ---


async def test_streams_roundtrip(redis_client: Any) -> None:
    queue = "itest-streams"
    await ensure_consumer_groups(redis_client, [queue])
    # creating groups twice is idempotent (BUSYGROUP ignored)
    await ensure_consumer_groups(redis_client, [queue])

    low = await xadd_task(redis_client, stream_name(queue, 0), {"task_id": "low", "payload": "{}"})
    high = await xadd_task(
        redis_client, stream_name(queue, 9), {"task_id": "high", "payload": "{}"}
    )
    assert low and high

    # strict priority: p9 entry comes before p0 even though p0 was added first
    got = await xreadgroup_priority(redis_client, queue, "workers", "c1", count=10, block_ms=100)
    assert [(m[1], m[2]["task_id"]) for m in got] == [(high, "high"), (low, "low")]
    assert got[0][0] == stream_name(queue, 9)

    assert await xpending_count(redis_client, stream_name(queue, 9), "workers") == 1
    assert await xack(redis_client, stream_name(queue, 9), "workers", high) == 1
    assert await xpending_count(redis_client, stream_name(queue, 9), "workers") == 0
    assert await queue_depth(redis_client, queue) == 2


async def test_xautoclaim_reclaims_idle_entries(redis_client: Any) -> None:
    queue = "itest-claim"
    await ensure_consumer_groups(redis_client, [queue])
    stream = stream_name(queue, 5)
    eid = await xadd_task(redis_client, stream, {"task_id": "t1"})
    await xreadgroup_priority(redis_client, queue, "workers", "c1", count=10, block_ms=10)
    assert await xpending_count(redis_client, stream, "workers") == 1

    reclaimed = await xautoclaim(redis_client, stream, "workers", "c2", min_idle_ms=0)
    assert [(e, f["task_id"]) for e, f in reclaimed] == [(eid, "t1")]
    assert await xack(redis_client, stream, "workers", eid) == 1
    assert await xpending_count(redis_client, stream, "workers") == 0


async def test_dlq_helpers(redis_client: Any) -> None:
    e1 = await dlq_xadd(redis_client, {"task_id": "d1", "attempts_made": "3"})
    e2 = await dlq_xadd(redis_client, {"task_id": "d2", "attempts_made": "5"})
    entries = await dlq_range(redis_client, count=10)
    assert [f["task_id"] for _, f in entries] == ["d2", "d1"]  # newest first
    assert await dlq_xdel(redis_client, e1, e2) == 2
    assert await dlq_range(redis_client) == []

    stream = stream_name("itest-trim", 1)
    for i in range(500):
        await xadd_task(redis_client, stream, {"i": str(i)})
    removed = await trim_stream(redis_client, stream, 100)
    assert removed > 0
    assert await redis_client.xlen(stream) <= 150  # approximate trim bound


# ------------------------------------------------------------ idempotency ---


async def test_idempotency_acquire_complete(redis_client: Any) -> None:
    state, tid = await idem_store.acquire(redis_client, "q", "k1", "task-1", 60)
    assert (state, tid) == ("created", "task-1")

    state, tid = await idem_store.acquire(redis_client, "q", "k1", "task-2", 60)
    assert (state, tid) == ("processing", "task-1")

    assert await idem_store.peek(redis_client, "q", "k1") == ("processing", "task-1")
    assert await idem_store.peek(redis_client, "q", "absent") is None

    assert await idem_store.complete(redis_client, "q", "k1", "completed") is True
    state, tid = await idem_store.acquire(redis_client, "q", "k1", "task-3", 60)
    assert (state, tid) == ("completed", "task-1")

    assert await idem_store.complete(redis_client, "q", "missing", "failed") is False
    with pytest.raises(ValueError):
        await idem_store.complete(redis_client, "q", "k1", "bogus")


# ----------------------------------------------------------------- retry ---


async def test_retry_schedule_claim_lua(redis_client: Any) -> None:
    await scheduler.schedule_retry(redis_client, "t-due", 0)
    await scheduler.schedule_retry(redis_client, "t-future", 3600)

    claimed = await scheduler.claim_due(redis_client, 10)
    assert claimed == ["t-due"]
    assert await redis_client.zscore(RETRY_SCHEDULE, "t-due") is None
    assert await redis_client.zscore(RETRY_CLAIMED, "t-due") is not None
    # the future task is untouched
    assert await redis_client.zscore(RETRY_SCHEDULE, "t-future") is not None

    # age the claim past the stale threshold and requeue it
    old_ms = int(time.time() * 1000) - 120_000
    await redis_client.zadd(RETRY_CLAIMED, {"t-due": old_ms})
    stale = await scheduler.requeue_stale_claims(redis_client, stale_after_s=60)
    assert stale == ["t-due"]
    assert await redis_client.zscore(RETRY_SCHEDULE, "t-due") is not None
    assert await redis_client.zscore(RETRY_CLAIMED, "t-due") is None


async def test_claim_due_batch_capped_at_100(redis_client: Any) -> None:
    for i in range(150):
        await scheduler.schedule_retry(redis_client, f"cap-{i}", 0)
    claimed = await scheduler.claim_due(redis_client, 500)
    assert len(claimed) == 100


async def test_requeue_claimed_moves_to_stream(redis_client: Any) -> None:
    task = make_task(task_id="rq-1", status=TaskStatus.RETRYING, attempt=2, priority=3)
    await task_state.save_task(redis_client, task, 600)
    now_ms = int(time.time() * 1000)
    await redis_client.zadd(RETRY_CLAIMED, {"rq-1": now_ms})

    streams = {"rq-1": stream_name("rq", 3)}
    requeued = await scheduler.requeue_claimed(
        redis_client, ["rq-1", "ghost"], streams, retention_s=600
    )
    assert requeued == ["rq-1"]

    loaded = await task_state.get_task(redis_client, "rq-1")
    assert loaded is not None
    assert loaded.status is TaskStatus.QUEUED
    assert loaded.attempt == 3  # incremented to the upcoming attempt

    raw = await redis_client.xread({stream_name("rq", 3): "0-0"})
    assert raw[0][1][0][1]["task_id"] == "rq-1"
    assert raw[0][1][0][1]["attempt"] == "3"
    assert await redis_client.zscore(RETRY_CLAIMED, "rq-1") is None
    # unknown task ids are dropped from claimed, not stranded
    assert await redis_client.zscore(RETRY_CLAIMED, "ghost") is None


async def test_lua_scripts_are_atomic_strings(redis_client: Any) -> None:
    # direct script-level check: due members move in one call
    now_ms = int(time.time() * 1000)
    await redis_client.zadd(RETRY_SCHEDULE, {"lua-1": now_ms - 10})
    moved = await lua_scripts.claim_due(redis_client, now_ms, 10)
    assert moved == ["lua-1"]


# ------------------------------------------------------------- task_state ---


async def test_task_state_roundtrip(redis_client: Any) -> None:
    task = make_task(
        task_id="ts-1",
        idempotency_key="idem-1",
        metadata={"ttl_seconds": 60},
        priority=7,
    )
    assert await task_state.get_task(redis_client, "ts-1") is None
    await task_state.save_task(redis_client, task, 600)

    loaded = await task_state.get_task(redis_client, "ts-1")
    assert loaded == task
    assert loaded is not None and loaded.payload == {"message": "hello"}
    assert loaded.metadata == {"ttl_seconds": 60}
    assert await redis_client.ttl(task_key("ts-1")) > 0

    updated = await task_state.update_task(redis_client, "ts-1", TaskStatus.RUNNING, 600)
    assert updated.status is TaskStatus.RUNNING
    with pytest.raises(InvalidTransition):
        await task_state.update_task(redis_client, "ts-1", TaskStatus.QUEUED, 600)
    with pytest.raises(KeyError):
        await task_state.update_task(redis_client, "nope", TaskStatus.RUNNING, 600)


async def test_record_attempt_lifecycle(redis_client: Any) -> None:
    task = make_task(task_id="ts-2", status=TaskStatus.QUEUED)
    await task_state.save_task(redis_client, task, 600)

    started = await task_state.record_attempt_start(redis_client, "ts-2", "worker-1", 600)
    assert started.status is TaskStatus.RUNNING
    assert started.worker_id == "worker-1"
    assert started.started_at is not None
    assert started.attempt == 1  # assigned at ingest, not incremented here

    redelivered = await task_state.record_redelivery(redis_client, "ts-2", "worker-2", 600)
    assert redelivered.attempt == 2
    assert redelivered.status is TaskStatus.RUNNING

    ended = await task_state.record_attempt_end(
        redis_client,
        "ts-2",
        600,
        error_class="ValueError",
        error_message="bad\ninput" * 200,  # sanitized + capped at 500
        retryable=False,
    )
    assert ended.finished_at is not None
    assert ended.duration_ms is not None and ended.duration_ms >= 0
    assert ended.error_class == "ValueError"
    assert "\n" not in (ended.error_message or "")
    assert len(ended.error_message or "") <= 500

    with pytest.raises(task_state.InvalidTransitionForRedelivery):
        pending = make_task(task_id="ts-3", status=TaskStatus.PENDING)
        await task_state.save_task(redis_client, pending, 600)
        await task_state.record_redelivery(redis_client, "ts-3", "worker-1", 600)


# ----------------------------------------------------------------- ingest ---


async def test_ingest_happy_path(redis_client: Any) -> None:
    settings = Settings()
    task, duplicate = await ingest_mod.submit_task(
        redis_client,
        task_type="echo_task",
        payload={"n": 1},
        queue="ingestq",
        idempotency_key="ik-1",
        settings=settings,
        is_registered=lambda name: True,
    )
    assert duplicate is False
    assert task.status is TaskStatus.QUEUED
    assert task.attempt == 1
    assert len(task.task_id) == 32

    assert await queue_depth(redis_client, "ingestq") == 1
    stored = await task_state.get_task(redis_client, task.task_id)
    assert stored == task
    # recent zset indexed
    assert await redis_client.zscore("dtq:tasks:recent", task.task_id) is not None


async def test_ingest_delayed_goes_to_schedule(redis_client: Any) -> None:
    settings = Settings()
    task, _ = await ingest_mod.submit_task(
        redis_client,
        task_type="echo_task",
        payload={},
        queue="delayq",
        delay_seconds=30,
        settings=settings,
    )
    assert task.status is TaskStatus.PENDING
    assert await queue_depth(redis_client, "delayq") == 0
    assert await redis_client.zscore(RETRY_SCHEDULE, task.task_id) is not None


async def test_ingest_validation(redis_client: Any) -> None:
    settings = Settings()
    base: dict[str, Any] = {
        "task_type": "echo_task",
        "payload": {"n": 1},
        "queue": "valq",
        "settings": settings,
    }
    with pytest.raises(InvalidTaskError):
        await ingest_mod.submit_task(redis_client, **{**base, "queue": "Bad Name!"})
    with pytest.raises(UnknownTaskTypeError):
        await ingest_mod.submit_task(redis_client, **{**base, "is_registered": lambda name: False})
    with pytest.raises(InvalidTaskError):
        await ingest_mod.submit_task(redis_client, **{**base, "priority": 10})
    with pytest.raises(InvalidTaskError):
        await ingest_mod.submit_task(redis_client, **{**base, "max_attempts": 99})
    with pytest.raises(InvalidTaskError):
        await ingest_mod.submit_task(redis_client, **{**base, "delay_seconds": -1})
    with pytest.raises(InvalidTaskError):
        await ingest_mod.submit_task(redis_client, **{**base, "metadata": {"blob": "x" * 5000}})
    small_settings = Settings(max_payload_bytes=16)
    with pytest.raises(PayloadTooLargeError):
        await ingest_mod.submit_task(
            redis_client,
            **{**base, "payload": {"blob": "x" * 100}, "settings": small_settings},
        )


async def test_ingest_backpressure(redis_client: Any) -> None:
    settings = Settings(max_queue_depth=1)
    await ingest_mod.submit_task(
        redis_client, task_type="echo_task", payload={}, queue="fullq", settings=settings
    )
    with pytest.raises(QueueFullError) as exc_info:
        await ingest_mod.submit_task(
            redis_client,
            task_type="echo_task",
            payload={},
            queue="fullq",
            settings=settings,
        )
    assert exc_info.value.http_status == 429
    assert exc_info.value.retry_after_s == 5


async def test_ingest_duplicate_key_convergence(redis_client: Any) -> None:
    """Two concurrent submits with the same key produce exactly one task."""
    settings = Settings()
    kwargs: dict[str, Any] = {
        "task_type": "echo_task",
        "payload": {"n": 1},
        "queue": "convq",
        "idempotency_key": "conv-key",
        "settings": settings,
        "is_registered": lambda name: True,
    }
    first, second = await asyncio.gather(
        ingest_mod.submit_task(redis_client, **kwargs),
        ingest_mod.submit_task(redis_client, **kwargs),
        return_exceptions=True,
    )
    outcomes = []
    winner: Task | None = None
    for result in (first, second):
        if isinstance(result, BaseException):
            assert isinstance(result, IdempotencyInProgressError)
            outcomes.append("conflict")
        else:
            task, duplicate = result
            assert duplicate is False
            winner = task
            outcomes.append("created")
    assert sorted(outcomes) == ["conflict", "created"]
    assert winner is not None
    # exactly one logical task exists
    assert await queue_depth(redis_client, "convq") == 1

    # once completed, the same key replays the stored task as a duplicate
    assert await idem_store.complete(redis_client, "convq", "conv-key", "completed")
    replay, duplicate = await ingest_mod.submit_task(redis_client, **kwargs)
    assert duplicate is True
    assert replay.task_id == winner.task_id
    assert await queue_depth(redis_client, "convq") == 1


async def test_ingest_stream_entry_fields(redis_client: Any) -> None:
    settings = Settings()
    task, _ = await ingest_mod.submit_task(
        redis_client,
        task_type="echo_task",
        payload={"a": [1, 2]},
        queue="fieldsq",
        priority=8,
        idempotency_key="ik-fields",
        settings=settings,
    )
    raw = await redis_client.xread({stream_name("fieldsq", 8): "0-0"})
    fields = raw[0][1][0][1]
    assert fields["task_id"] == task.task_id
    assert fields["queue"] == "fieldsq"
    assert fields["task_type"] == "echo_task"
    assert fields["attempt"] == "1"
    assert fields["max_attempts"] == "5"
    assert fields["priority"] == "8"
    assert fields["timeout_ms"] == "30000"
    assert fields["idempotency_key"] == "ik-fields"
    assert fields["payload"] == task.payload_json()


# ------------------------------------------------------------------ events ---


async def test_event_bus_pubsub(redis_client: Any) -> None:
    received: list[dict[str, Any]] = []

    async def listener() -> None:
        async for event in events_bus.subscribe(redis_client):
            received.append(event)
            break

    listen_task = asyncio.create_task(listener())
    try:
        for _ in range(50):
            await events_bus.publish(
                redis_client,
                make_event(EventType.WORKER_STARTED, worker_id="w-test"),
            )
            await asyncio.sleep(0.1)
            if received:
                break
        await asyncio.wait_for(listen_task, timeout=10)
    finally:
        if not listen_task.done():
            listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                await listen_task

    assert len(received) == 1
    assert received[0]["type"] == "WORKER_STARTED"
    assert received[0]["worker_id"] == "w-test"


# ------------------------------------------------------------------ helpers ---


async def test_idempotent_operation_helper(redis_client: Any) -> None:
    calls = []

    async def fn() -> str:
        calls.append(1)
        return "done"

    ran, result = await idempotent_operation(redis_client, "idemop:1", 60, fn)
    assert (ran, result) == (True, "done")
    ran, result = await idempotent_operation(redis_client, "idemop:1", 60, fn)
    assert (ran, result) == (False, None)
    assert len(calls) == 1

    ran, result = await idempotent_operation(redis_client, "idemop:2", 60, lambda: 42)
    assert (ran, result) == (True, 42)


async def test_stream_entry_fields_builder() -> None:
    task = make_task(task_id="f1", priority=2, idempotency_key=None)
    fields = stream_entry_fields(task)
    assert fields["idempotency_key"] == ""
    assert fields["priority"] == "2"
    assert set(fields) == {
        "task_id",
        "queue",
        "task_type",
        "payload",
        "attempt",
        "max_attempts",
        "priority",
        "timeout_ms",
        "idempotency_key",
    }
