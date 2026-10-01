"""Concurrency and race tests for the worker (real Redis, db 3)."""

from __future__ import annotations

import asyncio
import os
import signal
import time
import uuid
from typing import Any

from dtq_core.keys import (
    CANCELLED_SET,
    RETRY_CLAIMED,
    RETRY_SCHEDULE,
    stream_name,
    task_key,
)
from dtq_core.models import TaskStatus
from dtq_queue.task_state import update_task
from dtq_worker.idem import claim_idem
from dtq_worker.redis_client import await_redis
from dtq_worker.worker import Worker
from redis.asyncio import Redis

import examples.tasks  # noqa: F401  (registers demo handlers)
from tests.fixtures.redis import make_redis
from tests.fixtures.worker_helpers import (
    background_worker,
    event_collector,
    make_test_settings,
    status_is,
    submit_task,
    task_status,
    total_pending,
    wait_for,
)

pytest_plugins = "tests.fixtures.redis"


async def _hash(redis_client: Redis, task_id: str) -> dict[Any, Any]:
    pending: Any = redis_client.hgetall(task_key(task_id))
    return dict(await pending)


async def test_two_workers_race_same_entries_no_double_execution(redis_client: Redis) -> None:
    async with event_collector(redis_client) as events:
        tids = [
            await submit_task(redis_client, task_type="echo_task", payload={"i": i})
            for i in range(10)
        ]
        settings = make_test_settings()
        extra = make_redis()
        try:
            async with background_worker(redis_client, settings):
                async with background_worker(extra, settings):
                    for tid in tids:
                        await wait_for(
                            status_is(redis_client, tid, "succeeded"),
                            timeout=60,
                            what=f"{tid} succeeded",
                        )
        finally:
            await extra.aclose()
    for tid in tids:
        raw = await _hash(redis_client, tid)
        assert raw["status"] == "succeeded", tid
        assert int(raw["attempt"]) == 1, tid
        started = events.of_type("TASK_STARTED", tid)
        assert len(started) == 1, tid
    assert await total_pending(redis_client) == 0


async def test_concurrent_submits_same_idempotency_key_one_winner(redis_client: Redis) -> None:
    key = "race-key-1"

    async def try_ingest(i: int) -> str | None:
        tid = uuid.uuid4().hex
        won = await claim_idem(redis_client, "default", key, tid, 86400)
        if not won:
            return None
        await submit_task(
            redis_client,
            task_type="counter_task",
            payload={"counter": "idem-race"},
            task_id=tid,
            idempotency_key=key,
        )
        return tid

    results = await asyncio.gather(*[try_ingest(i) for i in range(20)])
    winners = [r for r in results if r]
    assert len(winners) == 1
    tid = winners[0]
    async with event_collector(redis_client) as events:
        async with background_worker(redis_client):
            await wait_for(
                status_is(redis_client, tid, "succeeded"),
                what="idempotent task succeeded",
            )
            await asyncio.sleep(0.5)
    assert int(await redis_client.get("dtq:example:counter:idem-race")) == 1
    assert len(events.of_type("TASK_STARTED", tid)) == 1


async def test_two_schedulers_requeue_retry_exactly_once(redis_client: Redis) -> None:
    async with event_collector(redis_client) as events:
        tid = await submit_task(redis_client, task_type="echo_task", payload={})
        # Walk the contract's transition path the way the executor would:
        # QUEUED -> RUNNING (claim) -> RETRYING (retryable failure recorded).
        await update_task(redis_client, tid, TaskStatus.RUNNING, 604800)
        await update_task(redis_client, tid, TaskStatus.RETRYING, 604800)
        now_ms = int(time.time() * 1000)
        await redis_client.zadd(RETRY_SCHEDULE, {tid: now_ms - 1000})
        settings = make_test_settings()
        extra = make_redis()
        try:
            w1 = Worker(settings, queues=["default"], redis_client=redis_client)
            w2 = Worker(settings, queues=["default"], redis_client=extra)
            await asyncio.gather(w1._scheduler_pass(), w2._scheduler_pass())
        finally:
            await extra.aclose()
    # Exactly one requeued entry (attempt 2). The original attempt-1 entry
    # from submit_task stays in the stream history, like an ACKed entry
    # would; the scheduler must add exactly one new entry.
    entries = await redis_client.xrange(stream_name("default", 5), "-", "+")
    mine = [dict(e[1]) for e in entries if dict(e[1]).get("task_id") == tid]
    requeued = [f for f in mine if f.get("attempt") == "2"]
    assert len(requeued) == 1
    assert await redis_client.zscore(RETRY_SCHEDULE, tid) is None
    assert await redis_client.zscore(RETRY_CLAIMED, tid) is None
    raw = await _hash(redis_client, tid)
    assert raw["status"] == "queued"
    assert int(raw["attempt"]) == 2
    retried = events.of_type("TASK_RETRIED", tid)
    assert len(retried) == 1


async def test_cancel_while_running_is_deterministic(redis_client: Redis) -> None:
    async with event_collector(redis_client) as events:
        tid = await submit_task(
            redis_client,
            task_type="sleep_task",
            payload={"seconds": 30},
            timeout_ms=60000,
        )
        async with background_worker(redis_client):
            await wait_for(
                status_is(redis_client, tid, "running"),
                what="task running",
            )
            await await_redis(redis_client.sadd(CANCELLED_SET, tid))
            await await_redis(redis_client.hset(task_key(tid), "cancel_requested", "1"))
            await wait_for(
                status_is(redis_client, tid, "cancelled"),
                timeout=15,
                what="task cancelled mid-run",
            )
            # Settles exactly once: still cancelled after a beat, never succeeded.
            await asyncio.sleep(1.0)
            assert await task_status(redis_client, tid) == "cancelled"
    assert events.of_type("TASK_SUCCEEDED", tid) == []
    cancelled = events.of_type("TASK_CANCELLED", tid)
    assert len(cancelled) == 1
    assert await total_pending(redis_client) == 0


async def test_sigterm_drain_completes_inflight_task(redis_client: Redis) -> None:
    tid = await submit_task(
        redis_client,
        task_type="sleep_task",
        payload={"seconds": 1},
        timeout_ms=30000,
    )
    settings = make_test_settings(drain_timeout_s=10)
    worker = Worker(settings, queues=["default"], redis_client=redis_client)
    run_task = asyncio.create_task(worker.run())
    try:
        await wait_for(
            status_is(redis_client, tid, "running"),
            what="task running",
        )
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(run_task, timeout=30)
    finally:
        worker.initiate_shutdown()
    assert await task_status(redis_client, tid) == "succeeded"
    assert await total_pending(redis_client) == 0


async def test_sigterm_never_acks_unexecuted_entry(redis_client: Redis) -> None:
    tid = await submit_task(
        redis_client,
        task_type="sleep_task",
        payload={"seconds": 30},
        timeout_ms=120000,
    )
    settings = make_test_settings(drain_timeout_s=2)
    worker = Worker(settings, queues=["default"], redis_client=redis_client)
    run_task = asyncio.create_task(worker.run())
    try:
        await wait_for(
            status_is(redis_client, tid, "running"),
            what="task running",
        )
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(run_task, timeout=30)
    finally:
        worker.initiate_shutdown()
    # Not acknowledged, not terminal: still RUNNING, still in the PEL.
    assert await task_status(redis_client, tid) == "running"
    assert await total_pending(redis_client) == 1
    # A peer reclaims it after the visibility timeout; cancel it so the
    # outcome is deterministic and fast.
    await await_redis(redis_client.sadd(CANCELLED_SET, tid))
    settings2 = make_test_settings(visibility_timeout_s=2, heartbeat_interval_s=1)
    async with background_worker(redis_client, settings2):
        await wait_for(
            status_is(redis_client, tid, "cancelled"),
            timeout=60,
            what="entry reclaimed by a peer",
        )
    assert await total_pending(redis_client) == 0
