"""Worker integration tests: claim pipeline against real Redis (db 3).

Raw ingest mirrors the API contract, so these tests do not depend on the
API builder's ingest path.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from dtq_core.keys import CANCELLED_SET, task_key
from dtq_queue.streams import stream_entry_fields, xadd_task
from dtq_queue.task_state import get_task
from dtq_worker import metrics as worker_metrics
from dtq_worker.redis_client import await_redis
from redis.asyncio import Redis

import examples.tasks  # noqa: F401  (registers demo handlers)
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


async def test_success_path(redis_client: Redis) -> None:
    before = worker_metrics.tasks_succeeded_total.labels(
        queue="default", task_type="echo_task"
    )._value.get()
    async with event_collector(redis_client) as events:
        tid = await submit_task(redis_client, task_type="echo_task", payload={"hello": "world"})
        async with background_worker(redis_client):
            await wait_for(
                status_is(redis_client, tid, "succeeded"),
                what="echo task succeeded",
            )
    raw = await _hash(redis_client, tid)
    assert raw["status"] == "succeeded"
    assert int(raw["attempt"]) == 1
    assert int(raw["duration_ms"]) >= 0
    assert raw["worker_id"].startswith("worker-")
    result = json.loads(await redis_client.get(f"dtq:result:{tid}"))
    assert result["echo"] == {"hello": "world"}
    started = events.of_type("TASK_STARTED", tid)
    assert len(started) == 1
    succeeded = events.of_type("TASK_SUCCEEDED", tid)
    assert len(succeeded) == 1
    after = worker_metrics.tasks_succeeded_total.labels(
        queue="default", task_type="echo_task"
    )._value.get()
    assert after == before + 1
    assert await total_pending(redis_client) == 0


async def test_flaky_retries_then_succeeds(redis_client: Redis) -> None:
    async with event_collector(redis_client) as events:
        tid = await submit_task(
            redis_client,
            task_type="flaky_task",
            payload={"fail_times": 2},
            max_attempts=5,
        )
        async with background_worker(redis_client):
            await wait_for(
                status_is(redis_client, tid, "succeeded"),
                timeout=60,
                what="flaky task succeeded after retries",
            )
    raw = await _hash(redis_client, tid)
    assert int(raw["attempt"]) == 3
    retried = events.of_type("TASK_RETRIED", tid)
    assert len(retried) == 2
    scheduled = events.of_type("TASK_RETRY_SCHEDULED", tid)
    assert len(scheduled) == 2
    assert await total_pending(redis_client) == 0


async def test_always_fail_goes_to_dlq(redis_client: Redis) -> None:
    async with event_collector(redis_client) as events:
        tid = await submit_task(
            redis_client, task_type="always_fail_task", payload={}, max_attempts=3
        )
        async with background_worker(redis_client):
            await wait_for(
                status_is(redis_client, tid, "dead_lettered"),
                timeout=60,
                what="always-fail task dead-lettered",
            )
    raw = await _hash(redis_client, tid)
    assert raw["status"] == "dead_lettered"
    assert int(raw["attempt"]) == 3
    assert raw["error_class"] == "RetryableError"
    dlq_raw = await redis_client.xrevrange("dtq:stream:dead-letter", max="+", min="-", count=20)
    mine = [dict(f) for _, f in dlq_raw if dict(f).get("task_id") == tid]
    assert len(mine) == 1
    assert mine[0]["attempts_made"] == "3"
    assert "always-fail" in mine[0]["last_error"]
    assert mine[0]["original_queue"] == "default"
    dead = events.of_type("TASK_DEAD_LETTERED", tid)
    assert len(dead) == 1
    assert await total_pending(redis_client) == 0


async def test_unknown_task_type_goes_to_dlq_and_worker_survives(redis_client: Redis) -> None:
    async with event_collector(redis_client) as events:
        tid = await submit_task(redis_client, task_type="nope_not_registered", payload={"x": 1})
        async with background_worker(redis_client):
            await wait_for(
                status_is(redis_client, tid, "dead_lettered"),
                what="unknown task type dead-lettered",
            )
            # The worker is still healthy: a well-formed task runs to completion.
            tid2 = await submit_task(redis_client, task_type="echo_task", payload={"ok": True})
            await wait_for(
                status_is(redis_client, tid2, "succeeded"),
                what="worker survived unknown task type",
            )
    raw = await _hash(redis_client, tid)
    assert raw["status"] == "dead_lettered"
    assert raw["error_class"] == "UnknownTaskType"
    assert raw["error_message"] == "unknown_task_type:nope_not_registered"
    assert int(raw["attempt"]) == 1
    started = events.of_type("TASK_STARTED", tid)
    assert started == []


async def test_sleep_exceeding_timeout_is_retried_then_exhausted(redis_client: Redis) -> None:
    async with event_collector(redis_client) as events:
        tid = await submit_task(
            redis_client,
            task_type="sleep_task",
            payload={"seconds": 30},
            timeout_ms=800,
            max_attempts=2,
        )
        async with background_worker(redis_client):
            await wait_for(
                status_is(redis_client, tid, "dead_lettered"),
                timeout=60,
                what="timed-out task exhausted into the DLQ",
            )
    raw = await _hash(redis_client, tid)
    assert raw["error_class"] == "TimeoutError"
    assert int(raw["attempt"]) == 2
    scheduled = events.of_type("TASK_RETRY_SCHEDULED", tid)
    assert len(scheduled) == 1
    assert await total_pending(redis_client) == 0


async def test_cancelled_queued_task_never_executes(redis_client: Redis) -> None:
    async with event_collector(redis_client) as events:
        tid = await submit_task(redis_client, task_type="sleep_task", payload={"seconds": 5})
        await await_redis(redis_client.sadd(CANCELLED_SET, tid))
        async with background_worker(redis_client):
            await wait_for(
                status_is(redis_client, tid, "cancelled"),
                what="queued task cancelled",
            )
    started = events.of_type("TASK_STARTED", tid)
    assert started == []
    cancelled = events.of_type("TASK_CANCELLED", tid)
    assert len(cancelled) == 1
    assert await total_pending(redis_client) == 0


async def test_expired_ttl_task(redis_client: Redis) -> None:
    async with event_collector(redis_client) as events:
        tid = await submit_task(
            redis_client,
            task_type="echo_task",
            payload={},
            metadata={"ttl_seconds": 0},
        )
        async with background_worker(redis_client):
            await wait_for(
                status_is(redis_client, tid, "expired"),
                what="expired task",
            )
    started = events.of_type("TASK_STARTED", tid)
    assert started == []
    expired = events.of_type("TASK_EXPIRED", tid)
    assert len(expired) == 1
    assert await total_pending(redis_client) == 0


async def test_duplicate_entries_do_not_double_execute(redis_client: Redis) -> None:
    async with event_collector(redis_client) as events:
        tid = await submit_task(
            redis_client, task_type="counter_task", payload={"counter": "dup-test"}
        )
        # A second, identical stream entry for the same task (duplicate delivery).
        task = await get_task(redis_client, tid)
        assert task is not None
        from dtq_core.keys import stream_name

        await xadd_task(redis_client, stream_name("default", 5), stream_entry_fields(task))
        async with background_worker(redis_client):
            await wait_for(
                status_is(redis_client, tid, "succeeded"),
                what="task succeeded once",
            )
            # Give the worker a beat: if duplicate absorption were broken, the
            # second entry would double-increment the counter here.
            await asyncio.sleep(1.0)
    assert int(await redis_client.get("dtq:example:counter:dup-test")) == 1
    started = events.of_type("TASK_STARTED", tid)
    assert len(started) == 1
    assert await total_pending(redis_client) == 0


async def test_delayed_task_stays_pending_until_due(redis_client: Redis) -> None:
    tid = await submit_task(
        redis_client,
        task_type="echo_task",
        payload={"hello": "later"},
        delay_seconds=3,
    )
    assert await task_status(redis_client, tid) == "pending"
    async with background_worker(redis_client):
        # Not yet due: the scheduler must not promote it early.
        await asyncio.sleep(1.5)
        assert await task_status(redis_client, tid) == "pending"
        await wait_for(
            status_is(redis_client, tid, "succeeded"),
            timeout=30,
            what="delayed task succeeded after due",
        )
    assert await total_pending(redis_client) == 0


async def test_worker_id_setting_is_honored(redis_client: Redis) -> None:
    tid = await submit_task(redis_client, task_type="echo_task", payload={"hello": "named"})
    settings = make_test_settings()
    settings.worker_id = "test-worker-7"
    async with background_worker(redis_client, settings):
        await wait_for(
            status_is(redis_client, tid, "succeeded"),
            what="task succeeded",
        )
    raw = await _hash(redis_client, tid)
    assert raw["worker_id"] == "test-worker-7"


@pytest.mark.parametrize("priority", [0, 5, 9])
async def test_priorities_all_consumed(redis_client: Redis, priority: int) -> None:
    tid = await submit_task(
        redis_client,
        task_type="echo_task",
        payload={"p": priority},
        priority=priority,
    )
    async with background_worker(redis_client):
        await wait_for(
            status_is(redis_client, tid, "succeeded"),
            what=f"priority {priority} task succeeded",
        )
    assert await total_pending(redis_client) == 0
