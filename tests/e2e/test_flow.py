"""End-to-end flow tests: full submit-to-result flow (real Redis, db 3)."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import signal
import sys
from pathlib import Path
from typing import Any

from dtq_core.keys import WORKERS_SET
from dtq_worker.registry import TaskContext, task
from redis.asyncio import Redis

import examples.tasks  # noqa: F401  (registers demo handlers)
from examples.tasks._redis import sync_redis
from tests.fixtures.worker_helpers import (
    TEST_DB,
    background_worker,
    event_collector,
    make_test_settings,
    status_is,
    submit_task,
    total_pending,
    wait_for,
)

pytest_plugins = "tests.fixtures.redis"

REPO_ROOT = str(Path(__file__).resolve().parents[2])


@task("e2e_crash_task")
async def e2e_crash_task(payload: dict[str, Any], ctx: TaskContext) -> dict[str, Any]:
    """Increment a counter, then sleep 30s on attempt 1 only.

    The SIGKILL test lands mid-sleep; a peer worker reclaims the entry and
    attempt 2 completes immediately.
    """
    count_raw: Any = sync_redis().incr("dtq:e2e:crash-counter")
    count = int(count_raw)
    if ctx.attempt == 1:
        await asyncio.sleep(30)
    return {"count": count, "attempt": ctx.attempt}


def _child_main(repo_root: str, db: int) -> None:
    """Spawn target: run a worker process until killed.

    Module-level so multiprocessing can pickle it; the child re-imports this
    module, which registers e2e_crash_task in the child's registry too.
    """
    import os

    sys.path.insert(0, repo_root)
    os.environ["DTQ_TEST_REDIS_DB"] = str(db)
    os.environ["DTQ_LOG_LEVEL"] = "WARNING"

    async def _amain() -> None:
        from dtq_config.settings import Settings
        from dtq_worker.worker import Worker

        settings = Settings(
            redis_url=f"redis://localhost:6379/{db}",
            worker_concurrency=2,
            visibility_timeout_s=30,
            heartbeat_interval_s=5,
            log_level="WARNING",
        )
        await Worker(settings, queues=["default"]).run()

    asyncio.run(_amain())


async def _counter_at_least(redis_client: Redis, key: str, minimum: int) -> bool:
    pending: Any = redis_client.get(key)
    raw: Any = await pending
    return bool(raw is not None and int(raw) >= minimum)


async def test_full_flow_submit_to_succeeded(redis_client: Redis) -> None:
    async with event_collector(redis_client) as events:
        tid = await submit_task(
            redis_client,
            task_type="fibonacci_task",
            payload={"n": 20},
            idempotency_key="e2e-fib-1",
            max_attempts=3,
        )
        async with background_worker(redis_client):
            await wait_for(
                status_is(redis_client, tid, "succeeded"),
                what="e2e fibonacci task succeeded",
            )
    result = json.loads(await redis_client.get(f"dtq:result:{tid}"))
    assert result["fib"] == 6765
    assert result["n"] == 20
    # The full lifecycle event sequence is present and ordered.
    captured = events.all(task_id=tid)
    types = [e["type"] for e in reversed(captured)]
    for expected in (
        "TASK_QUEUED",
        "TASK_STARTED",
        "TASK_SUCCEEDED",
    ):
        assert expected in types, types
    assert types.index("TASK_QUEUED") < types.index("TASK_STARTED") < types.index("TASK_SUCCEEDED")
    assert await total_pending(redis_client) == 0


async def test_sigkill_mid_execution_reclaimed_by_peer(redis_client: Redis) -> None:
    ctx = multiprocessing.get_context("spawn")
    proc = ctx.Process(target=_child_main, args=(REPO_ROOT, TEST_DB), daemon=True)
    proc.start()
    try:
        # Wait until the child worker registers itself.
        async def _alive() -> bool:
            pending: Any = redis_client.scard(WORKERS_SET)
            return bool((await pending) >= 1)

        await wait_for(_alive, timeout=60, what="child worker alive")
        async with event_collector(redis_client) as events:
            tid = await submit_task(
                redis_client,
                task_type="e2e_crash_task",
                payload={},
                timeout_ms=120000,
                max_attempts=5,
            )
            # Wait until attempt 1 incremented the counter (handler is mid-sleep).
            await wait_for(
                lambda: _counter_at_least(redis_client, "dtq:e2e:crash-counter", 1),
                timeout=30,
                what="first attempt started",
            )
            proc.kill()  # SIGKILL: no cleanup, entry stays pending
            await asyncio.to_thread(proc.join, 30)
            assert proc.exitcode == -signal.SIGKILL
            # A peer worker reclaims the entry and completes attempt 2.
            settings2 = make_test_settings(visibility_timeout_s=2, heartbeat_interval_s=1)
            async with background_worker(redis_client, settings2):
                await wait_for(
                    status_is(redis_client, tid, "succeeded"),
                    timeout=60,
                    what="entry reclaimed and completed",
                )
    finally:
        if proc.is_alive():
            proc.kill()
            proc.join(10)
    # Exactly two handler executions: attempt 1 (killed) and attempt 2.
    counter_raw: Any = redis_client.get("dtq:e2e:crash-counter")
    assert int(await counter_raw) == 2
    succeeded = events.of_type("TASK_SUCCEEDED", tid)
    assert len(succeeded) == 1
    assert await total_pending(redis_client) == 0
