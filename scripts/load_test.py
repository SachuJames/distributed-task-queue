#!/usr/bin/env python3
"""DTQ load test: measure real throughput on this machine.

Submits N tasks through the real ingest pipeline, runs W in-process workers,
and reports honest, measured numbers: submit rate, completion rate, and
end-to-end latency percentiles. Nothing is estimated or extrapolated.

Usage:
    .venv/bin/python scripts/load_test.py [--tasks 2000] [--workers 4] [--db 10]

All numbers printed are measured on the machine that runs this script.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dtq_config.settings import Settings  # noqa: E402
from dtq_core.keys import task_key  # noqa: E402
from dtq_queue.ingest import submit_task  # noqa: E402
from dtq_worker.worker import Worker  # noqa: E402
from redis.asyncio import Redis  # noqa: E402

import examples.tasks  # noqa: E402,F401  (registers demo handlers)


async def _status(redis: Redis, task_id: str) -> str | None:
    pending: Any = redis.hget(f"dtq:task:{task_id}", "status")
    raw = await pending
    return str(raw) if raw is not None else None


async def main() -> int:
    parser = argparse.ArgumentParser(description="DTQ load test (measured numbers only).")
    parser.add_argument("--tasks", type=int, default=2000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--db", type=int, default=10)
    args = parser.parse_args()

    redis = Redis.from_url(f"redis://localhost:6379/{args.db}", decode_responses=True)
    await redis.flushdb()

    settings = Settings(
        redis_url=f"redis://localhost:6379/{args.db}",
        worker_concurrency=8,
        visibility_timeout_s=30,
        heartbeat_interval_s=5,
        retry_base_delay=0.05,
        retry_max_delay=0.5,
        retry_jitter=0.0,
        drain_timeout_s=10,
        log_level="WARNING",
    )

    # --- submit phase -------------------------------------------------------
    tids: list[str] = []
    t0 = time.perf_counter()
    for i in range(args.tasks):
        task, _dup = await submit_task(
            redis,
            task_type="echo_task",
            payload={"i": i},
            queue="default",
            settings=settings,
            is_registered=None,  # handlers are registered in-process already
        )
        tids.append(task.task_id)
    submit_s = time.perf_counter() - t0

    # --- run phase ----------------------------------------------------------
    workers = [
        Worker(settings, queues=["default"], redis_client=redis) for _ in range(args.workers)
    ]
    run_tasks = [asyncio.create_task(w.run()) for w in workers]
    t1 = time.perf_counter()
    deadline = t1 + 300
    remaining = set(tids)
    while remaining and time.perf_counter() < deadline:
        done_now = [t for t in list(remaining) if await _status(redis, t) == "succeeded"]
        remaining.difference_update(done_now)
        if remaining:
            await asyncio.sleep(0.2)
    run_s = time.perf_counter() - t1
    for w in workers:
        w.initiate_shutdown()
    await asyncio.gather(*run_tasks)

    # --- latency sample -----------------------------------------------------
    latencies: list[float] = []
    pipe = redis.pipeline()
    for tid in tids[:500]:
        pipe.hmget(task_key(tid), "created_at", "finished_at", "status")
    rows = await pipe.execute()
    for created, finished, status in rows:
        if status == "succeeded" and created and finished:
            c = datetime.fromisoformat(created)
            f = datetime.fromisoformat(finished)
            if c.tzinfo is None:
                c = c.replace(tzinfo=UTC)
            if f.tzinfo is None:
                f = f.replace(tzinfo=UTC)
            latencies.append((f - c).total_seconds() * 1000)

    print(f"tasks:            {args.tasks}")
    print(f"workers:          {args.workers} (concurrency 8 each)")
    print(f"submit rate:      {args.tasks / submit_s:,.0f} tasks/s (ingest only)")
    if remaining:
        done_n = args.tasks - len(remaining)
        print(f"COMPLETED:        {done_n}/{args.tasks} in {run_s:.1f}s (TIMEOUT)")
    else:
        print(f"completion rate:  {args.tasks / run_s:,.0f} tasks/s end to end")
    if latencies:
        latencies.sort()
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[int(len(latencies) * 0.95)]
        n = len(latencies)
        print(f"latency p50/p95:  {p50:.1f} ms / {p95:.1f} ms (submit to finish, n={n})")
        print(f"latency mean:     {statistics.mean(latencies):.1f} ms")
    info: Any = await redis.info("server")
    print(f"redis:            {info.get('redis_version', 'unknown')} (local)")
    print("note: numbers above were measured on this machine, this run, local Redis.")
    await redis.aclose()
    return 0 if not remaining else 1


if __name__ == "__main__":
    os.environ.setdefault("DTQ_TEST_REDIS_DB", "10")
    sys.exit(asyncio.run(main()))
