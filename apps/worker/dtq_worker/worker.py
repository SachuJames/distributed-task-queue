"""The DTQ worker: poll, reaper, scheduler and heartbeat loops.

Contract section 6. Loops run with jitter and are supervised: a crashing
loop is logged and restarted, never taking the worker down. Shutdown on
SIGTERM/SIGINT drains in-flight executions for DTQ_DRAIN_TIMEOUT_S, then
exits without ACKing unexecuted work; unacked entries are reclaimed by
peers via XAUTOCLAIM.
"""

from __future__ import annotations

import asyncio
import logging
import random
import signal
import socket
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from datetime import UTC, datetime, timedelta

from dtq_config.settings import Settings
from dtq_core.keys import (
    CANCELLED_SET,
    CONSUMER_GROUP,
    RETRY_CLAIMED,
    SCHEDULER_LOCK,
    WORKERS_SET,
    pause_key,
    stream_name,
    worker_key,
)
from dtq_core.models import QUEUE_NAME_RE, Task, TaskStatus, is_terminal, transition
from dtq_queue.lua import claim_due, requeue_stale
from dtq_queue.streams import (
    ensure_consumer_groups,
    stream_entry_fields,
    xadd_task,
    xreadgroup_priority,
)
from dtq_queue.task_state import (
    get_task,
    record_attempt_end,
    save_task,
    update_task,
)
from redis.asyncio import Redis

from . import events
from . import metrics as m
from .events import WORKER_STARTED, WORKER_STOPPED
from .executor import (
    OUTCOME_CANCELLED,
    OUTCOME_DEAD_LETTERED,
    OUTCOME_FAILED,
    OUTCOME_RETRY_SCHEDULED,
    OUTCOME_SUCCEEDED,
    ExecContext,
    StreamEntry,
    execute_entry,
)
from .idem import complete_idem
from .redis_client import await_redis, make_redis

logger = logging.getLogger("dtq.worker")

#: Scheduler pass interval (s) before jitter.
_SCHEDULER_INTERVAL_S = 1.0
#: Scheduler batch cap (contract section 7: retry pressure).
_SCHEDULER_BATCH = 100
#: Stale-claim age before the reaper returns a claim to the schedule (ms).
_CLAIM_STALE_MS = 60_000
#: Leader lock TTL (ms); refreshed while leading.
_LOCK_TTL_MS = 10_000


def _jitter(interval_s: float, amount: float = 0.2) -> float:
    # S311: jitter needs uniform spread, not cryptographic randomness.
    return interval_s * random.uniform(1.0 - amount, 1.0 + amount)  # noqa: S311


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


class Worker:
    """A worker process: claims stream entries and executes handlers."""

    def __init__(
        self,
        settings: Settings,
        queues: Sequence[str] | None = None,
        redis_client: Redis | None = None,
    ) -> None:
        self.settings = settings
        self.queues = list(queues) if queues else ["default"]
        for q in self.queues:
            if not QUEUE_NAME_RE.match(q):
                raise ValueError(f"invalid queue name: {q!r}")
        self.worker_id = settings.worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.redis: Redis = redis_client or make_redis(settings)
        self._owns_redis = redis_client is None

        self._sema = asyncio.Semaphore(settings.worker_concurrency)
        # asyncio.Task -> (stream key, entry id) for every launched execution.
        self._in_flight: dict[asyncio.Task[None], tuple[str, str]] = {}
        self._active_count = 0
        self._exec_ctx = ExecContext(worker_id=self.worker_id, settings=settings)

        self._shutdown_event = asyncio.Event()
        self._shutdown_requested = False
        self._started_at = _utcnow_iso()

        self._tasks_processed = 0
        self._tasks_failed = 0
        self._tasks_retried = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def initiate_shutdown(self) -> None:
        """Begin graceful shutdown (safe to call from a signal handler)."""
        if self._shutdown_requested:
            return
        logger.info("shutdown requested; draining", extra={"worker_id": self.worker_id})
        self._shutdown_requested = True
        self._shutdown_event.set()

    async def run(self) -> None:
        """Run all loops until SIGTERM/SIGINT, then drain and exit."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.initiate_shutdown)
            except (RuntimeError, ValueError, OSError):
                # Not the main thread or handlers unsupported; shutdown can
                # still be triggered via initiate_shutdown().
                logger.debug("could not install handler for %s", sig)
        try:
            await self._startup()
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._supervise(self._poll_loop, "poll"))
                tg.create_task(self._supervise(self._reaper_loop, "reaper"))
                tg.create_task(self._supervise(self._scheduler_loop, "scheduler"))
                tg.create_task(self._supervise(self._heartbeat_loop, "heartbeat"))
                await self._shutdown_event.wait()
        finally:
            for sig in (signal.SIGTERM, signal.SIGINT):
                with suppress(RuntimeError, ValueError, OSError):
                    loop.remove_signal_handler(sig)
        await self._drain_and_close()

    async def _startup(self) -> None:
        await self.redis.ping()
        await ensure_consumer_groups(self.redis, self.queues)
        m.workers_active.set(1)
        await self._heartbeat_once(status="starting")
        await events.publish_event(
            self.redis,
            WORKER_STARTED,
            worker_id=self.worker_id,
            metadata={
                "hostname": socket.gethostname(),
                "concurrency": self.settings.worker_concurrency,
                "queues": list(self.queues),
            },
        )
        logger.info(
            "worker started",
            extra={"worker_id": self.worker_id, "service": "dtq-worker"},
        )

    async def _drain_and_close(self) -> None:
        await self._heartbeat_once(status="draining")
        try:
            await asyncio.wait_for(
                self._wait_for_in_flight(), timeout=self.settings.drain_timeout_s
            )
        except TimeoutError:
            logger.warning(
                "drain timeout with %d in-flight; leaving entries unacked",
                len(self._in_flight),
                extra={"worker_id": self.worker_id},
            )
            for t in list(self._in_flight):
                t.cancel()
            if self._in_flight:
                await asyncio.gather(*list(self._in_flight), return_exceptions=True)
        await events.publish_event(self.redis, WORKER_STOPPED, worker_id=self.worker_id)
        m.workers_active.set(0)
        m.worker_inflight.labels(worker_id=self.worker_id).set(0)
        await self._heartbeat_once(status="stopped")
        if self._owns_redis:
            await self.redis.aclose()
        logger.info("worker stopped", extra={"worker_id": self.worker_id})

    async def _wait_for_in_flight(self) -> None:
        deadline = time.monotonic() + self.settings.drain_timeout_s
        pending = set(self._in_flight)
        while pending:
            timeout = deadline - time.monotonic()
            if timeout <= 0:
                return
            _done, pending = await asyncio.wait(pending, timeout=timeout)

    async def _supervise(self, coro_fn: Callable[[], Awaitable[None]], name: str) -> None:
        """Run a loop forever, restarting it after crashes until shutdown."""
        while not self._shutdown_requested:
            try:
                await coro_fn()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "worker loop %s crashed; restarting",
                    name,
                    extra={"worker_id": self.worker_id},
                )
            if self._shutdown_requested:
                break
            # S311: restart backoff needs uniform spread, not cryptographic randomness.
            await asyncio.sleep(random.uniform(0.5, 1.5))  # noqa: S311

    async def _idle_wait(self, delay_s: float) -> None:
        """Sleep between loop iterations, waking early on shutdown."""
        with suppress(TimeoutError):
            await asyncio.wait_for(self._shutdown_event.wait(), timeout=delay_s)

    # ------------------------------------------------------------------
    # Execution fan-out (bounded by the semaphore; never unbounded).
    # ------------------------------------------------------------------

    def _launch(self, stream: str, entry_id: str, fields: dict[str, str], reclaimed: bool) -> None:
        if self._shutdown_requested:
            return
        coro = self._guarded_execute(stream, entry_id, fields, reclaimed)
        task = asyncio.ensure_future(coro)
        self._in_flight[task] = (stream, entry_id)
        task.add_done_callback(lambda t: self._in_flight.pop(t, None))

    async def _guarded_execute(
        self, stream: str, entry_id: str, fields: dict[str, str], reclaimed: bool
    ) -> None:
        async with self._sema:
            self._active_count += 1
            try:
                outcome = await execute_entry(
                    self.redis,
                    stream,
                    StreamEntry(entry_id=entry_id, fields=fields),
                    self._exec_ctx,
                    reclaimed=reclaimed,
                )
            except asyncio.CancelledError:
                # Shutdown path: never ACK unexecuted work; the entry stays
                # pending and is reclaimed by a peer.
                raise
            except Exception:
                logger.exception(
                    "execute_entry crashed; entry left unacked",
                    extra={"worker_id": self.worker_id},
                )
                outcome = "internal_error"
            finally:
                self._active_count -= 1
            self._note_outcome(fields.get("task_type", ""), outcome)

    def _note_outcome(self, task_type: str, outcome: str) -> None:
        if outcome in (
            OUTCOME_SUCCEEDED,
            OUTCOME_RETRY_SCHEDULED,
            OUTCOME_DEAD_LETTERED,
            OUTCOME_FAILED,
            OUTCOME_CANCELLED,
        ):
            self._tasks_processed += 1
        if outcome in (OUTCOME_DEAD_LETTERED, OUTCOME_FAILED):
            self._tasks_failed += 1
        if outcome == OUTCOME_RETRY_SCHEDULED:
            self._tasks_retried += 1

    # ------------------------------------------------------------------
    # Poll loop: strict priority p9 -> p0, prefetch bounded by concurrency.
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        while not self._shutdown_requested:
            try:
                launched = await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("poll failed", extra={"worker_id": self.worker_id})
                await asyncio.sleep(1.0)
                continue
            if launched == 0:
                # xreadgroup_priority blocks up to 500ms across all bands while idle.
                await self._idle_wait(0.05)

    async def _poll_once(self) -> int:
        capacity = self.settings.worker_concurrency - len(self._in_flight)
        if capacity <= 0:
            await asyncio.sleep(0.05)
            return 0
        launched = 0
        for index, queue in enumerate(self.queues):
            if self._shutdown_requested:
                break
            remaining = capacity - launched
            if remaining <= 0:
                break
            if await self._is_paused(queue):
                continue
            block_ms = 500 if (index == 0 and launched == 0) else 0
            messages = await xreadgroup_priority(
                self.redis,
                queue,
                CONSUMER_GROUP,
                self.worker_id,
                min(remaining, 32),
                block_ms=block_ms,
            )
            for stream, entry_id, fields in messages:
                self._launch(stream, entry_id, fields, reclaimed=False)
                launched += 1
        return launched

    async def _is_paused(self, queue: str) -> bool:
        flag = await await_redis(self.redis.get(pause_key(queue)))
        return str(flag) == "1"

    # ------------------------------------------------------------------
    # Reaper loop: XAUTOCLAIM idle entries (crash redelivery).
    # ------------------------------------------------------------------

    async def _reaper_loop(self) -> None:
        interval = max(0.5, self.settings.visibility_timeout_s / 2.0)
        while not self._shutdown_requested:
            try:
                await self._reaper_pass()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("reaper failed", extra={"worker_id": self.worker_id})
            await self._idle_wait(_jitter(interval))

    async def _reaper_pass(self) -> None:
        min_idle_ms = self.settings.visibility_timeout_s * 1000
        budget = self.settings.worker_concurrency * 2  # bound reaper intake
        for queue in self.queues:
            for priority in range(9, -1, -1):
                if budget <= 0 or self._shutdown_requested:
                    return
                stream = stream_name(queue, priority)
                cursor = "0-0"
                while budget > 0:
                    cursor, messages = await self._xautoclaim_page(
                        stream, cursor, min_idle_ms, min(32, budget)
                    )
                    for entry_id, fields in messages:
                        self._launch(stream, entry_id, fields, reclaimed=True)
                        budget -= 1
                    if cursor == "0-0":
                        break

    async def _xautoclaim_page(
        self, stream: str, cursor: str, min_idle_ms: int, count: int
    ) -> tuple[str, list[tuple[str, dict[str, str]]]]:
        raw: object = await self.redis.xautoclaim(
            stream,
            CONSUMER_GROUP,
            self.worker_id,
            min_idle_ms,
            start_id=cursor,
            count=count,
        )
        items = list(raw) if isinstance(raw, list | tuple) else []
        next_cursor = str(items[0]) if items else "0-0"
        blobs = items[1] if len(items) > 1 else []
        messages: list[tuple[str, dict[str, str]]] = []
        for entry_id_blob, fields_blob in blobs or []:
            fields = {str(k): str(v) for k, v in dict(fields_blob).items()}
            messages.append((str(entry_id_blob), fields))
        return next_cursor, messages

    # ------------------------------------------------------------------
    # Scheduler loop: leader claims due retries/delayed tasks and requeues
    # them; stale claims are returned to the schedule.
    # ------------------------------------------------------------------

    async def _scheduler_loop(self) -> None:
        while not self._shutdown_requested:
            try:
                await self._scheduler_pass()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("scheduler failed", extra={"worker_id": self.worker_id})
            await self._idle_wait(_jitter(_SCHEDULER_INTERVAL_S))

    async def _ensure_leader(self) -> bool:
        token = self.worker_id
        acquired: object = await self.redis.set(SCHEDULER_LOCK, token, nx=True, px=_LOCK_TTL_MS)
        if acquired:
            return True
        current: object = await self.redis.get(SCHEDULER_LOCK)
        if current is not None and str(current) == token:
            await self.redis.pexpire(SCHEDULER_LOCK, _LOCK_TTL_MS)
            return True
        return False

    async def _scheduler_pass(self) -> None:
        """One scheduler iteration. Exposed for tests (two schedulers racing)."""
        if not await self._ensure_leader():
            return
        now_ms = int(time.time() * 1000)
        due = await claim_due(self.redis, now_ms, _SCHEDULER_BATCH)
        for task_id in due:
            try:
                await self._requeue_claimed(task_id, now_ms)
            except Exception:
                logger.exception(
                    "requeue failed for task",
                    extra={"worker_id": self.worker_id, "task_id": task_id},
                )
        stale = await requeue_stale(self.redis, now_ms - _CLAIM_STALE_MS, now_ms, _SCHEDULER_BATCH)
        if stale:
            logger.info(
                "returned %d stale claims to the schedule",
                len(stale),
                extra={"worker_id": self.worker_id},
            )

    async def _requeue_claimed(self, task_id: str, now_ms: int) -> None:
        """Move one claimed task back into its priority stream."""
        retention = self.settings.task_retention_s
        task = await get_task(self.redis, task_id)
        if task is None or is_terminal(task.status):
            await self.redis.zrem(RETRY_CLAIMED, task_id)
            return
        # Cancelled while waiting?
        if await await_redis(self.redis.sismember(CANCELLED_SET, task_id)):
            await record_attempt_end(self.redis, task_id, retention)
            if task.status is not TaskStatus.CANCELLED:
                try:
                    await update_task(self.redis, task_id, TaskStatus.CANCELLED, retention)
                except Exception:
                    logger.warning("cancel race on claimed task", extra={"task_id": task_id})
            await await_redis(self.redis.srem(CANCELLED_SET, task_id))
            await await_redis(self.redis.zrem(RETRY_CLAIMED, task_id))
            await events.publish_event(
                self.redis,
                events.TASK_CANCELLED,
                task_id=task_id,
                queue=task.queue,
                task_type=task.task_type,
                attempt=task.attempt,
                worker_id=self.worker_id,
                metadata={"reason": "cancelled_while_scheduled"},
            )
            await complete_idem(
                self.redis,
                task.queue,
                task.idempotency_key or "",
                "failed",
                task_id,
                self.settings.idem_ttl_s,
            )
            return
        # TTL expiry while waiting.
        if self._expired(task):
            await record_attempt_end(self.redis, task_id, retention)
            try:
                await update_task(self.redis, task_id, TaskStatus.EXPIRED, retention)
            except Exception:
                logger.warning("expire race on claimed task", extra={"task_id": task_id})
            await self.redis.zrem(RETRY_CLAIMED, task_id)
            await events.publish_event(
                self.redis,
                events.TASK_EXPIRED,
                task_id=task_id,
                queue=task.queue,
                task_type=task.task_type,
                attempt=task.attempt,
                worker_id=self.worker_id,
            )
            await complete_idem(
                self.redis,
                task.queue,
                task.idempotency_key or "",
                "failed",
                task_id,
                self.settings.idem_ttl_s,
            )
            return
        if task.status is TaskStatus.QUEUED:
            # Already requeued by an earlier pass that crashed between XADD
            # and ZREM: just drop the stale claim, no double enqueue.
            await self.redis.zrem(RETRY_CLAIMED, task_id)
            return
        if task.status not in (TaskStatus.RETRYING, TaskStatus.PENDING):
            await self.redis.zrem(RETRY_CLAIMED, task_id)
            return

        was_retry = task.status is TaskStatus.RETRYING
        if was_retry:
            # The retry counts as the next attempt; the number is assigned
            # here so the stream entry carries the attempt to execute.
            task.attempt += 1
        transition(task, TaskStatus.QUEUED)
        task.available_at = datetime.now(UTC)
        await save_task(self.redis, task, retention)
        # XADD before ZREM: a crash between them leaves a stale claim that
        # the reaper returns to the schedule; the QUEUED check above makes
        # that requeue idempotent (no double enqueue).
        await xadd_task(
            self.redis,
            stream_name(task.queue, task.priority),
            stream_entry_fields(task),
        )
        await events.publish_event(
            self.redis,
            events.TASK_RETRIED if was_retry else events.TASK_QUEUED,
            task_id=task_id,
            queue=task.queue,
            task_type=task.task_type,
            attempt=task.attempt,
            worker_id=self.worker_id,
        )
        await self.redis.zrem(RETRY_CLAIMED, task_id)

    def _expired(self, task: Task) -> bool:
        ttl = task.metadata.get("ttl_seconds")
        if ttl is None:
            return False
        try:
            ttl_s = float(ttl)
        except (TypeError, ValueError):
            return False
        if ttl_s < 0:
            return False
        return datetime.now(UTC) >= task.created_at + timedelta(seconds=ttl_s)

    # ------------------------------------------------------------------
    # Heartbeat loop: liveness hash + touch in-flight entries.
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        while not self._shutdown_requested:
            try:
                await self._heartbeat_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("heartbeat failed", extra={"worker_id": self.worker_id})
            await self._idle_wait(_jitter(self.settings.heartbeat_interval_s))

    async def _heartbeat_once(self, status: str | None = None) -> None:
        key = worker_key(self.worker_id)
        if status is None:
            status = (
                "draining"
                if self._shutdown_requested
                else ("busy" if self._active_count > 0 else "ready")
            )
        mapping = {
            "worker_id": self.worker_id,
            "hostname": socket.gethostname(),
            "started_at": self._started_at,
            "last_heartbeat": _utcnow_iso(),
            "active_tasks": str(self._active_count),
            "concurrency": str(self.settings.worker_concurrency),
            "tasks_processed": str(self._tasks_processed),
            "tasks_failed": str(self._tasks_failed),
            "tasks_retried": str(self._tasks_retried),
            "status": status,
        }
        await await_redis(self.redis.hset(key, mapping=mapping))
        await await_redis(self.redis.expire(key, self.settings.heartbeat_interval_s * 3))
        await await_redis(self.redis.sadd(WORKERS_SET, self.worker_id))
        m.worker_inflight.labels(worker_id=self.worker_id).set(self._active_count)
        # Touch in-flight entries so long-running tasks are not falsely
        # reclaimed while their worker is alive and heartbeating.
        for stream, entry_id in list(self._in_flight.values()):
            with suppress(Exception):
                await self.redis.xclaim(stream, CONSUMER_GROUP, self.worker_id, 0, [entry_id])
