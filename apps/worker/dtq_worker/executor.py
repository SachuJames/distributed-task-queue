"""Claim pipeline for a single stream entry.

Implements contract sections 3 (ACK order), 4 (retry), 6 (worker behavior),
8 (events), 9 (metrics) and 13 (security) for one claimed entry.

Execution model
---------------
* Async handlers are awaited directly and are cancelled promptly on timeout
  or cancel requests.
* Sync handlers run in a worker thread via :func:`asyncio.to_thread`. They
  are GIL-bound: a CPU-heavy sync handler occupies one thread, so true
  parallelism for CPU-bound work scales with worker processes, not the
  concurrency setting. A sync handler cannot be interrupted: on timeout or
  cancel the awaiting future is cancelled and the thread keeps running
  detached; its result is discarded and the task moves on.
* At-least-once semantics: a crash or timeout after the handler ran but
  before the completion write means the handler may run again on redelivery.
  Handlers must be idempotent.

ACK order (contract section 3): read -> validate -> execute -> record
completion/idempotency state -> ACK. The entry is ACKed only after the task
hash holds the post-execution status, the lifecycle event is published, and
the follow-up write (retry ZADD, DLQ XADD, or idempotency update) is done.
An unexpected internal error never ACKs: the entry stays pending and is
redelivered. ``asyncio.CancelledError`` (worker shutdown) also never ACKs.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from dtq_config.settings import Settings
from dtq_core.keys import (
    CANCELLED_SET,
    CONSUMER_GROUP,
    RETRY_SCHEDULE,
    task_key,
)
from dtq_core.models import InvalidTransition, Task, TaskStatus, is_terminal
from dtq_queue.streams import dlq_xadd, xackdel
from dtq_queue.task_state import (
    get_task,
    record_attempt_end,
    sanitize_error_message,
    update_task,
)
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from . import events
from . import metrics as m
from .idem import complete_idem
from .redis_client import await_redis
from .registry import (
    Handler,
    PermanentError,
    RetryableError,
    TaskContext,
    get_handler,
)
from .retry import compute_retry_delay_ms

logger = logging.getLogger("dtq.worker.executor")

#: How often the cancel watcher polls the task hash while a handler runs.
_CANCEL_POLL_S = 0.25

#: Key holding a handler's JSON result (not in the contract's key list; the
#: contract's DTQ_RESULT_RETENTION_S / DTQ_RESULT_MAX_BYTES imply it).
_RESULT_KEY = "dtq:result:{task_id}"

#: Outcomes returned by execute_entry (also used for worker counters).
OUTCOME_SUCCEEDED = "succeeded"
OUTCOME_RETRY_SCHEDULED = "retry_scheduled"
OUTCOME_DEAD_LETTERED = "dead_lettered"
OUTCOME_FAILED = "failed"
OUTCOME_CANCELLED = "cancelled"
OUTCOME_EXPIRED = "expired"
OUTCOME_ABSORBED = "absorbed"
OUTCOME_SKIPPED = "skipped"
OUTCOME_INTERNAL_ERROR = "internal_error"


@dataclass(frozen=True)
class StreamEntry:
    """One message read from a priority stream."""

    entry_id: str
    fields: dict[str, str]


@dataclass(frozen=True)
class ExecContext:
    """Worker identity and settings for one execution."""

    worker_id: str
    settings: Settings


# ---------------------------------------------------------------------------
# Atomic claim scripts.
#
# The QUEUED -> RUNNING transition must be atomic: two workers (or two
# concurrent poll batches in one worker) can hold duplicate entries for the
# same task, and only one may start the execution. The loser re-reads the
# hash and absorbs its entry. Redelivery of a RUNNING task is likewise
# atomic: exactly one reclaimer increments the attempt.
# ---------------------------------------------------------------------------

#: KEYS[1] = task hash. ARGV = [entry_attempt, worker_id, started_at_iso,
#: retention_s]. Returns 1 when this caller claimed the execution.
_CLAIM_LUA = """
local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'queued' then
    return 0
end
local hattempt = tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0')
local eattempt = tonumber(ARGV[1])
if eattempt < hattempt then
    return 0
end
redis.call('HSET', KEYS[1],
    'status', 'running',
    'attempt', ARGV[1],
    'started_at', ARGV[3],
    'worker_id', ARGV[2],
    'finished_at', '',
    'duration_ms', '',
    'error_class', '',
    'error_message', '',
    'retryable', '',
    'retry_delay_ms', '')
redis.call('EXPIRE', KEYS[1], ARGV[4])
return 1
"""

#: KEYS[1] = task hash. ARGV = [entry_attempt, worker_id, started_at_iso,
#: retention_s]. Returns the new attempt number, or 0 when this entry is not
#: the currently-running attempt (stale duplicate: absorb it).
_RECLAIM_LUA = """
local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'running' then
    return 0
end
local hattempt = tonumber(redis.call('HGET', KEYS[1], 'attempt') or '0')
local eattempt = tonumber(ARGV[1])
if eattempt ~= hattempt then
    return 0
end
local nattempt = hattempt + 1
redis.call('HSET', KEYS[1],
    'attempt', tostring(nattempt),
    'started_at', ARGV[3],
    'worker_id', ARGV[2],
    'finished_at', '',
    'duration_ms', '',
    'error_class', '',
    'error_message', '',
    'retryable', '',
    'retry_delay_ms', '')
redis.call('EXPIRE', KEYS[1], ARGV[4])
return nattempt
"""


async def _claim_for_execution(
    redis: Redis, task_id: str, entry_attempt: int, worker_id: str, retention_s: int
) -> bool:
    claimed = await await_redis(
        redis.eval(
            _CLAIM_LUA,
            1,
            task_key(task_id),
            str(entry_attempt),
            worker_id,
            datetime.now(UTC).isoformat(),
            str(retention_s),
        )
    )
    return int(claimed) == 1


async def _claim_redelivery(
    redis: Redis, task_id: str, entry_attempt: int, worker_id: str, retention_s: int
) -> int:
    """Atomically adopt a crash redelivery. Returns the new attempt, else 0."""
    new_attempt = await await_redis(
        redis.eval(
            _RECLAIM_LUA,
            1,
            task_key(task_id),
            str(entry_attempt),
            worker_id,
            datetime.now(UTC).isoformat(),
            str(retention_s),
        )
    )
    return int(new_attempt)


# ---------------------------------------------------------------------------
# Handler invocation with timeout and cooperative cancel.
# ---------------------------------------------------------------------------


@dataclass
class _Outcome:
    kind: str  # success | timeout | cancelled | retryable | permanent
    result: Any = None
    error_class: str = ""
    error_message: str = ""


async def _cancel_watcher(
    cancel_check: Callable[[], Awaitable[bool]],
) -> bool:
    """Poll until cancellation is requested; never returns False."""
    while True:
        await asyncio.sleep(_CANCEL_POLL_S)
        try:
            if await cancel_check():
                return True
        except Exception:
            logger.warning("cancel check failed; will retry", exc_info=True)


def _classify_error(exc: BaseException) -> _Outcome:
    cls_name = type(exc).__name__
    message = sanitize_error_message(str(exc) or cls_name)
    if isinstance(exc, RetryableError):
        return _Outcome("retryable", error_class=cls_name, error_message=message)
    if isinstance(exc, PermanentError):
        return _Outcome("permanent", error_class=cls_name, error_message=message)
    if isinstance(exc, TimeoutError):  # includes asyncio.TimeoutError
        return _Outcome("timeout", error_class="TimeoutError", error_message=message)
    if isinstance(exc, RedisConnectionError | RedisTimeoutError):
        return _Outcome("retryable", error_class=cls_name, error_message=message)
    if isinstance(exc, ValidationError):
        return _Outcome("permanent", error_class=cls_name, error_message=message)
    # Unknown handler errors are permanent: never retry everything, infinite
    # retry loops are forbidden (contract section 4).
    return _Outcome("permanent", error_class=cls_name, error_message=message)


async def _invoke_handler(
    handler: Handler,
    payload: dict[str, Any],
    hctx: TaskContext,
    timeout_s: float,
    cancel_check: Callable[[], Awaitable[bool]],
) -> _Outcome:
    """Run the handler with timeout and cooperative cancel.

    Returns an _Outcome; never raises except for asyncio.CancelledError from
    an outer scope (worker shutdown), which must propagate without ACKing.
    """
    is_async = inspect.iscoroutinefunction(handler)

    async def _runner() -> Any:
        if is_async:
            return await handler(payload, hctx)
        # Sync handlers run in a thread: GIL-bound, not interruptible. On
        # timeout/cancel the future is cancelled and the thread is detached;
        # its eventual result is discarded.
        return await asyncio.to_thread(handler, payload, hctx)

    exec_task = asyncio.ensure_future(_runner())
    watch_task = asyncio.ensure_future(_cancel_watcher(cancel_check))
    outcome: _Outcome | None = None
    try:
        async with asyncio.timeout(timeout_s):
            done, _pending = await asyncio.wait(
                {exec_task, watch_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if exec_task in done:
                watch_task.cancel()
                with suppress(asyncio.CancelledError):
                    await watch_task
                if exec_task.cancelled():
                    # Cancelled from an outer scope (shutdown), not by us.
                    raise asyncio.CancelledError
                exc = exec_task.exception()
                if exc is not None:
                    return _classify_error(exc)
                return _Outcome("success", result=exec_task.result())
            # The cancel watcher fired first: cancel requested.
            exec_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await exec_task
            return _Outcome("cancelled")
    except TimeoutError:
        # Handler exceeded timeout_ms: retryable (contract section 4). The
        # thread running a sync handler keeps going detached; its result is
        # discarded, and redelivery may repeat side effects (at-least-once).
        outcome = _Outcome(
            "timeout",
            error_class="TimeoutError",
            error_message=sanitize_error_message(f"handler exceeded timeout of {timeout_s:.2f}s"),
        )
    finally:
        for t in (exec_task, watch_task):
            if not t.done():
                t.cancel()
    # Settle without waiting on detached threads: awaiting a cancelled
    # to_thread future raises CancelledError immediately.
    with suppress(asyncio.CancelledError):
        await asyncio.gather(exec_task, watch_task)
    if outcome is None:
        # Unreachable: every path above either returns or sets outcome via
        # the TimeoutError branch. Raise instead of asserting so a future
        # refactor turns this into a loud error, not a silent wrong result.
        raise RuntimeError("handler invocation finished without an outcome")
    return outcome


# ---------------------------------------------------------------------------
# Pre-execution checks.
# ---------------------------------------------------------------------------


async def _cancel_requested(redis: Redis, task_id: str) -> bool:
    raw = await await_redis(redis.hget(task_key(task_id), "cancel_requested"))
    return str(raw).lower() in ("1", "true", "yes")


def _is_expired(task: Task, now: datetime) -> bool:
    ttl = task.metadata.get("ttl_seconds")
    if ttl is None:
        return False
    try:
        ttl_s = float(ttl)
    except (TypeError, ValueError):
        return False
    if ttl_s < 0:
        return False
    return now >= task.created_at + timedelta(seconds=ttl_s)


def _entry_attempt(fields: dict[str, str], task: Task) -> int:
    try:
        return int(fields.get("attempt") or task.attempt)
    except (TypeError, ValueError):
        return task.attempt


# ---------------------------------------------------------------------------
# execute_entry
# ---------------------------------------------------------------------------


async def execute_entry(
    redis: Redis,
    stream: str,
    entry: StreamEntry,
    ctx: ExecContext,
    *,
    reclaimed: bool = False,
) -> str:
    """Run the full claim pipeline for one stream entry.

    Returns an outcome string (OUTCOME_*). Never raises for task-level
    problems; unexpected internal errors are logged and the entry is left
    unacked for redelivery. ``asyncio.CancelledError`` propagates without
    ACKing (shutdown path: never ACK unexecuted work).
    """
    try:
        return await _execute_inner(redis, stream, entry, ctx, reclaimed)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            "execute_entry failed; entry left unacked for redelivery",
            extra={"worker_id": ctx.worker_id},
        )
        return OUTCOME_INTERNAL_ERROR


async def _execute_inner(
    redis: Redis,
    stream: str,
    entry: StreamEntry,
    ctx: ExecContext,
    reclaimed: bool,
) -> str:
    settings = ctx.settings
    retention = settings.task_retention_s
    task_id = entry.fields.get("task_id", "")

    async def _ack() -> None:
        # ACK + delete atomically: XACK alone leaves the entry in the stream,
        # which would grow priority streams without bound and inflate the
        # XLEN-based queue depth used for backpressure.
        await xackdel(redis, stream, CONSUMER_GROUP, entry.entry_id)

    if not task_id:
        await _ack()
        return OUTCOME_SKIPPED

    task = await get_task(redis, task_id)
    if task is None:
        logger.warning("entry references unknown task; ACKing", extra={"worker_id": ctx.worker_id})
        await _ack()
        return OUTCOME_SKIPPED

    # Terminal (or already-scheduled-for-retry: the follow-up write is done,
    # the entry just was not ACKed before a crash): ACK and skip.
    if is_terminal(task.status) or task.status is TaskStatus.RETRYING:
        await _ack()
        return OUTCOME_SKIPPED

    log_extra = {
        "worker_id": ctx.worker_id,
        "task_id": task_id,
        "task_type": task.task_type,
        "queue": task.queue,
        "attempt": task.attempt,
    }

    # Cancelled while queued (set, hash status, or cancel_requested flag).
    if (
        task.status is TaskStatus.CANCELLED
        or await await_redis(redis.sismember(CANCELLED_SET, task_id))
        or await _cancel_requested(redis, task_id)
    ):
        return await _handle_cancelled(
            redis, task, stream, entry, ctx, _ack, "cancelled_while_queued"
        )

    # TTL expiry.
    if _is_expired(task, datetime.now(UTC)):
        return await _handle_expired(redis, task, stream, entry, ctx, _ack)

    entry_attempt = _entry_attempt(entry.fields, task)

    # Duplicate absorption / crash-redelivery routing.
    #
    # Duplicate rule: an entry is absorbed (ACKed, never re-executed) when the
    # task hash already records that attempt as started or superseded:
    #   * RUNNING and entry_attempt <= task.attempt -> a duplicate entry.
    #   * QUEUED and entry_attempt < task.attempt -> a stale duplicate.
    # The (QUEUED, entry_attempt == task.attempt) case is the first delivery
    # of that attempt and must execute.
    # A reclaimed entry (XAUTOCLAIM) with RUNNING and entry_attempt ==
    # task.attempt is a genuine crash redelivery: it counts as a new
    # execution, so the attempt increments atomically.
    if task.status is TaskStatus.RUNNING:
        if entry_attempt < task.attempt:
            return await _absorb(redis, task, stream, entry, ctx, entry_attempt, reclaimed)
        if entry_attempt == task.attempt and not reclaimed:
            return await _absorb(redis, task, stream, entry, ctx, entry_attempt, reclaimed)
        if entry_attempt == task.attempt and reclaimed:
            new_attempt = await _claim_redelivery(
                redis, task_id, entry_attempt, ctx.worker_id, retention
            )
            if new_attempt == 0:
                # Lost the race to another reclaimer; re-read and absorb.
                task = await get_task(redis, task_id)
                if task is None or is_terminal(task.status):
                    await _ack()
                    return OUTCOME_SKIPPED
                return await _absorb(redis, task, stream, entry, ctx, entry_attempt, True)
            task = await get_task(redis, task_id)
            if task is None:  # pragma: no cover - defensive
                await _ack()
                return OUTCOME_SKIPPED
            return await _run_and_finalize(redis, task, stream, entry, ctx, _ack, log_extra)
        # entry_attempt > task.attempt while RUNNING: defensive absorb; the
        # running execution will schedule its own retry if it fails.
        return await _absorb(redis, task, stream, entry, ctx, entry_attempt, reclaimed)

    if task.status is TaskStatus.QUEUED:
        if entry_attempt < task.attempt:
            return await _absorb(redis, task, stream, entry, ctx, entry_attempt, reclaimed)
        claimed = await _claim_for_execution(
            redis, task_id, entry_attempt, ctx.worker_id, retention
        )
        if not claimed:
            # Lost a claim race (or the task moved on): re-read and let the
            # checks above absorb/skip it.
            task = await get_task(redis, task_id)
            if task is None or is_terminal(task.status) or task.status is TaskStatus.RETRYING:
                await _ack()
                return OUTCOME_SKIPPED
            return await _absorb(redis, task, stream, entry, ctx, entry_attempt, reclaimed)
        task = await get_task(redis, task_id)
        if task is None:  # pragma: no cover - defensive
            await _ack()
            return OUTCOME_SKIPPED
        return await _run_and_finalize(redis, task, stream, entry, ctx, _ack, log_extra)

    # Any other non-terminal status in a stream (e.g. PENDING) is unexpected;
    # ACK defensively rather than executing.
    logger.warning(
        "entry for task in unexpected status %s; ACKing without executing",
        task.status.value,
        extra=log_extra,
    )
    await _ack()
    return OUTCOME_SKIPPED


async def _absorb(
    redis: Redis,
    task: Task,
    stream: str,
    entry: StreamEntry,
    ctx: ExecContext,
    entry_attempt: int,
    reclaimed: bool,
) -> str:
    """ACK a duplicate entry without executing. Duplicates become absorbed
    attempts, never new logical tasks (contract section 4)."""
    await xackdel(redis, stream, CONSUMER_GROUP, entry.entry_id)
    await events.publish_event(
        redis,
        events.TASK_DUPLICATE_ABSORBED,
        task_id=task.task_id,
        queue=task.queue,
        task_type=task.task_type,
        attempt=task.attempt,
        worker_id=ctx.worker_id,
        metadata={
            "entry_attempt": entry_attempt,
            "task_attempt": task.attempt,
            "reclaimed": reclaimed,
        },
    )
    return OUTCOME_ABSORBED


async def _transition_or_skip(
    redis: Redis, task_id: str, new_status: TaskStatus, retention_s: int
) -> bool:
    """Move status, tolerating a concurrent terminal move by someone else."""
    try:
        await update_task(redis, task_id, new_status, retention_s)
        return True
    except InvalidTransition:
        logger.warning(
            "status transition to %s refused; task moved concurrently",
            new_status.value,
            extra={"task_id": task_id},
        )
        return False


async def _handle_cancelled(
    redis: Redis,
    task: Task,
    stream: str,
    entry: StreamEntry,
    ctx: ExecContext,
    ack: Callable[[], Awaitable[None]],
    reason: str,
) -> str:
    settings = ctx.settings
    await record_attempt_end(redis, task.task_id, settings.task_retention_s)
    await _transition_or_skip(redis, task.task_id, TaskStatus.CANCELLED, settings.task_retention_s)
    await await_redis(redis.srem(CANCELLED_SET, task.task_id))
    await await_redis(redis.hdel(task_key(task.task_id), "cancel_requested"))
    await events.publish_event(
        redis,
        events.TASK_CANCELLED,
        task_id=task.task_id,
        queue=task.queue,
        task_type=task.task_type,
        attempt=task.attempt,
        worker_id=ctx.worker_id,
        metadata={"reason": reason},
    )
    await complete_idem(
        redis,
        task.queue,
        task.idempotency_key or "",
        "failed",
        task.task_id,
        settings.idem_ttl_s,
    )
    await ack()
    m.tasks_completed_total.labels(
        queue=task.queue, task_type=task.task_type, status="cancelled"
    ).inc()
    m.worker_tasks_total.labels(
        worker_id=ctx.worker_id, task_type=task.task_type, result="cancelled"
    ).inc()
    return OUTCOME_CANCELLED


async def _handle_expired(
    redis: Redis,
    task: Task,
    stream: str,
    entry: StreamEntry,
    ctx: ExecContext,
    ack: Callable[[], Awaitable[None]],
) -> str:
    settings = ctx.settings
    await record_attempt_end(redis, task.task_id, settings.task_retention_s)
    await _transition_or_skip(redis, task.task_id, TaskStatus.EXPIRED, settings.task_retention_s)
    await events.publish_event(
        redis,
        events.TASK_EXPIRED,
        task_id=task.task_id,
        queue=task.queue,
        task_type=task.task_type,
        attempt=task.attempt,
        worker_id=ctx.worker_id,
        metadata={"ttl_seconds": task.metadata.get("ttl_seconds")},
    )
    await complete_idem(
        redis,
        task.queue,
        task.idempotency_key or "",
        "failed",
        task.task_id,
        settings.idem_ttl_s,
    )
    await ack()
    m.tasks_completed_total.labels(
        queue=task.queue, task_type=task.task_type, status="expired"
    ).inc()
    return OUTCOME_EXPIRED


def _decode_payload(entry: StreamEntry) -> tuple[dict[str, Any] | None, str]:
    """Decode the entry payload. Returns (payload, "") or (None, error)."""
    raw = entry.fields.get("payload", "")
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        return None, f"payload decode error: {exc}"
    if not isinstance(payload, dict):
        return None, "payload must be a JSON object"
    return payload, ""


async def _run_and_finalize(
    redis: Redis,
    task: Task,
    stream: str,
    entry: StreamEntry,
    ctx: ExecContext,
    ack: Callable[[], Awaitable[None]],
    log_extra: dict[str, Any],
) -> str:
    settings = ctx.settings
    task_id = task.task_id

    handler = get_handler(task.task_type)
    if handler is None:
        # Unknown task type: permanent, one attempt, never executed
        # (contract sections 4 and 13).
        await record_attempt_end(
            redis,
            task_id,
            settings.task_retention_s,
            error_class="UnknownTaskType",
            error_message=sanitize_error_message(f"unknown_task_type:{task.task_type}"),
            retryable=False,
        )
        return await _finalize_permanent(
            redis,
            task,
            stream,
            entry,
            ctx,
            ack,
            error_class="UnknownTaskType",
            error_message=f"unknown_task_type:{task.task_type}",
            attempts_result="error",
            log_extra=log_extra,
        )

    payload, decode_error = _decode_payload(entry)
    if payload is None:
        await record_attempt_end(
            redis,
            task_id,
            settings.task_retention_s,
            error_class="PayloadDecodeError",
            error_message=sanitize_error_message(decode_error),
            retryable=False,
        )
        return await _finalize_permanent(
            redis,
            task,
            stream,
            entry,
            ctx,
            ack,
            error_class="PayloadDecodeError",
            error_message=decode_error,
            attempts_result="error",
            log_extra=log_extra,
        )

    await events.publish_event(
        redis,
        events.TASK_STARTED,
        task_id=task_id,
        queue=task.queue,
        task_type=task.task_type,
        attempt=task.attempt,
        worker_id=ctx.worker_id,
    )
    m.tasks_started_total.labels(queue=task.queue, task_type=task.task_type).inc()
    if task.created_at is not None and task.started_at is not None:
        wait_s = max(0.0, (task.started_at - task.created_at).total_seconds())
        m.task_wait_seconds.labels(queue=task.queue, task_type=task.task_type).observe(wait_s)

    hctx = TaskContext(
        task_id=task_id,
        idempotency_key=task.idempotency_key,
        attempt=task.attempt,
        worker_id=ctx.worker_id,
        queue=task.queue,
        deadline=time.monotonic() + task.timeout_ms / 1000.0,
    )

    async def _cancel_check() -> bool:
        return await _cancel_requested(redis, task_id)

    exec_started = time.monotonic()
    outcome = await _invoke_handler(handler, payload, hctx, task.timeout_ms / 1000.0, _cancel_check)
    exec_s = time.monotonic() - exec_started

    if outcome.kind == "success":
        return await _finalize_success(
            redis, task, stream, entry, ctx, ack, outcome.result, exec_s, log_extra
        )
    if outcome.kind == "cancelled":
        await record_attempt_end(redis, task_id, settings.task_retention_s)
        await _transition_or_skip(redis, task_id, TaskStatus.CANCELLED, settings.task_retention_s)
        await await_redis(redis.hdel(task_key(task_id), "cancel_requested"))
        await events.publish_event(
            redis,
            events.TASK_CANCELLED,
            task_id=task_id,
            queue=task.queue,
            task_type=task.task_type,
            attempt=task.attempt,
            worker_id=ctx.worker_id,
            metadata={"reason": "cancel_requested_during_execution"},
        )
        await complete_idem(
            redis,
            task.queue,
            task.idempotency_key or "",
            "failed",
            task_id,
            settings.idem_ttl_s,
        )
        await ack()
        m.tasks_completed_total.labels(
            queue=task.queue, task_type=task.task_type, status="cancelled"
        ).inc()
        m.worker_tasks_total.labels(
            worker_id=ctx.worker_id, task_type=task.task_type, result="cancelled"
        ).inc()
        return OUTCOME_CANCELLED
    if outcome.kind in ("retryable", "timeout"):
        return await _finalize_retry(
            redis, task, stream, entry, ctx, ack, outcome, exec_s, log_extra
        )
    # permanent
    await record_attempt_end(
        redis,
        task_id,
        settings.task_retention_s,
        error_class=outcome.error_class,
        error_message=outcome.error_message,
        retryable=False,
    )
    return await _finalize_permanent(
        redis,
        task,
        stream,
        entry,
        ctx,
        ack,
        error_class=outcome.error_class,
        error_message=outcome.error_message,
        attempts_result="error",
        log_extra=log_extra,
    )


async def _finalize_success(
    redis: Redis,
    task: Task,
    stream: str,
    entry: StreamEntry,
    ctx: ExecContext,
    ack: Callable[[], Awaitable[None]],
    result: Any,
    exec_s: float,
    log_extra: dict[str, Any],
) -> str:
    settings = ctx.settings
    task_id = task.task_id
    try:
        result_json = json.dumps(result, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        # Handler returned a non-JSON-serializable value: permanent, retrying
        # cannot fix it.
        await record_attempt_end(
            redis,
            task_id,
            settings.task_retention_s,
            error_class="ResultSerializationError",
            error_message=sanitize_error_message(f"result not JSON-serializable: {exc}"),
            retryable=False,
        )
        return await _finalize_permanent(
            redis,
            task,
            stream,
            entry,
            ctx,
            ack,
            error_class="ResultSerializationError",
            error_message=f"result not JSON-serializable: {exc}",
            attempts_result="error",
            log_extra=log_extra,
        )
    if len(result_json) > settings.result_max_bytes:
        result_json = result_json[: settings.result_max_bytes]

    finished = await record_attempt_end(redis, task_id, settings.task_retention_s)
    await _transition_or_skip(redis, task_id, TaskStatus.SUCCEEDED, settings.task_retention_s)
    await redis.set(
        _RESULT_KEY.format(task_id=task_id), result_json, ex=settings.result_retention_s
    )
    duration_ms = finished.duration_ms or 0
    await events.publish_event(
        redis,
        events.TASK_SUCCEEDED,
        task_id=task_id,
        queue=task.queue,
        task_type=task.task_type,
        attempt=task.attempt,
        worker_id=ctx.worker_id,
        metadata={"duration_ms": duration_ms},
    )
    await complete_idem(
        redis,
        task.queue,
        task.idempotency_key or "",
        "completed",
        task_id,
        settings.idem_ttl_s,
    )
    await ack()

    labels = {"queue": task.queue, "task_type": task.task_type}
    m.tasks_succeeded_total.labels(**labels).inc()
    m.tasks_completed_total.labels(**labels, status="succeeded").inc()
    m.task_attempts_total.labels(**labels, result="success").inc()
    m.task_execution_seconds.labels(**labels).observe(exec_s)
    if finished.created_at is not None and finished.finished_at is not None:
        m.task_duration_seconds.labels(**labels).observe(
            max(0.0, (finished.finished_at - finished.created_at).total_seconds())
        )
    m.worker_tasks_total.labels(
        worker_id=ctx.worker_id, task_type=task.task_type, result="success"
    ).inc()
    logger.info(
        "task succeeded",
        extra={**log_extra, "status": "succeeded", "duration_ms": duration_ms},
    )
    return OUTCOME_SUCCEEDED


async def _finalize_retry(
    redis: Redis,
    task: Task,
    stream: str,
    entry: StreamEntry,
    ctx: ExecContext,
    ack: Callable[[], Awaitable[None]],
    outcome: _Outcome,
    exec_s: float,
    log_extra: dict[str, Any],
) -> str:
    settings = ctx.settings
    task_id = task.task_id
    attempts_result = "timeout" if outcome.kind == "timeout" else "failure"

    if task.attempt >= task.max_attempts:
        # Attempts exhausted: permanent path (contract section 4).
        return await _finalize_permanent(
            redis,
            task,
            stream,
            entry,
            ctx,
            ack,
            error_class=outcome.error_class,
            error_message=outcome.error_message,
            attempts_result=attempts_result,
            log_extra=log_extra,
        )

    delay_ms = compute_retry_delay_ms(
        task.attempt,
        settings.retry_base_delay,
        settings.retry_max_delay,
        settings.retry_jitter,
    )
    due_ms = int(time.time() * 1000) + delay_ms
    await record_attempt_end(
        redis,
        task_id,
        settings.task_retention_s,
        error_class=outcome.error_class,
        error_message=outcome.error_message,
        retryable=True,
        retry_delay_ms=delay_ms,
    )
    await _transition_or_skip(redis, task_id, TaskStatus.RETRYING, settings.task_retention_s)
    await events.publish_event(
        redis,
        events.TASK_RETRY_SCHEDULED,
        task_id=task_id,
        queue=task.queue,
        task_type=task.task_type,
        attempt=task.attempt,
        worker_id=ctx.worker_id,
        metadata={
            "retry_delay_ms": delay_ms,
            "next_attempt": task.attempt + 1,
            "error_class": outcome.error_class,
        },
    )
    # Follow-up write BEFORE the ACK (contract section 3).
    await redis.zadd(RETRY_SCHEDULE, {task_id: due_ms})
    # The idempotency record stays "processing": the task is not terminal.
    await ack()

    labels = {"queue": task.queue, "task_type": task.task_type}
    m.tasks_retried_total.labels(**labels).inc()
    m.task_attempts_total.labels(**labels, result=attempts_result).inc()
    m.retry_delay_seconds.labels(**labels).observe(delay_ms / 1000.0)
    m.task_execution_seconds.labels(**labels).observe(exec_s)
    m.worker_tasks_total.labels(
        worker_id=ctx.worker_id, task_type=task.task_type, result=attempts_result
    ).inc()
    logger.info(
        "task retry scheduled",
        extra={
            **log_extra,
            "status": "retrying",
            "retry_delay_ms": delay_ms,
            "error_class": outcome.error_class,
        },
    )
    return OUTCOME_RETRY_SCHEDULED


async def _finalize_permanent(
    redis: Redis,
    task: Task,
    stream: str,
    entry: StreamEntry,
    ctx: ExecContext,
    ack: Callable[[], Awaitable[None]],
    *,
    error_class: str,
    error_message: str,
    attempts_result: str,
    log_extra: dict[str, Any],
) -> str:
    settings = ctx.settings
    task_id = task.task_id
    last_error = sanitize_error_message(error_message)
    new_status = TaskStatus.DEAD_LETTERED if settings.dlq_enabled else TaskStatus.FAILED

    # Persist the error on the hash before the terminal transition.
    await record_attempt_end(
        redis,
        task_id,
        settings.task_retention_s,
        error_class=error_class,
        error_message=last_error,
        retryable=False,
    )
    await _transition_or_skip(redis, task_id, new_status, settings.task_retention_s)
    if settings.dlq_enabled:
        dlq_fields = dict(entry.fields)
        dlq_fields.update(
            {
                "failed_at_ms": str(int(time.time() * 1000)),
                "attempts_made": str(task.attempt),
                "last_error": last_error,
                "last_error_class": error_class,
                "retryable": "0",
                "worker_id": ctx.worker_id,
                "original_queue": task.queue,
            }
        )
        # Follow-up write BEFORE the ACK (contract section 3).
        await dlq_xadd(redis, dlq_fields, maxlen=settings.dlq_maxlen)
        event_type = events.TASK_DEAD_LETTERED
        completed_status = "dead_lettered"
        outcome = OUTCOME_DEAD_LETTERED
        m.tasks_dead_lettered_total.labels(queue=task.queue, task_type=task.task_type).inc()
    else:
        event_type = events.TASK_FAILED
        completed_status = "failed"
        outcome = OUTCOME_FAILED
        m.tasks_failed_total.labels(queue=task.queue, task_type=task.task_type).inc()

    await events.publish_event(
        redis,
        event_type,
        task_id=task_id,
        queue=task.queue,
        task_type=task.task_type,
        attempt=task.attempt,
        worker_id=ctx.worker_id,
        metadata={
            "error_class": error_class,
            "attempts_made": task.attempt,
        },
    )
    await complete_idem(
        redis,
        task.queue,
        task.idempotency_key or "",
        "failed",
        task_id,
        settings.idem_ttl_s,
    )
    await ack()

    labels = {"queue": task.queue, "task_type": task.task_type}
    m.tasks_completed_total.labels(**labels, status=completed_status).inc()
    m.task_attempts_total.labels(**labels, result=attempts_result).inc()
    m.worker_tasks_total.labels(
        worker_id=ctx.worker_id, task_type=task.task_type, result="failure"
    ).inc()
    logger.info(
        "task reached terminal failure",
        extra={
            **log_extra,
            "status": completed_status,
            "error_class": error_class,
        },
    )
    return outcome
