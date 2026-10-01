# Retries

When a handler fails, the executor decides: retry later with backoff, or
fail permanently. This document covers the retry policy, the exact backoff
formula, error classification, the schedule/claimed sorted sets, the
scheduler's claim loop, and the attempt counting rules.

## Retry policy

After an attempt fails, the executor (`executor._finalize_retry`) retries
only if **both** hold:

1. The error is classified retryable (see below).
2. `task.attempt < task.max_attempts`. If the attempt number has reached
   `max_attempts`, the failure takes the permanent path (DLQ or `FAILED`)
   even if the error is retryable.

Timeouts are retryable: a handler that exceeds `timeout_ms` is treated like
a retryable failure, so a slow-but-retryable task gets its remaining
attempts rather than failing immediately.

On retry, the executor computes the delay, writes the attempt's error
metadata to the hash, transitions the task to `RETRYING`, publishes
`TASK_RETRY_SCHEDULED`, `ZADD`s the task id to `dtq:retry:schedule` with
score `now_ms + delay_ms`, and ACKs the entry. The idempotency record stays
`processing` because the task is not terminal.

## The backoff formula

The worker computes the delay in `dtq_worker.retry.compute_retry_delay_ms`
(the shared `dtq_retry.policy.compute_delay_s` implements the same formula
in seconds):

```
capped = min(max_delay, base * 2 ** (attempt - 1))
actual = capped * (1 + uniform(-jitter, jitter))
delay  = max(0, int(actual * 1000))          # milliseconds
```

`attempt` is the attempt number that just failed, starting at 1. Defaults
from `DTQ_RETRY_BASE_DELAY=1.0`, `DTQ_RETRY_MAX_DELAY=60.0`,
`DTQ_RETRY_JITTER=0.2`. The jitter is a uniform spread, applied
multiplicatively, so with defaults:

| Failed attempt | Capped delay | Actual delay range |
|---|---|---|
| 1 | 1.0s | 0.8s - 1.2s |
| 2 | 2.0s | 1.6s - 2.4s |
| 3 | 4.0s | 3.2s - 4.8s |
| 4 | 8.0s | 6.4s - 9.6s |
| 5 | 16.0s | 12.8s - 19.2s |
| 6 | 32.0s | 25.6s - 38.4s |
| 7+ | 60.0s (cap) | 48.0s - 72.0s |

(The ranges are derived from the formula with the default settings, not
measured.) The exponential growth is capped so a task with a high
`max_attempts` does not schedule retries hours out, and the jitter spreads
simultaneous failures so a fleet of workers does not retry in lockstep.

## Retryable vs permanent classification

`dtq_retry.policy.classify_retryable` implements a closed allowlist. Unknown
exceptions are permanent: the policy never retries everything, because an
always-failing handler with an always-retryable classification would loop
until `max_attempts` on every task.

**Retryable:** `dtq_tasks.RetryableError` (raised deliberately by the
handler), `TimeoutError` / `asyncio.TimeoutError` (including the
handler-timeout path), `redis.exceptions.ConnectionError`,
`redis.exceptions.TimeoutError`.

**Permanent:** `dtq_tasks.PermanentError`, Pydantic `ValidationError`,
unknown `task_type` (never executed; one attempt, then DLQ/`FAILED` with
`last_error="unknown_task_type:<name>"`), payload decode errors,
non-JSON-serializable handler results, over-limit error messages, and any
other unrecognized exception.

Handlers signal intent explicitly: raise `RetryableError` for transient
problems (a downstream blip), `PermanentError` for problems where retrying
cannot help (bad arguments, poison data). Anything else is treated as a bug
in the handler and fails permanently.

## The retry schedule, the claimed set, and the scheduler loop

Two sorted sets coordinate delayed work:

- `dtq:retry:schedule`: member `task_id`, score = due epoch milliseconds.
  Holds both backoff retries and delayed (`delay_seconds`) tasks.
- `dtq:retry:claimed`: member `task_id`, score = claim epoch milliseconds.
  Holds tasks a scheduler pass has taken ownership of but not yet enqueued.

The scheduler loop runs inside each worker but only acts while holding the
leader lock (see [worker-coordination](worker-coordination.md)). Each pass,
roughly every second with jitter:

1. **Claim** up to 100 due members with one atomic Lua script
   (`dtq_queue.lua.claim_due`): `ZRANGEBYSCORE` due tasks, then move each
   `schedule -> claimed`. Atomicity matters because two schedulers can
   briefly overlap during leader failover; the script guarantees a task is
   claimed at most once per pass.
2. **Enqueue** each claimed task (`Worker._requeue_claimed`): reload the
   hash and handle the cases:
   - hash missing or terminal: drop the claim, nothing to do;
   - in `dtq:cancelled` set: move to `CANCELLED`, publish the event, drop
     the claim;
   - TTL expired: move to `EXPIRED`, publish the event, drop the claim;
   - already `QUEUED`: drop the claim *without* `XADD`ing. This is the
     crash-recovery path: a previous pass `XADD`ed the entry but died
     before `ZREM`ing the claim, so the claim is stale. Dropping it avoids
     a duplicate entry.
   - `RETRYING`: increment the attempt (this assigns the upcoming attempt
     number), transition to `QUEUED`, `XADD` to the priority stream,
     publish `TASK_RETRIED`, then `ZREM` the claim;
   - `PENDING` (delay elapsed): transition to `QUEUED` with the attempt
     unchanged (stays 1), `XADD`, publish `TASK_QUEUED`, then `ZREM`.
   
   The order is `XADD` before `ZREM` on purpose: a crash between them
   leaves a stale claim that the next pass absorbs via the already-`QUEUED`
   check, so the task is never lost and never double-enqueued.
3. **Reap stale claims**: claims older than 60s are moved back to the
   schedule with a Lua script (`requeue_stale`), scored due-now so the
   task is retried promptly. This covers a scheduler that died mid-pass.

Note on a code detail: `dtq_retry.scheduler` also ships a
`requeue_claimed` helper that increments the attempt for every requeue, but
the worker's scheduler path uses its own `_requeue_claimed` (which only
increments for `RETRYING`, not for delayed `PENDING` tasks). The shared
helper is exercised by tests, not by the live scheduler loop.

## Attempt counting rules

- `attempt` starts at **1**, assigned at ingest.
- It is **not** incremented when an execution starts; the number is
  assigned beforehand (at ingest or by the scheduler), so the stream entry
  carries the attempt it will execute.
- It **is** incremented in exactly two situations: the scheduler requeues
  a `RETRYING` task (the retry becomes attempt N+1), and a crash redelivery
  re-adopts a `RUNNING` task (atomic increment inside `_RECLAIM_LUA`).
- Delayed (`PENDING`) tasks keep attempt 1 when first enqueued.
- DLQ requeue (admin) resets the attempt to 1 and archives the old count
  in `metadata.previous_attempts`. (Deviates from `CONTRACT.md` section 11,
  which says 0; the `Task` model requires `attempt >= 1`.)
- The retry gate is `task.attempt >= task.max_attempts`: when the failed
  attempt number has reached the cap, no more retries are scheduled. With
  the default `max_attempts=5`, a task executes at most 5 times total.
- Redelivery counts as an execution: a task that crashes on every attempt
  burns through its attempts and lands in the DLQ, it does not retry
  forever.

## See also

- [delivery-guarantees](delivery-guarantees.md): crash windows and why redelivery re-runs handlers
- [task-lifecycle](task-lifecycle.md): the `retrying` holding state and who moves it
- [worker-coordination](worker-coordination.md): the scheduler leader lock and multi-worker races
- [redis-streams](redis-streams.md): the schedule/claimed key layout
- [idempotency](idempotency.md): why retries keep the idempotency record `processing`
