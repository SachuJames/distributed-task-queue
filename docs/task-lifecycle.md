# Task lifecycle

Every task moves through a fixed set of statuses defined in
`dtq_core.models.TaskStatus`. All transitions go through
`dtq_core.models.transition`, which rejects any move not listed in
`VALID_TRANSITIONS`. Terminal statuses have no outgoing transitions.

## Statuses

| Status | Meaning |
|---|---|
| `pending` | Accepted but not yet in a stream. Only delayed tasks (`delay_seconds > 0`) sit here, waiting in `dtq:retry:schedule`. |
| `queued` | An entry exists in `dtq:stream:<queue>:p<N>`. Eligible for claiming. |
| `running` | Claimed by a worker; the handler is executing (or was, before a crash). |
| `succeeded` | Terminal. The handler returned a JSON-serializable result. |
| `retrying` | Non-terminal holding state. The attempt failed retryably; the task sits in `dtq:retry:schedule` until its due time. |
| `failed` | Terminal. Attempts exhausted or permanent failure, with the DLQ disabled. |
| `dead_lettered` | Terminal. Attempts exhausted or permanent failure, moved to `dtq:stream:dead-letter`. |
| `cancelled` | Terminal. Cancelled before completion. |
| `expired` | Terminal. `metadata.ttl_seconds` elapsed before the task ran. |

## Transition diagram

```
                        +-----------+
                        |  pending  |  (delayed only)
                        +-----+-----+
                              | scheduler requeue (delay elapsed)
                              v
   +------------------> +-----------+  scheduler requeue (retry due)
   |                    |  queued   |<-------------------+
   |                    +-----+-----+                    |
   |                          | worker claim (Lua)       | executor:
   |                          v                    retrying -> queued
   |                    +-----------+                    |
   |                    |  running  |                    |
   |                    +-----+-----+                    |
   |                          | handler finished         |
   |          +---------------+---------------+          |
   |          |               |               |          |
   |          v               v               v          |
   |    +----------+   +-----------+   +------------+   |
   +----| retrying |   | succeeded |   |   failed   |   |
        +----------+   | (terminal)|   | (terminal) |   |
                       +-----------+   +------------+   |
                       +------------+                   |
                       |dead_lettered|                  |
                       | (terminal) |                  |
                       +------------+                  |
                                                       |
   Any non-terminal status can move to cancelled or    |
   expired (expiry only when metadata.ttl_seconds set).+
```

In table form (`VALID_TRANSITIONS` in `models.py`):

- `pending` -> `queued`, `cancelled`, `expired`
- `queued` -> `running`, `cancelled`, `expired`
- `running` -> `succeeded`, `retrying`, `failed`, `dead_lettered`, `cancelled`
- `retrying` -> `queued`, `cancelled`
- `succeeded`, `failed`, `dead_lettered`, `cancelled`, `expired`: terminal, no outgoing transitions

Note: `retrying` is the only non-terminal status that is not executable; it
is a holding state between the failure write and the scheduler's requeue.
The executor also treats a `retrying` task found in a stream as
already-finalized (its follow-up write happened; only the ACK was lost) and
ACKs it without executing.

## Who moves the task

- **API ingest** (`dtq_queue.ingest.submit_task`): creates the task as
  `PENDING` (delayed) or `QUEUED` (immediate). This is the only place tasks
  are born. Attempt starts at 1.
- **Worker claim** (`executor._claim_for_execution`, Lua): `QUEUED ->
  RUNNING`, atomically. Sets `started_at`, `worker_id`, clears the previous
  attempt's error fields. Only one claimant wins; losers absorb their entry.
- **Worker redelivery** (`executor._claim_redelivery`, Lua):
  re-adopts a `RUNNING` task after a crash. Status stays `RUNNING`; the
  attempt increments atomically.
- **Executor finalize**: `RUNNING -> SUCCEEDED` (result recorded),
  `RUNNING -> RETRYING` (retry scheduled, due time in the schedule zset),
  `RUNNING -> DEAD_LETTERED` (DLQ entry written) or `RUNNING -> FAILED`
  (DLQ disabled). Permanent failures that never execute (unknown task type,
  payload decode errors, non-serializable results) take the same terminal
  path directly.
- **Scheduler** (`Worker._requeue_claimed`, leader only): `RETRYING ->
  QUEUED` (attempt incremented) or `PENDING -> QUEUED` (delay elapsed,
  attempt stays 1), each followed by an `XADD` into the priority stream.
- **Cancel**: the API moves `QUEUED`/`PENDING`/`RETRYING` tasks straight to
  `CANCELLED` (also adding them to `dtq:cancelled` and removing them from
  the schedule). A `RUNNING` task gets `cancel_requested` set on its hash;
  the worker's cancel watcher or pre-execution check moves it to
  `CANCELLED` and discards the handler result.
- **TTL expiry**: checked when a worker is about to execute a claimed entry
  and when the scheduler requeues a claim. Expired tasks move to `EXPIRED`
  without executing.
- **DLQ admin** (`dtq_api.store.dlq_requeue`): `DEAD_LETTERED -> QUEUED`.
  This is an admin resurrection outside the worker lifecycle, so it
  deliberately bypasses `models.transition`. The attempt resets to 1 and
  the previous attempt count is preserved in
  `metadata.previous_attempts`. Note: `CONTRACT.md` section 11 says the
  attempt resets to 0, but the shared `Task` model requires `attempt >= 1`,
  so the code uses 1; this is documented in `store.py`.

## Cancel flow

1. `POST /api/v1/tasks/{id}/cancel` (requires `X-API-Key` when
   `DTQ_API_KEY` is set).
2. Task not found -> 404. Already terminal -> 410.
3. Task `RUNNING` -> the API sets `cancel_requested` on the task hash and
   returns 409 with code `CANCEL_REQUESTED`. Cancellation is best effort:
   async handlers are cancelled promptly; sync handlers running in a thread
   run to completion and their result is discarded, with the task still
   ending `CANCELLED`.
4. Task `QUEUED`, `PENDING`, or `RETRYING` -> the API transitions it to
   `CANCELLED` immediately (202), adds the id to `dtq:cancelled` so any
   in-flight stream entry is skipped by workers, and removes it from the
   retry schedule and claimed set.
5. Every cancel is written to the audit stream. The idempotency record is
   set to `failed` (see [idempotency](idempotency.md)).

## TTL expiry

A task carries an optional `metadata.ttl_seconds`. Expiry is evaluated
lazily, never by a sweeper:

- `expired(now)` is true when `now >= created_at + ttl_seconds`. Negative,
  zero-absent, or non-numeric values never expire.
- Checked in two places: the executor's pre-execution checks (a claimed
  entry for an expired task is ACKed and moved to `EXPIRED`), and the
  scheduler's `_requeue_claimed` (a scheduled task that expired while
  waiting is moved to `EXPIRED` instead of being enqueued).
- Expiry applies only before the task runs. A `RUNNING` task whose TTL
  passes mid-execution is not interrupted; the TTL gates *starting*, not
  finishing.

The task hash itself lives for `DTQ_TASK_RETENTION_S` (default 7 days)
regardless of status; that is storage retention, not task TTL.

## See also

- [delivery-guarantees](delivery-guarantees.md): crash windows between these transitions
- [retries](retries.md): the `retrying` holding state and attempt counting
- [idempotency](idempotency.md): terminal-state replay of idempotency keys
- [redis-streams](redis-streams.md): where each status lives in Redis
- [architecture](architecture.md): system overview
