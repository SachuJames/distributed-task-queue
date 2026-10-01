# Delivery guarantees

dtq provides **at-least-once** execution semantics. Every task that is
accepted will be executed one or more times, unless it is cancelled or its
TTL expires first. The system never silently drops a task, and it never
claims exactly-once. This document is precise about where duplicates can
come from and what the code does to contain them.

## The core rule: ACK last

The executor (`apps/worker/dtq_worker/executor.py`) follows one ordering
for every stream entry: read, validate, execute, record completion and
idempotency state, publish the lifecycle event, do the follow-up write,
then ACK. The entry is never acknowledged before the task hash holds the
post-execution status. The concrete write order per outcome:

- **Success:** `record_attempt_end` -> hash to `SUCCEEDED` -> result `SET`
  at `dtq:result:<task_id>` -> `TASK_SUCCEEDED` event -> idempotency record
  to `completed` -> `XACK`.
- **Retry scheduled:** `record_attempt_end` -> hash to `RETRYING` ->
  `TASK_RETRY_SCHEDULED` event -> `ZADD` to `dtq:retry:schedule` (due time)
  -> `XACK`. The idempotency record stays `processing`.
- **Permanent failure:** `record_attempt_end` -> hash to `DEAD_LETTERED`
  (plus `XADD` to the DLQ stream) or `FAILED` (DLQ disabled) ->
  `TASK_DEAD_LETTERED` / `TASK_FAILED` event -> idempotency record to
  `failed` -> `XACK`.
- **Cancel / expiry:** hash to `CANCELLED` / `EXPIRED`, event, idempotency
  record to `failed`, then `XACK`.

Because the ACK is the last step, a crash at any earlier point leaves the
entry pending in the consumer group's pending-entries list, where the
reaper's `XAUTOCLAIM` picks it up after the visibility timeout.

## Crash windows and what happens in each

| Crash point | What the reclaimer sees | Result |
|---|---|---|
| Before the entry is read | Entry still pending, idle | Redelivered, executed normally |
| During handler execution | Hash `RUNNING`, entry idle | Redelivered; attempt increments atomically (`_RECLAIM_LUA`); handler re-runs |
| After handler finished but before the hash update | Hash still `RUNNING` | Same as above: the reclaimer cannot tell the handler finished, so it re-runs it |
| After hash reached a terminal status (or `RETRYING`) but before `XACK` | Hash terminal / `RETRYING` | Pre-execution check ACKs without re-executing; no duplicate handler run |
| After `XACK` | Nothing pending | Done, no redelivery |

The one window the system cannot close is the third row: if the handler
produced a side effect and the process died before the completion write,
the next execution repeats the side effect. This is inherent to
at-least-once systems without distributed transactions spanning the
handler's side effects.

## What can duplicate

1. **Handler side effects.** Any crash between the handler doing its work
   and the completion write causes a re-execution. Handlers must be
   idempotent. For side effects that must not repeat, use
   `dtq_tasks.idempotent_operation` inside the handler (see
   [idempotency](idempotency.md)).
2. **Stream entries, not logical tasks.** The same logical task can have
   more than one entry in a stream: a scheduler crash between `XADD` and
   `ZREM` leaves a claim that the reaper returns to the schedule, producing
   a second entry on the next pass; two workers can hold duplicate entries
   from overlapping poll batches. These are *absorbed*, not executed twice:
   the claim pipeline compares the entry's attempt against the hash's
   attempt and status. A stale duplicate is ACKed with a
   `TASK_DUPLICATE_ABSORBED` event and never reaches the handler. Duplicates
   become absorbed attempts, never new logical tasks.
3. **Idempotency records.** The record maps duplicate *submissions* to one
   logical task (409 while processing, replay when terminal), but it cannot
   make the handler's own side effects exactly-once. See
   [idempotency](idempotency.md).

## What the system does NOT promise

- **Not exactly-once.** Re-execution after a crash is possible and expected.
  Do not use dtq for work whose side effects cannot tolerate repetition
  unless the handler itself is idempotent.
- **Not zero duplicates.** Duplicate stream entries exist by design in
  crash windows; the guarantee is that they are absorbed, not that they
  never occur.
- **Not ordered.** Tasks within a priority band are roughly FIFO, but
  retries, redeliveries, and multi-worker claiming reorder execution. If
  ordering matters, enforce it in the handler.
- **Not lossless under operator action.** `dtq purge --queue` trims streams
  and drops schedule entries; DLQ purge removes entries. These are explicit
  admin operations, audited, never automatic.
- **No cross-worker exactly-once for the retry schedule during leader
  failover.** The Lua claim script guarantees a due task is claimed by at
  most one scheduler per pass; the `QUEUED`-status guard makes a crash
  between `XADD` and `ZREM` idempotent. A task may get a duplicate *entry*,
  which is then absorbed; it will not get a duplicate *execution* from this
  path.

## How redelivery counts

Attempt numbering starts at 1. A crash redelivery of a `RUNNING` task
increments the attempt atomically inside `_RECLAIM_LUA`, so it counts
against `max_attempts` like any other execution. A task that crashes on
every attempt exhausts its attempts and goes to the DLQ (or `FAILED`),
it does not loop forever. See [retries](retries.md) for the counting
rules and [task-lifecycle](task-lifecycle.md) for the states involved.

## See also

- [task-lifecycle](task-lifecycle.md): the statuses these windows move between
- [retries](retries.md): attempt counting and the retry schedule
- [idempotency](idempotency.md): what idempotency keys do and do not cover
- [redis-streams](redis-streams.md): PEL, `XAUTOCLAIM`, and the visibility timeout
- [worker-coordination](worker-coordination.md): the reaper and heartbeat loops
