# Worker coordination

A worker is a single process (`apps/worker/dtq_worker/worker.py`) running
four supervised loops: poll, reaper, scheduler, and heartbeat. This
document covers heartbeats and liveness, stale worker detection, graceful
shutdown, the scheduler leader lock, consumer naming, and exactly how
multi-worker races are resolved without double execution.

## The four loops

Each loop runs with jitter and is supervised: if a loop raises, it is
logged and restarted after 0.5-1.5s, never taking the worker down.

- **Poll**: while not shutting down, compute
  `capacity = concurrency - in_flight`; for each configured queue in
  order, `XREADGROUP` up to `min(remaining, 32)` new entries across the
  priority bands `p9` to `p0`, blocking up to 500ms on the first queue's
  first band only while nothing has been collected. Skips paused queues.
  Each entry launches an asyncio task bounded by a semaphore sized to
  `DTQ_WORKER_CONCURRENCY`.
- **Reaper**: every `max(0.5, visibility_timeout / 2)` seconds (jittered),
  `XAUTOCLAIM` idle entries on every priority stream and launch them as
  reclaimed executions. Intake is bounded at `concurrency * 2` per pass.
- **Scheduler**: every ~1s (jittered), try to become leader; the leader
  claims due retries/delayed tasks and reaps stale claims (see below).
- **Heartbeat**: every `DTQ_HEARTBEAT_INTERVAL_S` seconds (jittered),
  write the liveness hash and touch in-flight entries.

## Heartbeats

`_heartbeat_once` writes the hash at `dtq:worker:<worker_id>`:

```
worker_id, hostname, started_at, last_heartbeat, active_tasks,
concurrency, tasks_processed, tasks_failed, tasks_retried, status
```

and refreshes its expiry to 3x the heartbeat interval, then adds the id
to the `dtq:workers` set. It also `XCLAIM`s every in-flight entry back to
itself with idle time 0, resetting the PEL idle clock so long-running
tasks on a live worker are not falsely reclaimed.

`status` is self-reported: `starting`, then `ready` (idle) or `busy`
(`active_tasks > 0`), then `draining` and `stopped` during shutdown.
Startup validation enforces
`DTQ_HEARTBEAT_INTERVAL_S < DTQ_VISIBILITY_TIMEOUT_S`, so a healthy
worker's heartbeat always refreshes before its entries become reclaimable.

## Stale worker detection

`STALE` is observer-computed, never self-reported. The API's
`WorkerRecord.is_live` returns false when `now - last_heartbeat` exceeds
3x the heartbeat interval; the dashboard and `/api/v1/workers` derive
liveness from heartbeat age alone, never from record existence. A worker
that stops heartbeating (crashed, partitioned, SIGKILLed) is therefore
detected as stale within about three heartbeat intervals, and its
heartbeat hash expires on its own. `WORKER_STALE` is a defined event type
for observers to emit; the worker itself never publishes it.

## Graceful shutdown and drain

On `SIGTERM` or `SIGINT` (handlers installed in `Worker.run`;
`initiate_shutdown` is also safe to call from elsewhere):

1. The shutdown event is set. All four loops stop at their next
   iteration boundary; no new entries are launched.
2. The worker publishes `WORKER_STOPPED` after draining and writes a final
   heartbeat with status `draining`, then `stopped`.
3. It waits up to `DTQ_DRAIN_TIMEOUT_S` (default 30s) for in-flight
   executions to finish.
4. If the deadline passes with executions still running, their asyncio
   tasks are cancelled and the worker exits anyway.

Two rules hold throughout: **never ACK unexecuted work**, and **never ACK
on cancellation**. A `CancelledError` in the execution path propagates
without ACKing, so the entry stays pending in the PEL and is reclaimed by
a peer after the visibility timeout. For sync handlers running in a
thread, cancellation detaches the thread (it runs to completion, its
result discarded) while the entry remains unacked for redelivery.

## SIGTERM vs SIGKILL

`SIGTERM` (and `SIGINT`) trigger the drain above. `SIGKILL` cannot be
caught: the process dies instantly with entries unacked and no final
heartbeat. Recovery is entirely via Redis state: the reaper on surviving
workers `XAUTOCLAIM`s the dead worker's idle entries after the visibility
timeout, the heartbeat hash expires after 3x the interval, and the
scheduler lock expires after 10s if the dead worker held it. No operator
cleanup is required after a SIGKILL, only patience for the timeouts.

## Scheduler lock

Only one scheduler acts at a time, coordinated by `dtq:lock:scheduler`:

- Acquisition: `SET lock <worker_id> NX PX 10000`. The value is the
  worker's id (the token).
- Refresh: a holder that finds its own token re-`PEXPIRE`s the lock to
  10000ms each pass, so leadership is stable while the worker is alive.
- Failover: if the leader dies, the lock expires within 10s and another
  worker's next pass (every ~1s) acquires it.

During failover two schedulers can briefly overlap. Overlap is safe
because the claim itself is atomic (Lua moves `schedule -> claimed` in one
script, so a due task is claimed by at most one scheduler per pass) and
because the enqueue step is idempotent: `XADD` happens before `ZREM`, and
a pass that finds a claimed task already `QUEUED` drops the claim without
`XADD`ing again. See [retries](retries.md) for the full claim loop.

## Multi-worker races and exactly-once requeue

Three races are closed with Lua scripts (Redis executes each script
atomically with respect to other clients):

1. **Two workers claim the same `QUEUED` entry** (duplicate entries for
   one task, or two poll batches racing). `_CLAIM_LUA` transitions
   `QUEUED -> RUNNING` only if the hash status is still `queued` and the
   entry's attempt is not older than the hash's attempt. Exactly one
   claimant gets `1`; the loser re-reads the hash and absorbs its entry
   (`TASK_DUPLICATE_ABSORBED`, ACK, no execution).
2. **Two reclaimers adopt the same `RUNNING` redelivery.**
   `_RECLAIM_LUA` increments the attempt only if the hash is `running`
   and the entry's attempt equals the hash's attempt. Exactly one
   reclaimer gets the new attempt number; the loser absorbs.
3. **Two schedulers claim the same due retry** during leader overlap.
   The claim script moves the task `schedule -> claimed` atomically, so
   only one scheduler ever sees it.

"Exactly-once requeue" therefore means: a task leaves the retry schedule
for a stream exactly once per due pass, and a stream entry starts an
execution exactly once across all workers. Duplicate *entries* can still
exist transiently (scheduler crash between `XADD` and `ZREM`); they are
absorbed by rule 1, never executed twice.

## Consumer naming

`worker_id` is `worker-` plus 8 hex characters from `uuid4` (never the
hostname alone), or the explicit `DTQ_WORKER_ID` when set. The id is the
consumer name in `XREADGROUP`/`XAUTOCLAIM`, the scheduler-lock token, and
the `worker_id` label on `dtq_worker_inflight` and
`dtq_worker_tasks_total`. Because the label is one per process, the
cardinality is bounded.

## See also

- [delivery-guarantees](delivery-guarantees.md): crash windows and redelivery
- [retries](retries.md): the scheduler claim loop and stale-claim reaper
- [redis-streams](redis-streams.md): PEL, visibility timeout, key TTLs
- [task-lifecycle](task-lifecycle.md): cancel and expiry during execution
- [architecture](architecture.md): the four loops in system context
