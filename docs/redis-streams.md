# Redis streams and key layout

Every piece of dtq state lives in Redis under the `dtq:` prefix. Key names
are built in exactly one place, `dtq_core.keys`, so a rename touches one
module. This document lists every key, what lives in it, and the
trimming or TTL that bounds it.

## Key layout

| Key | Type | Contents | Bound |
|---|---|---|---|
| `dtq:stream:<queue>:p<0-9>` | stream | Task entries, one stream per queue per priority band (`p9` highest) | Entries removed on `XACK`; no MAXLEN (depth gate is admission control) |
| `dtq:stream:dead-letter` | stream | Dead-lettered task entries | `MAXLEN ~10000` on `XADD` (`DTQ_DLQ_MAXLEN`) |
| `dtq:stream:audit` | stream | Admin actions `{actor, action, task_id, ts, request_id}` | `MAXLEN ~10000` on `XADD` |
| `dtq:retry:schedule` | zset | `task_id` scored by due epoch ms (retries and delayed tasks) | Members removed on claim/requeue/cancel |
| `dtq:retry:claimed` | zset | `task_id` scored by claim epoch ms (scheduler ownership) | Members removed after enqueue; stale claims reaped after 60s |
| `dtq:task:<task_id>` | hash | All task fields plus per-attempt execution metadata | `EXPIRE DTQ_TASK_RETENTION_S` (default 7 days), refreshed on every update |
| `dtq:result:<task_id>` | string | Handler result JSON (truncated to `DTQ_RESULT_MAX_BYTES`) | `EX DTQ_RESULT_RETENTION_S` (default 1h). Note: this key is not listed in `CONTRACT.md` section 2; the contract only implies it via the result settings, and the executor's comments say so. |
| `dtq:idempotency:<queue>:<key>` | string | JSON `{task_id, state, updated_at}`, state in `processing`/`completed`/`failed` | `EX DTQ_IDEM_TTL_S` (default 24h) |
| `dtq:worker:<worker_id>` | hash | `{worker_id, hostname, started_at, last_heartbeat, active_tasks, concurrency, tasks_processed, tasks_failed, tasks_retried, status}` | `EXPIRE` refreshed by heartbeat to 3x the heartbeat interval |
| `dtq:workers` | set | Known worker ids (best effort) | Unbounded by count, but one small member per worker process; liveness comes from heartbeat age, not membership |
| `dtq:queue:<queue>:paused` | string | `"1"` when the queue is paused | Deleted on resume |
| `dtq:cancelled` | set | Task ids cancelled while queued | Members removed when the worker observes them |
| `dtq:tasks:recent` | zset | `task_id` scored by created epoch ms | Trimmed to 10000 entries on ingest |
| `dtq:lock:scheduler` | string | Worker id of the scheduler leader | `PX 10000`, refreshed by the leader |
| `dtq:events` | pub/sub channel | Lifecycle event JSON (not a stream; no persistence) | No buffering; missed by disconnected subscribers |

Every key above has a TTL or a bounded trim strategy except the task
streams themselves, which are drained by `XACK`, and the two small sets
(`dtq:workers`, `dtq:cancelled`), which are cleaned as part of normal
operation.

## Priority streams and the consumer group

Each queue owns ten streams, `dtq:stream:<queue>:p0` (lowest) through
`dtq:stream:<queue>:p9` (highest). Every stream carries a consumer group
named `workers`, created with `MKSTREAM` on first use (`XGROUP CREATE ...
MKSTREAM`, `BUSYGROUP` ignored on restart). Workers drain strictly
`p9` down to `p0`: the poll loop reads `>` (new entries only) from each
band in order, blocking up to 500ms on the first band while idle and
checking lower bands without blocking once entries are collected.

A task entry's fields (set by `dtq_queue.streams.stream_entry_fields`):

```
task_id, queue, task_type, payload (JSON), attempt, max_attempts,
priority, timeout_ms, idempotency_key ("" when none)
```

The entry is a dispatch snapshot; the authoritative record is the task
hash. The entry's `attempt` is the attempt this entry will execute.

## Pending-entries list (PEL) and XAUTOCLAIM

When a consumer reads via `XREADGROUP`, the entry moves into the group's
pending-entries list until `XACK`ed. The PEL is what makes crash recovery
possible: an entry whose worker died stays pending, and the reaper loop
calls `XAUTOCLAIM` with `min_idle_ms = DTQ_VISIBILITY_TIMEOUT_S * 1000`
(default 30s) to transfer entries idle longer than the visibility timeout
to itself. Reclaimed entries are processed like fresh ones, with the
claim pipeline distinguishing genuine redelivery from duplicates (see
[delivery-guarantees](delivery-guarantees.md)).

The pending count per stream (`XPENDING`) feeds the `dtq_queue_pending`
gauge and the API's queue stats. A pending count that grows while workers
look healthy usually means handlers are slower than the visibility
timeout and entries are being reclaimed in a loop.

## Visibility timeout

The visibility timeout is the lease on a claimed entry: how long an entry
may stay unacked before another worker may take it. It is
`DTQ_VISIBILITY_TIMEOUT_S` (default 30s), enforced by startup validation
that requires `DTQ_HEARTBEAT_INTERVAL_S < DTQ_VISIBILITY_TIMEOUT_S` so a
live worker's heartbeat refresh always lands before its entries become
reclaimable. Additionally, each heartbeat `XCLAIM`s the worker's in-flight
entries to itself with idle time 0, resetting their idle clock so
long-running tasks on a healthy worker are not falsely reclaimed.

Operational rule: keep handler timeouts below the visibility timeout.
A handler that runs longer than the visibility timeout on a worker whose
heartbeats have stopped will be re-executed by a peer while the first copy
is still running.

## DLQ stream fields

Dead-letter entries carry the original task entry fields plus:

```
failed_at_ms, attempts_made, last_error, last_error_class,
retryable ("0"), worker_id, original_queue
```

Bounded with `MAXLEN ~10000` (approximate trim) on every `XADD`. DLQ
admin (`/api/v1/dlq/...`) lists newest-first via `XREVRANGE`, finds entries
by scanning for the `task_id` field, removes single entries with `XDEL`,
and purges with `XTRIM ... MAXLEN 0`. Discarding removes the stream entry
and marks `metadata.discarded=true` on the hash; the hash keeps status
`dead_lettered`. Nothing is silently destroyed.

## Audit stream

Every destructive operation (cancel, DLQ requeue/discard/purge, queue
pause/resume) appends `{actor, action, task_id, ts, request_id}` to
`dtq:stream:audit`, bounded with `MAXLEN ~10000`. The stream is the durable
record; a structured log line is emitted alongside it for operators.

## Events channel

`dtq:events` is a pub/sub channel, not a stream: `PUBLISH` returns the
receiver count and there is no history. Event JSON shape:

```
{event_id (uuid hex), ts (ISO UTC), type, task_id, queue,
 task_type, attempt, worker_id, metadata?}
```

Types: `TASK_QUEUED`, `TASK_STARTED`, `TASK_SUCCEEDED`, `TASK_FAILED`,
`TASK_RETRY_SCHEDULED`, `TASK_RETRIED`, `TASK_DEAD_LETTERED`,
`TASK_CANCELLED`, `WORKER_STARTED`, `WORKER_STOPPED`, `WORKER_STALE`,
`CONFIGURATION_CHANGED`, plus the worker-side extensions `TASK_EXPIRED`
and `TASK_DUPLICATE_ABSORBED` (not in `CONTRACT.md` section 8, documented
in the worker's `events.py`). The API runs a single shared subscriber and
fans out to per-client bounded queues for `/ws/events`.

## Trimming and retention summary

- Task hashes: expire after `DTQ_TASK_RETENTION_S` of no updates (default
  7 days). `save_task` refreshes the TTL on every write, including status
  transitions and heartbeat-adjacent updates.
- Results: expire after `DTQ_RESULT_RETENTION_S` (default 1 hour),
  truncated to `DTQ_RESULT_MAX_BYTES` (default 8192) at write time.
- Idempotency records: expire after `DTQ_IDEM_TTL_S` (default 24h);
  terminal-state updates use `KEEPTTL` so completion does not extend the
  window.
- Worker heartbeat hashes: expire 3x the heartbeat interval after the
  last heartbeat; a dead worker's record disappears on its own.
- DLQ and audit streams: approximate `MAXLEN 10000` trims on write.
- Recent-tasks zset: trimmed to 10000 entries on ingest.
- Scheduler lock: 10s TTL, refreshed by the leader while leading.

## See also

- [architecture](architecture.md): how the components use these keys
- [delivery-guarantees](delivery-guarantees.md): PEL, XAUTOCLAIM, and the ACK order
- [worker-coordination](worker-coordination.md): heartbeats, the scheduler lock, claim races
- [task-lifecycle](task-lifecycle.md): which status lives where
- [backpressure](backpressure.md): depth accounting and bounded buffers
