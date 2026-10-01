# Backpressure

dtq applies backpressure at ingest and bounds every buffer a slow consumer
can fill. This document covers the admission check, the 429 semantics,
payload limits, how depth is accounted, and operational guidance for
running a queue near its limits.

## Max queue depth enforcement

`DTQ_MAX_QUEUE_DEPTH` (default 10000) caps how many unacknowledged entries
a queue may hold. On every submit, after validation and the idempotency
replay check but before the idempotency claim, ingest computes the depth
and rejects when `depth >= max_queue_depth`:

- **429**, code `QUEUE_FULL`, with a `Retry-After: 5` header. The 5 seconds
  is a fixed hint, not computed from drain rate.
- The error body follows the standard shape
  `{code, message, request_id}` and includes the queue, the observed depth,
  and the limit in its details.

The check is deliberately ordered after idempotent replay: replaying a
completed key is a read-only operation that creates no work, so it is
never rejected by depth. New work is what gets throttled.

Depth is **approximate under concurrent ingest**: the depth is summed and
then the task is added, with no lock between the check and the `XADD`, so
concurrent submitters can each pass the check and overshoot the limit
slightly. Treat the limit as a soft ceiling that degrades gracefully, not
a hard invariant.

## How depth is accounted

Depth is the sum of `XLEN` across the queue's ten priority streams
(`dtq:stream:<queue>:p0` through `p9`), computed in one pipeline in
`dtq_queue.streams.queue_depth`. There is no per-priority-band cap; a
flood of `p9` tasks and a flood of `p0` tasks count the same toward the
limit. What counts:

- entries waiting in the streams (claimed or not);
- entries pending in the consumer group but not yet completed (they stay in
  the stream until the worker's atomic XACK+XDEL on the completion path;
  XACK alone would not remove them).

What does **not** count toward depth: tasks in `dtq:retry:schedule`
(retries and delayed tasks; see the `dtq_retry_scheduled` gauge), tasks in
the DLQ stream, and task hashes. A queue can therefore hold
`max_queue_depth` stream entries plus an unbounded-by-this-check number of
scheduled retries. The retry path is bounded separately (see below).

Payload size is a separate gate: payloads over `DTQ_MAX_PAYLOAD_BYTES`
(default 262144 bytes, measured on the compact JSON encoding) are rejected
with **413**, code `PAYLOAD_TOO_LARGE`, before any depth check. Metadata is
capped at 4KB serialized.

## Other bounded buffers

- **Worker prefetch** is bounded by concurrency: each poll pulls at most
  `concurrency - in_flight` entries (capped at 32 per read), and the
  reaper's intake is bounded at `concurrency * 2` per pass. Workers never
  hold unbounded entries.
- **Scheduler batch** is capped at 100 claimed tasks per pass
  (`_SCHEDULER_BATCH`), bounding retry throughput per iteration; the
  `dtq_retry_scheduled` gauge exposes the backlog.
- **DLQ stream** is bounded with `MAXLEN ~10000` (approximate trim) on
  every `XADD`, configurable via `DTQ_DLQ_MAXLEN`.
- **Audit stream** is bounded with `MAXLEN ~10000`.
- **Recent-tasks zset** (`dtq:tasks:recent`) is trimmed to 10000 entries.
- **WebSocket per-client buffer**: each `/ws/events` client gets a bounded
  asyncio queue of 1000 events. Slow-client policy: when the queue is full,
  non-critical events are dropped (counted); terminal events
  (`TASK_SUCCEEDED`, `TASK_FAILED`, `TASK_CANCELLED`,
  `TASK_DEAD_LETTERED`) evict the oldest queued event to make room. If even
  a terminal event cannot be queued, the client is disconnected with close
  code 1013.

## Operational guidance

- **Sustained 429s** mean producers outpace consumers. In order of
  preference: add worker processes (or raise `DTQ_WORKER_CONCURRENCY`),
  lower the submit rate, or raise `DTQ_MAX_QUEUE_DEPTH` if Redis memory
  allows. Do not just raise the limit to silence the alert; depth is
  latency.
- **Watch `dtq_retry_scheduled`.** A growing retry schedule means
  failures, not load; adding workers does not help. Check
  `dtq_tasks_failed_total` and the DLQ.
- **Long tasks vs visibility timeout.** Keep handler timeouts comfortably
  below `DTQ_VISIBILITY_TIMEOUT_S` (default 30s). A task that runs longer
  than the visibility timeout gets reclaimed and re-executed while the
  first execution is still running; heartbeats touch in-flight entries
  (`XCLAIM` with idle 0) to prevent this for healthy workers, but a truly
  stuck worker looks the same as a dead one.
- **Pause, don't purge, to shed load.** `POST /api/v1/queue/{queue}/pause`
  stops workers from polling while leaving state intact; `purge` destroys
  stream entries and schedule entries and is audited for a reason.
- **Memory.** Every task hash carries a TTL (`DTQ_TASK_RETENTION_S`,
  refreshed on updates) and results expire after `DTQ_RESULT_RETENTION_S`,
  so steady-state memory is bounded by throughput times retention. If Redis
  memory grows without bound, suspect a stuck consumer group (check
  `dtq_queue_pending`) or a DLQ that nobody drains.
- **Idempotent producers.** Because 429 is a normal signal, producers
  should retry with backoff and always submit with an idempotency key, so
  a retried submit after a 429 (or a timeout) replays instead of
  duplicating.

## See also

- [architecture](architecture.md): where the depth check sits in the submit path
- [redis-streams](redis-streams.md): trimming and retention for every key
- [worker-coordination](worker-coordination.md): prefetch bounds and the heartbeat touch
- [delivery-guarantees](delivery-guarantees.md): why ACK-last interacts with depth accounting
