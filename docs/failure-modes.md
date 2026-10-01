# Failure modes

What breaks, what survives, what is lost, and how the operator recovers.
dtq guarantees at-least-once execution: a task is never silently dropped
while its data exists in Redis, but handlers may run more than once, and
anything that only ever lived in memory or in pub/sub can be lost.

## Crash and restart scenarios

| Scenario | What happens | What is lost | Recovery |
|---|---|---|---|
| Worker process crashes mid-task (before handler finished) | Entry stays pending in the consumer group. After `DTQ_VISIBILITY_TIMEOUT_S` (default 30s) of idleness, a peer's reaper reclaims it via `XAUTOCLAIM`; the attempt counter increments and the handler runs again. | In-progress handler work. If the handler had already performed side effects, they will repeat on redelivery: handlers must be idempotent. | Automatic. Restart the worker; the reaper does the rest. |
| Worker crashes after the task hash was updated but before `XACK` | On redelivery the worker sees a terminal status (or `RETRYING`, whose follow-up write is already done) in the task hash and ACKs without re-executing. | Nothing. The event was already published; a duplicate terminal event is possible if the crash happened between the event publish and the hash update. | Automatic. |
| Worker crashes after `XACK` | Nothing to do. The execution is complete. | Nothing. | None needed. |
| Worker shut down with SIGTERM | Polling and scheduling stop; in-flight executions drain for up to `DTQ_DRAIN_TIMEOUT_S` (default 30s). Entries for executions that did not finish are left unacked and reclaimed by peers. Async handlers are cancelled promptly; sync (thread) handlers run detached to completion, their result discarded. | Work beyond the drain timeout is abandoned and will be retried by redelivery, possibly duplicating side effects already performed. | Automatic. |
| API process restarts | In-flight HTTP requests die; the API is stateless, so tasks already in Redis are unaffected. The WebSocket fanout restarts and clients reconnect. | Pub/sub events published while no subscriber existed are gone (fire-and-forget). Dashboard clients miss those events but can re-derive state via REST. | Restart the container; healthcheck uses `/health`, traffic gating should use `/ready`. |
| Redis restarts (with AOF, the compose default) | Streams, consumer groups, schedules, DLQ, and task hashes are restored from the append-only file. | Up to about 1 second of recent writes (default `appendfsync everysec`). Task hashes carry a 7-day TTL anyway, so recent ingest bursts are the main exposure. | Automatic on container restart. Verify with `redis-cli ping` and `/ready`. |
| Redis restarts without persistence (e.g. `make redis`, which disables it) | Total loss: streams, schedules, idempotency keys, worker registrations, DLQ, audit stream. | Everything not yet consumed. Tasks accepted but not executed are gone with no record. | None. Do not run production on a non-persistent Redis. |
| Network partition between worker/API and Redis | Executor treats Redis connection errors as retryable; the worker's supervised loops log, back off, and restart. The API returns 503 `REDIS_UNAVAILABLE` on `/ready`; ingest and reads fail fast. | Nothing durable, as long as Redis itself is fine. Tasks keep their place in streams and schedules. | Fix the network. Workers resume polling automatically; no manual catch-up. |
| Scheduler leader disappears (crash, long GC pause) | The leader lock (`dtq:lock:scheduler`, 10s TTL) expires and another worker takes over. Stale claims older than 60s are returned to the schedule by the claim reaper. | Nothing. Duplicate XADDs from a split-brain window are absorbed by the worker's duplicate-absorption logic (see below). | Automatic. |
| Poison message (payload that always fails validation, unknown task type) | Classified permanent on first sight: unknown task types and payload decode errors take the permanent path with a single attempt, then move to the DLQ (or `FAILED` when the DLQ is disabled). Generic handler exceptions are also permanent by default; only `RetryableError`, timeouts, and Redis connection errors retry. | Nothing systemic. The message is quarantined in the DLQ for inspection. | Inspect with `dtq dlq list` or the dashboard DLQ tab; fix the producer; `dtq retry-dlq` to requeue or `dtq discard-dlq` to drop. |
| DLQ reaches its cap | `XADD` with `MAXLEN ~10000` (configurable via `DTQ_DLQ_MAXLEN`) trims the oldest entries. | The oldest dead-letter entries: their payloads and error history. The task hashes keep their `DEAD_LETTERED` status until their 7-day TTL. | Raise `DTQ_DLQ_MAXLEN` or drain the DLQ regularly. Audit the trim pressure via `dtq_dlq_depth`. |
| Clock skew between hosts | The retry schedule, stale-claim reaper (60s), visibility timeout, heartbeats, and `ttl_seconds` expiry all use wall-clock time. A skewed scheduler requeues retries early or late; a skewed worker can look stale or keep a dead leader lock alive. | Timing accuracy only; no task data. | Run NTP on every host. Keep `DTQ_HEARTBEAT_INTERVAL_S` well under `DTQ_VISIBILITY_TIMEOUT_S` (the config validator enforces `<`). |
| Disk full on the Redis host | Redis write commands fail; ingest and task updates return errors; AOF rewrite can fail. With AOF corruption, Redis may refuse to start. | Writes that failed; potentially the AOF tail. | Free disk, then restore from the last good snapshot (see deployment.md). Consider `appendfsync` and snapshot schedules before this happens. |
| Cancel arrives while a task is running | The cancel is recorded (`cancel_requested` on the hash). The cancel watcher polls every 0.25s: async handlers are cancelled promptly, sync handlers run detached to completion and their result is discarded. Status becomes `CANCELLED`. | The handler's partial work. For sync handlers, the thread keeps running even though the result is thrown away. | None needed; the API returns 409 `CANCEL_REQUESTED` while cancellation is in flight. |
| API key compromised or misconfigured | When `DTQ_API_KEY` is set, destructive ops need `X-API-Key`; when empty, everything is open. Reads and submit stay open either way. | Operational, not data: unauthorized cancels, purges, pause/resume. | Rotate the key (restart the API and CLI with the new value), inspect `dtq:stream:audit` for what the old key did. |

## Data-loss windows, stated plainly

- **Pub/sub events are fire-and-forget.** `dtq:events` has no persistence and
  no replay. If no subscriber is listening, or a WebSocket client is
  disconnected, those events are gone forever. The dashboard's event feed is a
  live view, not a record. Task state in Redis remains the source of truth.
- **Slow WebSocket clients drop events.** Non-critical events are dropped
  when a client's 1000-event buffer fills; only terminal events are
  protected. The dashboard dedups by `event_id`, so what you see may skip
  intermediate transitions for a fast-moving task.
- **Crash between side effect and completion write.** If the handler
  performed its side effect and the worker crashed before recording the
  terminal status, redelivery re-runs the handler. dtq cannot make arbitrary
  side effects exactly-once. Use `dtq_tasks.idempotent_operation` or your
  own dedup keys inside handlers (see contributing.md).
- **Task hashes expire.** `dtq:task:<id>` lives for `DTQ_TASK_RETENTION_S`
  (default 7 days), refreshed on updates. After that, `GET /api/v1/tasks/<id>`
  returns 404. Results expire sooner: `DTQ_RESULT_RETENTION_S` (default 1
  hour), truncated to `DTQ_RESULT_MAX_BYTES`.
- **Idempotency keys expire.** `DTQ_IDEM_TTL_S` (default 24 hours). After
  expiry, resubmitting the same key creates a new logical task.
- **Audit stream is trimmed.** `dtq:stream:audit` keeps the last ~10000
  entries. Export it if you need a durable compliance trail.
- **DLQ stream is trimmed.** Oldest entries are evicted past
  `DTQ_DLQ_MAXLEN`.
- **Delayed tasks bypass the depth gate.** Ingest backpressure sums `XLEN`
  across a queue's priority streams; tasks submitted with `delay_seconds`
  wait in the retry schedule, not a stream, so they do not count toward
  `DTQ_MAX_QUEUE_DEPTH`. A flood of delayed submissions can grow the schedule
  unbounded. Watch `dtq_retry_scheduled`.

## Duplicate execution, by design

Duplicates are absorbed, never executed twice as new work:

- The scheduler XADDs before removing the claim; a crash between the two
  leaves a stale claim that requeues the same task again. The worker absorbs
  the duplicate: an entry is ACKed without executing when the task hash
  already shows that attempt started, superseded, or terminal.
- Claim races use Lua scripts: exactly one worker wins the `QUEUED -> RUNNING`
  transition or the crash-redelivery increment; losers re-read and absorb.
- What duplicates *cannot* prevent is re-running the handler after a crash
  that happened after side effects but before the completion write. That is
  the at-least-once contract: absorption stops duplicate *tasks*, not
  duplicate *executions*.

## Operator recovery checklist

1. Workers down: restart them. Pending entries are reclaimed automatically
   after the visibility timeout; stale scheduler claims after 60s.
2. Tasks stuck in `RETRYING` or `PENDING`: check the scheduler leader is
   alive (`dtq:lock:scheduler` in Redis, worker heartbeats fresh). Any worker
   can be leader; no manual election.
3. DLQ growing: inspect entries (`dtq dlq list`, dashboard DLQ tab), fix the
   root cause in the producer or handler, then requeue in bulk or discard.
4. Queue paused accidentally: `dtq queue resume <q>` (audited).
5. Redis data suspect: compare `dtq_queue_depth`, `dtq_queue_pending`,
   `dtq_retry_scheduled`, `dtq_dlq_depth` against expectations; restore from
   the AOF/RDB backup per deployment.md if corruption is confirmed.
6. Audit what happened: `XRANGE dtq:stream:audit - +` shows recent admin
   actions with actor and request id.

## See also

- [observability.md](observability.md): metrics, probes, logs, and the
  WebSocket stream you use to detect these failures.
- [deployment.md](deployment.md): Redis backup/restore and zero-downtime
  restart limits.
- [security.md](security.md): what is and is not protected when things go
  wrong.
