# Architecture

The Distributed Task Queue Engine (dtq) is a Redis-backed task queue for
Python. Producers submit tasks through a FastAPI service (or the CLI), tasks
wait in Redis Streams, worker processes claim and execute them, and results
flow back through task hashes, events, and a WebSocket fanout that a
React dashboard consumes.

This document describes the code as it is. Where the implementation
deliberately deviates from `CONTRACT.md`, it is noted.

## Components

```
                +-------------------+
                |   producers       |
                | (API clients,     |
                |  CLI, dashboard)  |
                +--------+----------+
                         | HTTP / Redis
          +--------------+--------------+
          v                             v
 +----------------+            +------------------+
 | API (FastAPI)  |            | CLI (argparse)   |
 | apps/api       |            | apps/cli         |
 | /api/v1/*,     |            | talks to Redis   |
 | /ws/events,    |            | directly, no HTTP|
 | /health,/ready |            +------------------+
 | /metrics       |
 +-------+--------+
         |
         v
 +----------------+        +-------------------+
 | Redis          |<------>| Workers             |
 | (single node,  | streams| apps/worker         |
 |  or cluster    | events | poll, reaper,       |
 |  via DTQ_      |        | scheduler,          |
 |  REDIS_URL)    |        | heartbeat loops     |
 +----------------+        +-------------------+
         ^
         |
 +------------------+
 | Dashboard (React) |
 | apps/dashboard   |
 | REST + WS client  |
 +------------------+
```

**API** (`apps/api/dtq_api/`). Stateless FastAPI service. Accepts task
submissions, validates and persists them, exposes task/queue/worker/DLQ
reads, admin operations (cancel, DLQ requeue/discard/purge, queue
pause/resume), Prometheus metrics, and a WebSocket endpoint that fans out
lifecycle events. It never executes handlers. Routes live in
`dtq_api/routes/`; Redis access is centralized in `dtq_api/store.py`, which
delegates to the shared packages (`dtq_queue.ingest`, `dtq_queue.task_state`,
`dtq_queue.streams`, `dtq_idem.store`, `dtq_events.bus`).

**Workers** (`apps/worker/dtq_worker/`). The execution engine. Each worker
process runs four supervised loops: poll, reaper, scheduler (leader only),
and heartbeat. Handler invocation, timeouts, cancel watching, and the
claim/finalize pipeline live in `executor.py`. Handlers are registered via
`@task("name")` in `dtq_tasks` and executed only from that registry.

**Redis**. The only state store. Streams hold queued work, sorted sets hold
the retry/delay schedule, hashes hold task records and worker heartbeats,
and a pub/sub channel carries events. See
[redis-streams](redis-streams.md) for the full key layout. There is no
separate database; if Redis is empty, the system is empty.

**Dashboard** (`apps/dashboard/`). A React/Vite single-page app with pages
for Overview, Tasks, Queues, Workers, DLQ, and System. It is a pure client
of the API: REST for state, `/ws/events` for live updates. It is served as
a static build (`apps/dashboard/dist/`); it holds no state of its own.

**CLI** (`apps/cli/dtq_cli/`). `dtq` talks to Redis directly using the same
`dtq_api.store` functions as the API (the API and CLI share one data-access
layer). It can submit, inspect, cancel, list, manage the DLQ, pause/resume
queues, and run a worker in-process (`dtq worker run`).

## Shared packages

```
packages/
  core/dtq_core/          Task model, TaskStatus, transitions, key builders, errors
  config/dtq_config/      pydantic-settings Settings (DTQ_* env), validated at startup
  queue/dtq_queue/        streams.py, ingest.py, task_state.py, lua.py
  retry/dtq_retry/        policy.py (delay formula, classification), scheduler.py
  idempotency/dtq_idem/   SET NX record store
  events/dtq_events/      schemas.py, bus.py (pub/sub)
  metrics/dtq_metrics/    prometheus_client metrics
  tasks/dtq_tasks/        @task registry, TaskContext, error types, helpers
```

The API, worker, and CLI all import these; no app duplicates the logic.

## Data flow: submit to result

1. **Submit.** `POST /api/v1/tasks` with `{task_type, payload, queue,
   idempotency_key, max_attempts, priority, timeout_ms, delay_seconds,
   metadata}`. The `Idempotency-Key` header wins over the body field.
   `ingest.submit_task` validates every field (no Redis writes yet), replays
   a completed/failed idempotency key as a duplicate, checks queue depth,
   claims the idempotency record with `SET NX`, writes the task hash and the
   recent-tasks zset entry, then routes: `delay_seconds > 0` puts the task in
   `dtq:retry:schedule` with status `PENDING`; otherwise it is `XADD`ed to
   `dtq:stream:<queue>:p<priority>` with status `QUEUED`. A `TASK_QUEUED`
   event is published. New tasks return 202; idempotent replays return 200
   with `duplicate: true`.

2. **Claim.** A worker's poll loop runs `XREADGROUP` on group `workers`
   across the ten priority streams, highest priority first (`p9` down to
   `p0`), pulling at most `concurrency - in_flight` entries. Each entry
   launches an asyncio task bounded by a semaphore sized to
   `DTQ_WORKER_CONCURRENCY`.

3. **Pre-execution checks.** `execute_entry` loads the task hash and, in
   order: ACKs and skips terminal or `RETRYING` entries (their follow-up
   write already happened); handles cancelled entries; handles TTL-expired
   tasks; then either absorbs a duplicate entry (ACK, `TASK_DUPLICATE_ABSORBED`
   event) or atomically claims the execution with a Lua script
   (`QUEUED -> RUNNING`, or redelivery adoption for `RUNNING`).

4. **Execute.** The handler runs with `asyncio.timeout(timeout_ms)`;
   async handlers are awaited, sync handlers run in a thread via
   `asyncio.to_thread`. A cancel watcher polls the task hash for
   `cancel_requested` every 0.25s.

5. **Finalize, then ACK.** The order is always: write the task hash to its
   post-execution status, publish the lifecycle event, do the follow-up
   write (retry `ZADD`, DLQ `XADD`, result `SET`, or idempotency update),
   and only then `XACK` the stream entry. The three terminal paths:
   - **Success:** `RUNNING -> SUCCEEDED`, result stored at
     `dtq:result:<task_id>`, idempotency record to `completed`,
     `TASK_SUCCEEDED` event, ACK.
   - **Retryable failure** (attempt < max_attempts): `RUNNING -> RETRYING`,
     due time `ZADD`ed to `dtq:retry:schedule`, idempotency record stays
     `processing`, `TASK_RETRY_SCHEDULED` event, ACK.
   - **Permanent failure** (attempt >= max_attempts or non-retryable):
     `RUNNING -> DEAD_LETTERED` (DLQ entry `XADD`ed) or `-> FAILED`
     (DLQ disabled), idempotency record to `failed`, `TASK_DEAD_LETTERED` /
     `TASK_FAILED` event, ACK.

6. **Retry/delayed delivery.** The scheduler leader claims due members of
   `dtq:retry:schedule` with an atomic Lua script (batch cap 100), `XADD`s
   each back into its priority stream as `QUEUED` (attempt incremented for
   retries; delayed tasks keep attempt 1), and publishes
   `TASK_RETRIED` / `TASK_QUEUED`.

7. **Crash recovery.** Unacked stream entries sit in the consumer group's
   pending-entries list (PEL). The reaper loop `XAUTOCLAIM`s entries idle
   longer than the visibility timeout and processes them like fresh ones.
   See [delivery-guarantees](delivery-guarantees.md) and
   [worker-coordination](worker-coordination.md).

## Directory layout

```
distributed-task-queue/
  CONTRACT.md               normative spec (sections 1-15)
  apps/
    api/dtq_api/            FastAPI: routes/, store.py, websocket.py, auth.py, audit.py
    worker/dtq_worker/      worker.py (loops), executor.py (claim pipeline),
                            retry.py, idem.py, registry.py, events.py, metrics.py
    cli/dtq_cli/            argparse CLI, direct Redis access
    dashboard/              React/Vite app (src/, dist/)
  packages/
    core/ config/ queue/ retry/ idempotency/ events/ metrics/ tasks/
  examples/tasks/           demo handlers (echo, sleep, fibonacci, flaky, ...)
  docs/                     this documentation
  docker/ docker-compose.yml
  tests/                    unit + integration tests
```

## Design decisions and why

**Redis Streams plus consumer groups, not lists.** A list gives you
at-most-once pop semantics with no recovery story. Streams give a
persistent, replayable log with a consumer group whose pending-entries list
records exactly which entries each consumer took but never acknowledged.
Crash recovery is then a first-class primitive (`XAUTOCLAIM`), not an
application-level hack.

**At-least-once delivery.** The ACK order (execute, then record completion,
then ACK) means a crash can only ever cause a re-execution, never a silent
loss. The system deliberately does not promise exactly-once: duplicate
stream entries are absorbed by the claim pipeline, but a handler that
already produced a side effect before the crash will produce it again.
Handlers must be idempotent; see
[delivery-guarantees](delivery-guarantees.md).

**Lua for the atomic operations.** Three races cannot be closed with
separate round trips: two workers claiming the same `QUEUED` entry, two
reclaimers adopting the same `RUNNING` redelivery, and two schedulers
claiming the same due retry during a leader failover. Each is a
read-check-write on the task hash or the schedule sorted sets, so each runs
inside one Lua script, which Redis executes atomically with respect to
other clients.

**Strict priority via separate streams.** One stream per queue per priority
band (`dtq:stream:<queue>:p0` .. `p9`), drained `p9` first. Priority is
structural, not a score that can be starved or misordered under load; the
cost is ten streams per queue and a drain loop that checks each band.

**Retry/delay schedule as a sorted set, not stream delays.** Streams have no
native delayed delivery. A zset scored by due epoch milliseconds, swept by a
single leader, handles both backoff retries and `delay_seconds` with one
mechanism. The claim loop is Lua-atomic so leader failover cannot
double-enqueue; a stale-claim reaper returns orphaned claims to the
schedule after 60s.

**Task hashes as the source of truth.** Stream entries carry a snapshot of
the fields needed to execute (payload, attempt, timeout). The task hash
(`dtq:task:<id>`) carries the authoritative status and attempt, which is
what makes duplicate absorption and crash redelivery decidable: the entry
says what was dispatched, the hash says what is true now.

**Events on pub/sub, fanned out by the API.** Workers publish to
`dtq:events`; the API runs one shared subscriber and dispatches to
per-client bounded queues for `/ws/events`. Pub/sub keeps the hot path
non-blocking (no subscriber, no waiting), at the cost that a disconnected
client misses events; clients re-sync over REST.

## See also

- [redis-streams](redis-streams.md): the exact key layout this architecture rests on
- [task-lifecycle](task-lifecycle.md): the status machine
- [delivery-guarantees](delivery-guarantees.md): what at-least-once means in practice
- [worker-coordination](worker-coordination.md): the four loops and multi-worker races
- [backpressure](backpressure.md): admission control and bounded buffers
