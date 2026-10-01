# dtq: Distributed Task Queue Engine

dtq is a Redis Streams-backed distributed task queue in Python. Producers
submit JSON tasks over HTTP; workers claim them from priority-ordered
streams, execute registered handlers with timeouts and retries, and move
exhausted tasks to a dead-letter stream. It ships with an HTTP API, a CLI, a
real-time dashboard, and Prometheus metrics, and it runs locally with one
`docker compose up`.

## Key features

- **Strict priority queues**: ten priority bands per queue (p9 down to p0),
  each a Redis Stream with a shared `workers` consumer group.
- **At-least-once execution**: read, validate, execute, record, then ACK.
  Crash redelivery via `XAUTOCLAIM`; duplicate entries are absorbed, never
  executed as new work.
- **Retries with backoff**: exponential backoff with jitter, capped delay,
  per-attempt error classification. Only retryable errors retry; everything
  else fails fast to the DLQ.
- **Idempotent ingest**: `Idempotency-Key` header scoped per queue; replays
  return the stored task, in-flight duplicates get 409.
- **Dead-letter stream**: bounded, inspectable, with requeue (attempt reset,
  history preserved), discard, and purge, all audit-logged.
- **Backpressure**: queue-depth admission gate (429 with `Retry-After`),
  payload size cap (413), bounded worker prefetch, bounded WS buffers.
- **Observability**: Prometheus metrics, `/health` vs `/ready`, structured
  JSON logs, a WebSocket event stream, and an audit stream for admin actions.
- **Delayed tasks**: `delay_seconds` parks tasks in the retry schedule until
  they are due.
- **Task cancellation**: cooperative cancel for queued and running tasks.
- **Single-binary ops story**: `docker compose up` gives you Redis, API,
  worker, dashboard, and Prometheus.

## Architecture

```
                    +------------------+
                    |    dashboard     |  React SPA + WebSocket client (:3000)
                    +--------+---------+
                             |  REST + WS /ws/events
                    +--------v---------+
                    |       api        |  FastAPI (:8000)
                    |  ingest, reads,  |  /api/v1, /health, /ready, /metrics
                    |  admin, WS fanout |
                    +--------+---------+
                             |
              +--------------v---------------+
              |            redis             |  streams, hashes, schedules,
              |  dtq:stream:<queue>:p<0-9>   |  DLQ, idempotency keys, audit,
              |  dtq:retry:schedule          |  worker heartbeats, pub/sub
              |  dtq:stream:dead-letter      |
              +--------------^---------------+
                             |
              +--------------+---------------+
              |      worker (x N)            |  poll / reaper / scheduler /
              |  dtq worker run              |  heartbeat loops, executors
              +------------------------------+
```

A task's life: `POST /api/v1/tasks` validates and writes the task hash, then
XADDs to the priority stream (or parks it in the retry schedule when
delayed). Workers XREADGROUP with bounded prefetch, claim the entry
atomically (`QUEUED -> RUNNING` via Lua), execute the registered handler with
a timeout, record the outcome, publish the lifecycle event, and only then
XACK. Failures schedule retries; exhausted tasks go to the dead-letter
stream. One worker at a time holds the scheduler leader lock and moves due
retries back into the streams.

## Quickstart

### With docker compose

```
docker compose up --build
```

- API: http://localhost:8000 (`/api/v1`, `/health`, `/ready`, `/metrics`,
  `/ws/events`)
- Dashboard: http://localhost:3000
- Prometheus: http://localhost:9090

Scale workers: `docker compose up --build --scale worker=3`.

### Local (no docker)

```
python3 -m venv .venv && .venv/bin/pip install -e .
redis-server --daemonize yes --save '' --appendonly no   # dev only, no persistence
.venv/bin/python -m uvicorn dtq_api.main:app --port 8000 &
.venv/bin/dtq worker run --concurrency 4
```

Submit a task (the worker loads the demo handlers in `examples/tasks` by
default):

```
curl -s -X POST localhost:8000/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"echo_task","payload":{"hello":"world"},"priority":9}' | head -c 300
echo
curl -s localhost:8000/api/v1/tasks/<task_id>
```

More CLI: `dtq get`, `dtq list`, `dtq cancel`, `dtq dlq list`,
`dtq retry-dlq`, `dtq queue pause <q>`, `dtq queue stats <q>`.
Set `DTQ_API_KEY` to require `X-API-Key` (or `--api-key`) for the destructive
commands.

## Delivery semantics

dtq guarantees **at-least-once** execution. A task is never silently dropped
while its data exists in Redis, but a handler may run more than once: a crash
after side effects and before the completion write causes redelivery, and the
re-execution will repeat those side effects. Duplicate *tasks* are absorbed by
claim-time checks; duplicate *executions* are inherent to the model. Write
handlers to be idempotent (see `dtq_tasks.idempotent_operation`), and do not
treat the WebSocket event stream as a record: it is Redis pub/sub,
fire-and-forget, with no replay for disconnected clients.

## Docs

- [docs/performance.md](docs/performance.md): measured throughput and
  latency numbers (this machine, local Redis), what limits them, and honest
  caveats.
- [docs/observability.md](docs/observability.md): Prometheus metrics,
  `/health` vs `/ready`, structured logging, WebSocket stream, audit trail,
  local Prometheus setup.
- [docs/failure-modes.md](docs/failure-modes.md): crash/restart scenarios,
  data-loss windows, duplicate execution, operator recovery checklist.
- [docs/security.md](docs/security.md): trust boundaries, handler execution
  guarantees, API-key auth, validation limits, what is not protected.
- [docs/threat-model.md](docs/threat-model.md): assets, actors, attack
  surface, abuse cases, residual risks.
- [docs/deployment.md](docs/deployment.md): compose topology, scaling,
  full `DTQ_` env reference, backup/restore, zero-downtime notes.
- [docs/contributing.md](docs/contributing.md): repo layout, dev setup,
  tests, lint/typecheck, commit conventions, adding a handler.
- [CONTRACT.md](CONTRACT.md): the normative spec (sections 1-15). Where code
  and contract differ, the docs describe the code and note the difference.

## License

MIT. See [LICENSE](LICENSE).
