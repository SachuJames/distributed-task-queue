# Observability

How to watch dtq in production: Prometheus metrics, health probes, structured
logs, the WebSocket event stream, and the audit trail for admin actions.

## Prometheus metrics

The API exposes the full metric set at `GET /metrics` (port 8000). Gauges are
recomputed from Redis on every scrape (`refresh_gauges`), so they reflect
point-in-time truth; the refresh is best effort and never breaks the
endpoint. All metric names and label sets are defined in
`packages/metrics/dtq_metrics/metrics.py`. Labels are limited to `task_type`,
`queue`, `status`, `result`, `operation`, `reason`, and `worker_id` (only on
the per-worker metrics, where cardinality is one value per process).
`task_id`, request ids, exception text, and payloads never appear as labels.

### Counters

| Metric | Labels | What it measures |
|---|---|---|
| `dtq_tasks_submitted_total` | `queue`, `task_type` | Tasks accepted by the API at ingest |
| `dtq_tasks_started_total` | `queue`, `task_type` | Handler executions started by workers |
| `dtq_tasks_succeeded_total` | `queue`, `task_type` | Executions that returned a result |
| `dtq_tasks_failed_total` | `queue`, `task_type` | Tasks that reached `FAILED` (attempts exhausted, DLQ disabled) |
| `dtq_tasks_retried_total` | `queue`, `task_type` | Attempts scheduled for retry |
| `dtq_tasks_dead_lettered_total` | `queue`, `task_type` | Tasks moved to the dead-letter stream |
| `dtq_tasks_completed_total` | `queue`, `task_type`, `status` | Tasks reaching a terminal state. `status` in `succeeded`, `failed`, `dead_lettered`, `cancelled`, `expired` |
| `dtq_task_attempts_total` | `queue`, `task_type`, `result` | Handler attempts by outcome. `result` in `success`, `failure`, `timeout`, `error` |
| `dtq_redis_operations_total` | `operation`, `result` | Redis operations by operation and result |
| `dtq_ingest_rejected_total` | `queue`, `reason` | Ingest rejections by reason (queue full, payload too large, unknown task type, invalid body) |

### Gauges

| Metric | Labels | What it measures |
|---|---|---|
| `dtq_queue_depth` | `queue` | Sum of `XLEN` across a queue's ten priority streams |
| `dtq_queue_pending` | `queue` | Total `XPENDING` entries across a queue's streams (claimed but unacked) |
| `dtq_retry_scheduled` | none | Tasks waiting in the retry/delay schedule |
| `dtq_dlq_depth` | none | Entries in the dead-letter stream |
| `dtq_workers_active` | none | Workers with a fresh heartbeat |
| `dtq_worker_inflight` | `worker_id` | Currently executing tasks per worker process |
| `dtq_worker_tasks_total` | `worker_id`, `task_type`, `result` | Tasks handled per worker by result |

### Histograms

| Metric | Labels | What it measures |
|---|---|---|
| `dtq_task_duration_seconds` | `queue`, `task_type` | End to end: task created to terminal status |
| `dtq_task_execution_seconds` | `queue`, `task_type` | Handler only, excluding queueing and retries |
| `dtq_task_wait_seconds` | `queue`, `task_type` | Created to first execution start |
| `dtq_retry_delay_seconds` | `queue`, `task_type` | Computed retry delay per attempt, before jitter |

### Useful queries

```
# Success rate per task type over the last 5 minutes
sum(rate(dtq_task_attempts_total{result="success"}[5m])) by (task_type)
/
sum(rate(dtq_task_attempts_total[5m])) by (task_type)

# Queue depth vs in-flight work
dtq_queue_depth{queue="default"} + dtq_queue_pending{queue="default"}

# p95 handler execution time
histogram_quantile(0.95, sum(rate(dtq_task_execution_seconds_bucket[5m])) by (le, task_type))

# Tasks stuck in the retry schedule
dtq_retry_scheduled

# Rejection pressure at ingest
sum(rate(dtq_ingest_rejected_total[5m])) by (reason)
```

### Metric type note

`dtq_worker_tasks_total` is defined as a **Gauge** in the shared bundle
(`packages/metrics/dtq_metrics/metrics.py`, the one `/metrics` actually
serves) and as a **Counter** in `apps/worker/dtq_worker/metrics.py`. The
worker owns a private registry that is currently not exposed over HTTP, so
the served variant is the Gauge. Treat the series as monotonically
increasing in practice, and see the deployment note below about worker
metrics exposition.

## /health vs /ready

Both live at the API root. They answer different questions.

- `GET /health`: liveness only. Returns `{"status": "ok", "service":
  "dtq-api"}` when the process is up. It touches no dependencies, so it stays
  green during a Redis outage. Use it for container healthchecks and process
  supervisors.
- `GET /ready`: readiness. Pings Redis and verifies the `workers` consumer
  group exists on the task streams. Returns `{"status": "ready"}` on success,
  or HTTP 503 with code `REDIS_UNAVAILABLE` and a detail message (ping
  failure, or how many streams are missing the consumer group) when a
  dependency is down. Use it for load balancer membership and rollout gates.

The compose healthcheck hits `/health`; orchestrators that gate traffic
should use `/ready`.

## Structured logging

Set `DTQ_LOG_JSON=true` for JSON lines, otherwise the default is a readable
single-line format. JSON records carry `timestamp`, `level`, `service`
(`dtq-api` or `dtq-worker`), `worker_id`, `task_id`, `task_type`, `queue`,
`attempt`, `status`, `duration_ms`, `event_type`, plus `actor`, `request_id`,
and `path` on the API side, and the human message in `message`.

Rules the code enforces:

- Payloads are never logged, and neither are secrets or API keys.
- Newlines and carriage returns in logged fields are replaced with spaces,
  so a hostile task type or error message cannot forge log lines.
- The API echoes a request id back in the `X-Request-ID` response header and
  includes it in the audit stream; send your own with the `X-Request-ID`
  header to correlate across services.
- Worker logs emit one line per task outcome (`task succeeded`, `task retry
  scheduled`, `task reached terminal failure`) with `error_class` on
  failures, and one line when a loop crashes and restarts.

Log level comes from `DTQ_LOG_LEVEL` (`DEBUG`, `INFO`, `WARNING`, `ERROR`,
`CRITICAL`), applied at startup.

## WebSocket event stream

`WS /ws/events` (path configurable via `DTQ_WS_PATH`) streams every lifecycle
event as JSON: `event_id`, `ts`, `type`, `task_id`, `queue`, `task_type`,
`attempt`, `worker_id`, optional `metadata`. The API runs one shared Redis
pub/sub subscriber and fans each event out to every connected client's own
bounded queue.

- Per-client queue cap: 1000 events.
- Slow-client policy: when a client's queue is full, non-critical events are
  dropped (the drop count is logged on disconnect). Terminal events
  (`TASK_SUCCEEDED`, `TASK_FAILED`, `TASK_CANCELLED`, `TASK_DEAD_LETTERED`)
  evict the oldest queued event instead. If even a terminal event cannot be
  queued, the server closes the connection with code 1013.
- Keepalive: both sides send `ping`/`pong` JSON frames every 20 seconds.

The dashboard connects with exponential backoff, dedups by `event_id`, keeps
a bounded local buffer, and labels derived rates as approximate. Note the
filter gap: the worker also emits `TASK_EXPIRED` and `TASK_DUPLICATE_ABSORBED`,
but the dashboard's client-side allowlist does not include them, so they do
not appear in the dashboard event feed. REST endpoints remain the source of
truth for those transitions.

Because the underlying transport is Redis pub/sub, events are fire-and-forget:
a client that is disconnected, or that connects after an event was published,
never sees it. The stream is a live view, not a replay log. See
[failure-modes.md](failure-modes.md) for the data-loss windows.

## Audit stream for destructive ops

Every destructive operation appends one entry to the Redis stream
`dtq:stream:audit` (trimmed with `MAXLEN ~10000`, approximate) with
`{actor, action, task_id, ts, request_id}`, and also emits a structured log
line. Covered operations: task cancel, DLQ requeue, DLQ discard, DLQ purge,
queue pause, queue resume, and CLI queue purge.

The `actor` is `api-key` when the request presented a valid `X-API-Key`,
`local` when auth is disabled, and `cli` for CLI commands. The audit stream
is the durable record; the log line is for operators. Because the stream is
trimmed at 10000 entries, export it somewhere durable if you need long-term
retention.

## Local Prometheus via docker compose

`docker compose up --build` starts Prometheus on
[http://localhost:9090](http://localhost:9090) with the config in
`docker/prometheus.yml`:

- job `dtq-api`: target `api:8000`, path `/metrics`.
- job `dtq-worker`: target `worker:8001`, path `/metrics`.

Scaling caveat: with `docker compose up --scale worker=3`, all replicas share
the service name `worker` and Docker's internal DNS round-robins between
them, so the static target scrapes only one replica per interval. For
per-replica worker metrics at scale, give each worker a distinct published
metrics port or use a discovery mechanism that lists every replica.

Current limitation: no code in `apps/worker` actually starts an HTTP server
on `DTQ_WORKER_METRICS_PORT`; the variable is set in the Dockerfile and
compose file, and Prometheus is configured to scrape it, but the worker's
Prometheus registry is not exposed over HTTP today. Worker-side counters
(`dtq_tasks_started_total`, `dtq_task_execution_seconds`, and friends) are
therefore only visible if you expose the registry yourself. The API's
`/metrics` is the one live endpoint, with gauges refreshed from Redis. This
is a known gap between the packaging and the code.

Grafana is not part of the compose stack. To add it, point a Grafana
container at `http://prometheus:9090` and import the queries above; keep the
dashboard's derived rates clearly labeled as approximate, since they come
from the lossy WebSocket stream.

## See also

- [failure-modes.md](failure-modes.md): what happens to observability data
  during outages (lost pub/sub events, stale gauges).
- [security.md](security.md): why payloads and keys stay out of logs and
  metrics labels.
- [deployment.md](deployment.md): compose topology, worker scaling, and the
  metrics-port limitation.
