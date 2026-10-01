# Deployment

Running dtq with docker compose, scaling it, configuring it, backing it up,
and restarting it without dropping work.

## Docker compose topology

`docker compose up --build` starts five services on the `dtq` bridge network:

```
                    +------------------+
                    |    dashboard     |  :3000 -> nginx SPA
                    |  (nginx proxy)   |  proxies /api/, /health, /ready,
                    +--------+---------+  /metrics, /ws/ to the API
                             |
                    +--------v---------+       +------------------+
                    |       api        |------>|   prometheus     |
                    |  uvicorn :8000   |  :8000|     :9090        |
                    | /api/v1 /health  |       +------------------+
                    | /ready /metrics  |
                    | /ws/events       |
                    +--------+---------+
                             |  redis://redis:6379/0
              +--------------v---------------+
              |            redis             |
              |   redis:7-alpine, AOF on     |
              |   volume: redis-data         |
              |   no host port published     |
              +--------------^---------------+
                             |
                    +--------+---------+
                    |      worker      |  x N (--scale worker=N)
                    |  dtq worker run |
                    |  no host port    |
                    +------------------+
```

- **redis**: `redis:7-alpine` with `--appendonly yes`, data in the named
  volume `redis-data`. No host port is published; only compose services can
  reach it.
- **api**: built from `docker/Dockerfile.api`, runs uvicorn on port 8000 as
  a non-root user. Healthcheck hits `/health`. Depends on Redis being healthy.
- **worker**: built from `docker/Dockerfile.worker`, runs `dtq worker run`
  as a non-root user. No host port published. The compose file sets
  `DTQ_WORKER_METRICS_PORT=8001` and Prometheus is configured to scrape
  `worker:8001`, but no code in the worker currently serves HTTP on that
  port; see the metrics limitation in [observability.md](observability.md).
  The container is considered healthy while the process is alive; watch
  worker heartbeats for real liveness.
- **dashboard**: built from `docker/Dockerfile.dashboard`, nginx serving the
  React SPA on host port 3000 and proxying API paths to `api:8000`.
- **prometheus**: `prom/prometheus` on host port 9090, config in
  `docker/prometheus.yml`, scraping `api:8000/metrics` every 15s.

## Scaling workers

```
docker compose up --build --scale worker=3
```

Workers coordinate through Redis, so no extra configuration is needed:

- Each worker joins the `workers` consumer group on every priority stream;
  Redis distributes pending entries across group members.
- Strict priority is per worker: each worker drains p9 down to p0.
- Exactly one worker holds the scheduler leader lock at a time
  (`dtq:lock:scheduler`, 10s TTL); the others skip scheduling. Leadership
  moves automatically on failure.
- Heartbeats (`dtq:worker:<id>`, refreshed every
  `DTQ_HEARTBEAT_INTERVAL_S`) let the API and dashboard show per-worker
  liveness; staleness is observer-computed from heartbeat age.

Prometheus scaling caveat: all replicas share the DNS name `worker`, which
round-robins, so the static `worker:8001` target scrapes only one replica
per interval. Give each replica a distinct published metrics port or use
service discovery for per-replica series.

For CPU-bound sync handlers, scale worker *processes* (containers), not the
concurrency setting: sync handlers run in threads and are GIL-bound, so one
container's worth of CPU-bound throughput does not grow with
`DTQ_WORKER_CONCURRENCY`. Async handlers scale with concurrency.

## Environment reference

All settings use the `DTQ_` prefix, are validated at startup, and fail fast
on bad values. The API additionally honors `DTQ_TEST_REDIS_DB` /
`DTQ_TEST_REDIS_URL` for isolated test runs (not for production).

| Variable | Default | Meaning |
|---|---|---|
| `DTQ_REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL (compose sets `redis://redis:6379/0`) |
| `DTQ_API_HOST` | `0.0.0.0` | API bind address |
| `DTQ_API_PORT` | `8000` | API port |
| `DTQ_WS_PATH` | `/ws/events` | WebSocket endpoint path |
| `DTQ_WORKER_CONCURRENCY` | `4` | Max concurrent handler executions per worker |
| `DTQ_WORKER_ID` | (auto `worker-` + 8 hex chars) | Fixed worker identity; leave empty for auto |
| `DTQ_VISIBILITY_TIMEOUT_S` | `30` | Idle time before a pending entry is reclaimable |
| `DTQ_HEARTBEAT_INTERVAL_S` | `5` | Worker heartbeat period; must be `<` visibility timeout (enforced) |
| `DTQ_MAX_QUEUE_DEPTH` | `10000` | Ingest admission gate: sum of `XLEN` across a queue's priority streams |
| `DTQ_MAX_PAYLOAD_BYTES` | `262144` | Max serialized payload size |
| `DTQ_TASK_TIMEOUT_MS` | `30000` | Default handler timeout; hard cap 3600000 |
| `DTQ_MAX_ATTEMPTS` | `10` | Cap for per-task `max_attempts` |
| `DTQ_RETRY_BASE_DELAY` | `1.0` | Base retry delay, seconds |
| `DTQ_RETRY_MAX_DELAY` | `60.0` | Retry delay cap, seconds |
| `DTQ_RETRY_JITTER` | `0.2` | Symmetric jitter fraction on retry delays |
| `DTQ_TASK_RETENTION_S` | `604800` | Task hash TTL, 7 days |
| `DTQ_RESULT_RETENTION_S` | `3600` | Handler result key TTL, 1 hour |
| `DTQ_RESULT_MAX_BYTES` | `8192` | Stored result truncation limit |
| `DTQ_IDEM_TTL_S` | `86400` | Idempotency key TTL, 24 hours |
| `DTQ_DRAIN_TIMEOUT_S` | `30` | Graceful shutdown drain budget per worker |
| `DTQ_DLQ_ENABLED` | `true` | Dead-letter exhausted tasks instead of marking `FAILED` |
| `DTQ_DLQ_MAXLEN` | `10000` | DLQ stream cap (oldest entries trimmed) |
| `DTQ_API_KEY` | (empty) | Admin key; empty means local-dev open mode |
| `DTQ_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `DTQ_LOG_JSON` | `false` | JSON structured logs when `true` |

Put overrides in a `.env` file next to `docker-compose.yml`; the api and
worker services load it automatically (`env_file`, optional).

Cross-field rules enforced at startup: `DTQ_RETRY_MAX_DELAY >=
DTQ_RETRY_BASE_DELAY`, and `DTQ_HEARTBEAT_INTERVAL_S <
DTQ_VISIBILITY_TIMEOUT_S`.

## Resource guidance

No load-test numbers are published here; size from your own workload. The
knobs that matter:

- **Worker containers**: one per few CPU cores for CPU-bound handlers; fewer
  for IO-bound async handlers with higher `DTQ_WORKER_CONCURRENCY`.
- **Redis memory**: bounded by design. Streams are trimmed (DLQ ~10000,
  audit ~10000, recent-tasks zset 10000); task hashes expire after 7 days;
  payloads are capped at 256 KB. Estimate worst case as
  `depth cap * avg payload + schedule + DLQ`, then add headroom and set a
  Redis `maxmemory` policy as a backstop.
- **Prometheus**: 15s scrape interval; the API's `/metrics` recomputes
  gauges from Redis per scrape, which costs a handful of Redis commands.
- **Dashboard**: negligible; it is a static SPA plus one WebSocket per
  browser tab.

## Backup and restore

Redis holds all durable state. The compose setup persists it two ways: the
AOF (`--appendonly yes`) inside the `redis-data` volume.

Backup:

```
# Snapshot to a file (run against the container's Redis)
docker compose exec redis redis-cli BGSAVE
# Copy the dump out
docker compose cp redis:/data/dump.rdb ./dump-$(date +%F).rdb
# Or back up the whole volume
docker run --rm -v distributed-task-queue_redis-data:/data \
  -v "$PWD":/backup alpine tar czf /backup/redis-data.tgz -C /data .
```

Restore: stop the stack, restore the files into the volume (or place
`dump.rdb` / the AOF files into `/data` before first start), then start.
Verify with `redis-cli ping`, the API's `/ready`, and the
`dtq_queue_depth` / `dtq_retry_scheduled` / `dtq_dlq_depth` gauges.

Test restores. An untested backup is not a backup.

## Zero-downtime notes and limits

What is safe:

- **API rolling restarts** are safe: the API is stateless; tasks live in
  Redis. Gate traffic on `/ready`, not `/health`, so a new instance only
  serves after Redis and consumer groups check out. WebSocket clients
  (including the dashboard) reconnect with backoff; they will miss events
  published during the gap.
- **Worker rolling restarts** are safe: send SIGTERM, each worker drains
  in-flight executions for up to `DTQ_DRAIN_TIMEOUT_S`, and unacked entries
  are reclaimed by surviving workers after the visibility timeout. The
  scheduler leader lock expires in 10s and another worker takes over.

Limits to plan around:

- A task executing across a worker restart **will run again** on the new
  worker (at-least-once). Restarts during long handlers duplicate side
  effects unless handlers are idempotent.
- There is no coordinated drain across the whole fleet: scaling down N
  workers at once leaves N workers' worth of in-flight entries to be
  reclaimed after 30s. Roll one at a time for smooth handoff.
- In-flight HTTP submissions during an API restart fail client-side and must
  be retried by the caller; use idempotency keys so the retry is safe.
- Changing `DTQ_VISIBILITY_TIMEOUT_S` or heartbeat intervals requires
  restarting workers; the config validator rejects
  heartbeat >= visibility timeout.

## See also

- [observability.md](observability.md): probes, metrics, and the Prometheus
  setup referenced here.
- [failure-modes.md](failure-modes.md): what survives each kind of restart
  and outage, and the data-loss windows.
- [security.md](security.md): TLS, Redis auth, and network isolation to add
  before exposing this topology.
