# Performance

Every number on this page was measured on the machine described below, on
2026-10-01, against a local Redis. Nothing here is estimated, extrapolated,
or claimed from design. Rerun `scripts/load_test.py` to reproduce.

## Test environment

- 2 vCPU (AMD EPYC 9D25), 7 GB RAM
- Redis 8.10.2, local, no persistence (`--save '' --appendonly no`)
- Python 3.12, `redis-py` async client
- Workload: `echo_task` (returns the payload; no I/O, no sleep), default
  priority band p5, `max_attempts=5`
- `scripts/load_test.py --tasks 2000 --workers 4 --db 10`: submits 2000
  tasks through the real ingest pipeline, then runs 4 in-process workers
  (`worker_concurrency=8` each, 32 execution slots total)

## Measured results

| Metric | Value |
|---|---|
| Ingest (submit) rate | 1,135 tasks/s |
| End-to-end completion rate | 262 tasks/s |
| End-to-end latency p50 | 2,356 ms |
| End-to-end latency p95 | 2,893 ms |
| End-to-end latency mean | 2,362 ms |

"End-to-end latency" is `finished_at - created_at` per task hash, sampled
over 500 of the 2000 tasks. Under this burst the submit rate (1,135/s) far
exceeds the completion rate (262/s), so most of the latency is queueing
delay, not execution cost: the last tasks wait ~7s for an execution slot.

Single-task latency with an idle worker (submit to `succeeded`, one worker,
empty queue) measured 106 ms. That is the floor for a no-op handler on this
machine: one claim round-trip, handler invoke, outcome write, event publish,
ACK.

## What limits throughput

Completion rate scales with worker execution slots, not with Redis: the
ingest path (1,135 tasks/s single-threaded) is roughly 4x the completion
rate of 4 workers. To go faster, add workers or raise
`DTQ_WORKER_CONCURRENCY`. The demo script (`scripts/demo.sh`) also shows the
backpressure behavior: once `XLEN` across a queue's bands reaches
`DTQ_MAX_QUEUE_DEPTH`, submits are rejected with 429 instead of queueing
without bound.

Two real bottlenecks were found and fixed during measurement (2026-10-01):

1. **Priority polling blocked only on p9.** The worker's poll loop used to
   issue one XREADGROUP per band with `BLOCK 500` on p9 only, so tasks on
   lower bands waited up to 500 ms to be discovered. Throughput was capped
   near `concurrency / 0.5s` (about 12 tasks/s at concurrency 8). The fix is
   a single XREADGROUP across all ten bands with one BLOCK, which wakes on
   any band while preserving strict p9-first order.
2. **Processed stream entries were never deleted.** XACK marks entries as
   delivered but does not remove them, so priority streams grew without bound
   and the XLEN-based depth gate counted dead entries. The worker now issues
   an atomic XACK+XDEL (Lua) on every completion path.

## Honest limits

- Delivery is at-least-once: a crash between the handler's side effects and
  the completion write causes redelivery, and the retried execution repeats
  those side effects. The numbers above count task completions, not
  side-effect executions.
- The WebSocket event stream is Redis pub/sub: fire-and-forget, no replay.
  Do not use it as an audit record; the audit stream
  (`dtq:stream:audit`) is the durable one.
- These numbers are for trivial no-op handlers on one machine. Real handlers
  doing I/O will be slower, and networked Redis will add round-trip time.
  Measure your own workload before capacity planning.
