#!/usr/bin/env bash
# DTQ end-to-end demo: six scenarios against a local Redis.
#
# Usage: scripts/demo.sh
# Requires: redis-server on localhost:6379, the project venv at .venv.
# Uses Redis DB 9 (DTQ_DEMO_REDIS_DB) and flushes it first; nothing else is touched.

set -u
set -o pipefail
cd "$(dirname "$0")/.."

DTQ_BIN=".venv/bin/dtq"
DB="${DTQ_DEMO_REDIS_DB:-9}"
export DTQ_TEST_REDIS_DB="$DB"
export DTQ_LOG_LEVEL=WARNING

pass=0
fail=0

say() { printf '\n==> %s\n' "$*"; }
ok()  { pass=$((pass + 1)); printf 'PASS: %s\n' "$*"; }
bad() { fail=$((fail + 1)); printf 'FAIL: %s\n' "$*"; }

need() {
    command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 1; }
}

need redis-cli
[ -x "$DTQ_BIN" ] || { echo "venv CLI not found at $DTQ_BIN; run 'make install' first" >&2; exit 1; }
redis-cli -n "$DB" ping >/dev/null 2>&1 || { echo "redis is not reachable on localhost:6379" >&2; exit 1; }

redis-cli -n "$DB" flushdb >/dev/null

start_worker() {
    # $1: extra env assignments, e.g. "DTQ_VISIBILITY_TIMEOUT_S=3"
    env $1 "$DTQ_BIN" worker run --task-module examples.tasks >/tmp/dtq-demo-worker.log 2>&1 &
    WORKER_PID=$!
    sleep 2
    kill -0 "$WORKER_PID" 2>/dev/null || { echo "worker failed to start; see /tmp/dtq-demo-worker.log" >&2; exit 1; }
}

stop_worker() {
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
}

wait_status() {
    # $1: task id, $2: expected status, $3: timeout seconds
    local deadline=$((SECONDS + $3))
    while [ $SECONDS -lt $deadline ]; do
        status=$("$DTQ_BIN" --task-module examples.tasks get "$1" 2>/dev/null | grep '"status"' | cut -d'"' -f4)
        if [ "$status" = "$2" ]; then return 0; fi
        sleep 1
    done
    return 1
}

submit_id() {
    "$DTQ_BIN" --task-module examples.tasks submit "$@" 2>/dev/null | grep '"task_id"' | cut -d'"' -f4
}

trap 'stop_worker 2>/dev/null; exit 130' INT TERM

# ---------------------------------------------------------------------------
say "Scenario 1: 100 successful tasks"
start_worker ""
ids=""
for i in $(seq 1 100); do
    ids="$ids $(submit_id --type echo_task --payload "{\"i\": $i}")"
done
all_ok=1
for tid in $ids; do
    wait_status "$tid" succeeded 60 || all_ok=0
done
[ "$all_ok" = "1" ] && ok "100/100 echo tasks succeeded" || bad "not all 100 tasks succeeded"
stop_worker

# ---------------------------------------------------------------------------
say "Scenario 2: flaky task fails twice, then succeeds"
export DTQ_RETRY_BASE_DELAY=0.2 DTQ_RETRY_MAX_DELAY=1
start_worker ""
tid=$(submit_id --type flaky_task --payload '{"fail_times": 2}' --max-attempts 5)
if wait_status "$tid" succeeded 60; then
    attempts=$("$DTQ_BIN" --task-module examples.tasks get "$tid" | grep '"attempt"' | head -1 | grep -o '[0-9]*')
    [ "$attempts" = "3" ] && ok "flaky task succeeded on attempt 3" || bad "flaky task attempts=$attempts, want 3"
else
    bad "flaky task never succeeded"
fi
stop_worker
unset DTQ_RETRY_BASE_DELAY DTQ_RETRY_MAX_DELAY

# ---------------------------------------------------------------------------
say "Scenario 3: always-failing task lands in the DLQ"
export DTQ_RETRY_BASE_DELAY=0.2 DTQ_RETRY_MAX_DELAY=1
start_worker ""
tid=$(submit_id --type always_fail_task --payload '{}' --max-attempts 3)
if wait_status "$tid" dead_lettered 60; then
    dlq_count=$("$DTQ_BIN" --task-module examples.tasks dlq list 2>/dev/null | grep -c '"task_id"')
    [ "$dlq_count" -ge 1 ] && ok "task dead-lettered and visible in 'dtq dlq list'" || bad "DLQ list is empty"
else
    bad "task never reached dead_lettered"
fi
stop_worker
unset DTQ_RETRY_BASE_DELAY DTQ_RETRY_MAX_DELAY

# ---------------------------------------------------------------------------
say "Scenario 4: queue saturation returns 429 (backpressure)"
export DTQ_MAX_QUEUE_DEPTH=5
ok_count=0
rejected=0
for i in $(seq 1 7); do
    if submit_id --type echo_task --payload "{\"i\": $i}" >/dev/null; then
        ok_count=$((ok_count + 1))
    else
        rejected=$((rejected + 1))
    fi
done
[ "$ok_count" = "5" ] && [ "$rejected" = "2" ] \
    && ok "5 accepted, 2 rejected with 429 at depth limit 5" \
    || bad "accepted=$ok_count rejected=$rejected, want 5 and 2"
unset DTQ_MAX_QUEUE_DEPTH
# Drain the 5 accepted tasks so later scenarios start clean.
start_worker ""
sleep 6
stop_worker

# ---------------------------------------------------------------------------
say "Scenario 5: worker killed mid-task, peer redelivers"
start_worker "DTQ_VISIBILITY_TIMEOUT_S=3 DTQ_HEARTBEAT_INTERVAL_S=1"
tid=$(submit_id --type sleep_task --payload '{"seconds": 8}' --timeout-ms 30000)
sleep 3  # let the worker pick it up and start sleeping
kill -9 "$WORKER_PID"
wait "$WORKER_PID" 2>/dev/null || true
start_worker "DTQ_VISIBILITY_TIMEOUT_S=3 DTQ_HEARTBEAT_INTERVAL_S=1"
if wait_status "$tid" succeeded 60; then
    attempts=$("$DTQ_BIN" --task-module examples.tasks get "$tid" | grep '"attempt"' | head -1 | grep -o '[0-9]*')
    [ "$attempts" = "2" ] && ok "task redelivered after SIGKILL and succeeded on attempt 2" \
        || bad "task succeeded but attempts=$attempts, want 2"
else
    bad "task was not recovered after worker SIGKILL"
fi
stop_worker

# ---------------------------------------------------------------------------
say "Scenario 6: idempotent duplicate submit executes once"
redis-cli -n "$DB" del "dtq:example:counter:demo-idem" >/dev/null
start_worker ""
tid1=$(submit_id --type counter_task --payload '{"counter": "demo-idem"}' --idempotency-key demo-key-1)
tid2=$(submit_id --type counter_task --payload '{"counter": "demo-idem"}' --idempotency-key demo-key-1)
if [ "$tid1" = "$tid2" ]; then
    wait_status "$tid1" succeeded 60
    count=$(redis-cli -n "$DB" get "dtq:example:counter:demo-idem")
    [ "$count" = "1" ] && ok "duplicate submit returned the same task id; handler ran once" \
        || bad "counter=$count, want exactly 1 execution"
else
    bad "duplicate idempotency key produced two task ids ($tid1 != $tid2)"
fi
stop_worker

# ---------------------------------------------------------------------------
printf '\nDemo result: %d passed, %d failed\n' "$pass" "$fail"
[ "$fail" = "0" ]
