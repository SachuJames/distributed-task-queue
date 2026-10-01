import { useEffect, useMemo, useState } from "react";
import { api } from "../lib/api";
import { usePoll, isStale } from "../lib/usePoll";
import { useWebSocket, WsStatus } from "../lib/useWebSocket";
import { eventRate, formatMs, parseDurationStats } from "../lib/format";
import { Sparkline } from "../components/Sparkline";

const RATE_WINDOW_MS = 60000;
const SUBMITTED = new Set(["TASK_QUEUED", "TASK_RETRIED"]);
const SUCCEEDED = new Set(["TASK_SUCCEEDED"]);
const FAILED = new Set(["TASK_FAILED"]);
const RETRIED = new Set(["TASK_RETRY_SCHEDULED"]);
const DEAD_LETTERED = new Set(["TASK_DEAD_LETTERED"]);

function WsDot({ status }: { status: WsStatus }) {
  const cls =
    status === "CONNECTED" ? "dot ok" : status === "RECONNECTING" ? "dot warn" : "dot bad";
  return (
    <span className="ws-status" title={`WebSocket ${status}`}>
      <span className={cls} />
      {status}
    </span>
  );
}

export function Overview({ ws }: { ws: ReturnType<typeof useWebSocket> }) {
  const ready = usePoll(() => api.getReady(), 10000);
  const queues = usePoll(() => api.listQueues(), 10000);
  const workers = usePoll(() => api.listWorkers(), 10000);
  const [metricsText, setMetricsText] = useState<string | null>(null);
  const [metricsAt, setMetricsAt] = useState<number | null>(null);
  const [metricsError, setMetricsError] = useState<string | null>(null);
  const [rateHistory, setRateHistory] = useState<number[]>([]);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const text = await api.getMetrics();
        if (!cancelled) {
          setMetricsText(text);
          setMetricsAt(Date.now());
          setMetricsError(null);
        }
      } catch (err) {
        if (!cancelled) setMetricsError(String(err));
      }
    };
    load();
    const id = setInterval(load, 15000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const duration = useMemo(
    () => (metricsText ? parseDurationStats(metricsText) : null),
    [metricsText],
  );

  const ingestRate = eventRate(ws.events, SUBMITTED, RATE_WINDOW_MS);
  const successRate = eventRate(ws.events, SUCCEEDED, RATE_WINDOW_MS);
  const failureRate = eventRate(ws.events, FAILED, RATE_WINDOW_MS);
  const retryRate = eventRate(ws.events, RETRIED, RATE_WINDOW_MS);
  const dlqRate = eventRate(ws.events, DEAD_LETTERED, RATE_WINDOW_MS);

  useEffect(() => {
    const id = setInterval(() => {
      setRateHistory((prev) => [...prev.slice(-59), eventRate(ws.events, SUCCEEDED, RATE_WINDOW_MS)]);
    }, 5000);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ws.events]);

  const redisDown = ready.error !== null || ready.data?.status !== "ok";
  const queuesStale = isStale(queues.lastUpdated);
  const workersStale = isStale(workers.lastUpdated);
  const metricsStale = isStale(metricsAt, 30000);

  const totalDepth = queues.data?.reduce((n, q) => n + q.depth, 0) ?? 0;
  const totalRetry = queues.data?.reduce((n, q) => n + q.retry_scheduled, 0) ?? 0;
  const activeWorkers = workers.data?.filter((w) => w.status === "ready" || w.status === "busy").length ?? 0;
  const inflight = workers.data?.reduce((n, w) => n + w.active_tasks, 0) ?? 0;

  return (
    <div>
      <div className="page-head">
        <h2>Overview</h2>
        <WsDot status={ws.status} />
      </div>

      {ready.error && (
        <div className="banner bad">
          <strong>API unreachable:</strong> {ready.error}. The dashboard cannot load live data.
        </div>
      )}
      {!ready.error && ready.data && ready.data.status !== "ok" && (
        <div className="banner bad">
          <strong>Not ready:</strong> {JSON.stringify(ready.data)}. Redis or consumer groups are
          unavailable; queues and workers may be empty or stale.
        </div>
      )}
      {ws.status !== "CONNECTED" && (
        <div className="banner warn">
          WebSocket {ws.status.toLowerCase()}. Rates below are computed from live events only;
          anything shown is labeled as it stands.
        </div>
      )}

      <div className="cards">
        <div className="card">
          <div className="card-title">Redis</div>
          <div className={`card-value ${redisDown ? "bad" : "ok"}`}>
            {ready.error ? "API down" : ready.data?.status === "ok" ? "healthy" : "not ready"}
          </div>
          <div className="card-sub">via /ready, 10s poll</div>
        </div>
        <div className="card">
          <div className="card-title">
            Queue depth {queuesStale && <span className="stale-tag">stale</span>}
          </div>
          <div className="card-value">{queues.error ? "n/a" : totalDepth}</div>
          <div className="card-sub">sum of priority streams, approx under concurrent ingest</div>
        </div>
        <div className="card">
          <div className="card-title">
            Successes/sec (live events)
            {ws.status !== "CONNECTED" && <span className="stale-tag">stale</span>}
          </div>
          <div className="card-value">{successRate.toFixed(2)}</div>
          <div className="card-sub">trailing 60s from WS events, approx</div>
          <Sparkline values={rateHistory} label="successes per second" />
        </div>
        <div className="card">
          <div className="card-title">
            Failures/sec (live events)
            {ws.status !== "CONNECTED" && <span className="stale-tag">stale</span>}
          </div>
          <div className="card-value bad">{failureRate.toFixed(2)}</div>
          <div className="card-sub">trailing 60s from WS events, approx</div>
        </div>
        <div className="card">
          <div className="card-title">
            Ingest/sec (live events)
            {ws.status !== "CONNECTED" && <span className="stale-tag">stale</span>}
          </div>
          <div className="card-value">{ingestRate.toFixed(2)}</div>
          <div className="card-sub">trailing 60s from WS events, approx</div>
        </div>
        <div className="card">
          <div className="card-title">
            Retries/sec (live events)
            {ws.status !== "CONNECTED" && <span className="stale-tag">stale</span>}
          </div>
          <div className="card-value warn">{retryRate.toFixed(2)}</div>
          <div className="card-sub">TASK_RETRY_SCHEDULED rate, approx</div>
        </div>
        <div className="card">
          <div className="card-title">
            Dead letters/sec (live events)
            {ws.status !== "CONNECTED" && <span className="stale-tag">stale</span>}
          </div>
          <div className="card-value bad">{dlqRate.toFixed(2)}</div>
          <div className="card-sub">TASK_DEAD_LETTERED rate, approx</div>
        </div>
        <div className="card">
          <div className="card-title">
            Active workers {workersStale && <span className="stale-tag">stale</span>}
          </div>
          <div className="card-value">{workers.error ? "n/a" : activeWorkers}</div>
          <div className="card-sub">
            {workers.error ? workers.error : `${inflight} tasks in flight, ${totalRetry} scheduled retries`}
          </div>
        </div>
        <div className="card">
          <div className="card-title">
            Task duration {metricsStale && <span className="stale-tag">stale</span>}
          </div>
          <div className="card-value">
            {metricsError
              ? "n/a"
              : duration
                ? `avg ${formatMs(duration.avgMs)}`
                : "no completed tasks"}
          </div>
          <div className="card-sub">
            {metricsError
              ? `/metrics: ${metricsError}`
              : duration && duration.p95Ms !== null
                ? `p95 ${formatMs(duration.p95Ms)}, n=${duration.count}, from /metrics histogram`
                : "end-to-end, from Prometheus /metrics"}
          </div>
        </div>
      </div>

      {queues.data && queues.data.length === 0 && (
        <div className="empty">No queues yet. Submit a task and one will appear here.</div>
      )}
      {workers.data && workers.data.length === 0 && (
        <div className="empty">No workers registered. Start a worker to see it here.</div>
      )}
    </div>
  );
}
