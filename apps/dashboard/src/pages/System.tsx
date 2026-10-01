import { api } from "../lib/api";
import { usePoll, isStale } from "../lib/usePoll";
import { useWebSocket, WsStatus } from "../lib/useWebSocket";
import { formatDateTime, shortId } from "../lib/format";

export function System({ ws }: { ws: ReturnType<typeof useWebSocket> }) {
  const health = usePoll(() => api.getHealth(), 10000);
  const ready = usePoll(() => api.getReady(), 10000);
  const healthStale = isStale(health.lastUpdated);
  const readyStale = isStale(ready.lastUpdated);

  return (
    <div>
      <div className="page-head">
        <h2>System</h2>
        <span className={ws.status === "CONNECTED" ? "ws-ok" : "ws-bad"}>
          WS: {wsStatusText(ws.status)}
        </span>
      </div>

      <h3>
        Service health {healthStale && <span className="stale-tag">stale</span>}
      </h3>
      {health.error ? (
        <div className="banner bad">API unreachable: {health.error}</div>
      ) : (
        <pre className="payload">{JSON.stringify(health.data, null, 2)}</pre>
      )}

      <h3>
        Readiness {readyStale && <span className="stale-tag">stale</span>}
      </h3>
      {ready.error ? (
        <div className="banner bad">Readiness check failed: {ready.error}</div>
      ) : ready.data && ready.data.status !== "ok" ? (
        <div className="banner bad">
          <strong>Not ready (503 detail):</strong>
          <pre className="payload">{JSON.stringify(ready.data, null, 2)}</pre>
        </div>
      ) : (
        <pre className="payload">{JSON.stringify(ready.data, null, 2)}</pre>
      )}

      <h3>Configuration</h3>
      <p className="muted">
        The API contract does not expose a config endpoint. All DTQ_* settings (DTQ_REDIS_URL,
        DTQ_WORKER_CONCURRENCY, DTQ_MAX_QUEUE_DEPTH, DTQ_API_KEY, and the rest in contract
        section 10) are read from environment variables at startup. The full list is documented
        in the contract.
      </p>

      <h3>
        Event stream ({ws.events.length} buffered){" "}
        <button className="btn small" onClick={ws.clear}>
          Clear
        </button>
      </h3>
      {ws.status !== "CONNECTED" && (
        <div className="banner warn">
          WebSocket {ws.status.toLowerCase()}; the log below only covers what this client has seen.
        </div>
      )}
      {ws.events.length === 0 ? (
        <div className="empty">No events received yet.</div>
      ) : (
        <ul className="event-list">
          {ws.events
            .slice()
            .reverse()
            .slice(0, 200)
            .map((e) => (
              <li key={e.event_id}>
                <code>{e.type}</code>{" "}
                <span className="muted">
                  {formatDateTime(e.ts)}
                  {e.task_id ? ` task ${shortId(e.task_id)}` : ""}
                  {e.queue ? ` queue ${e.queue}` : ""}
                  {e.worker_id ? ` worker ${shortId(e.worker_id)}` : ""}
                </span>
              </li>
            ))}
        </ul>
      )}
    </div>
  );
}

function wsStatusText(s: WsStatus): string {
  return s;
}
