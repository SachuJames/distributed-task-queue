import { api } from "../lib/api";
import { usePoll, isStale } from "../lib/usePoll";
import { formatAgo, formatDateTime, shortId } from "../lib/format";
import { WorkerStatusBadge } from "../components/Badges";

/**
 * STALE is observer-computed (contract section 6): last_heartbeat older than
 * 3x the heartbeat interval. Default interval is 5s, so the threshold is 15s.
 */
const STALE_AFTER_MS = 15000;

function heartbeatAgeMs(lastHeartbeat: string): number | null {
  const t = new Date(lastHeartbeat).getTime();
  if (Number.isNaN(t)) return null;
  return Date.now() - t;
}

export function Workers() {
  const workers = usePoll(() => api.listWorkers(), 5000);
  const stale = isStale(workers.lastUpdated, 15000);

  return (
    <div>
      <div className="page-head">
        <h2>Workers</h2>
        <div className="row">
          {stale && <span className="stale-tag">stale</span>}
          <button className="btn" onClick={workers.refresh}>
            Refresh
          </button>
        </div>
      </div>
      {workers.error && <div className="banner bad">API error: {workers.error}</div>}
      {workers.data && workers.data.length === 0 && !workers.error ? (
        <div className="empty">No workers registered. Start a worker to see it here.</div>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Status</th>
              <th>Heartbeat</th>
              <th>Concurrency</th>
              <th>In flight</th>
              <th>Processed</th>
              <th>Failed</th>
              <th>Retried</th>
              <th>Hostname</th>
              <th>Started</th>
            </tr>
          </thead>
          <tbody>
            {(workers.data ?? []).map((w) => {
              const age = heartbeatAgeMs(w.last_heartbeat);
              const isStaleWorker = age === null || age > STALE_AFTER_MS;
              return (
                <tr key={w.worker_id}>
                  <td className="mono" title={w.worker_id}>
                    {shortId(w.worker_id)}
                  </td>
                  <td>
                    <WorkerStatusBadge status={w.status} stale={isStaleWorker} />
                  </td>
                  <td className={isStaleWorker ? "bad" : "muted"}>
                    {age === null ? "unknown" : formatAgo(w.last_heartbeat)}
                  </td>
                  <td>{w.concurrency}</td>
                  <td>{w.active_tasks}</td>
                  <td>{w.tasks_processed}</td>
                  <td>{w.tasks_failed}</td>
                  <td>{w.tasks_retried}</td>
                  <td className="muted">{w.hostname}</td>
                  <td className="muted">{formatDateTime(w.started_at)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
      <p className="muted">
        Liveness is computed from heartbeat age (stale after 15s without a heartbeat).
      </p>
    </div>
  );
}
