import { useState } from "react";
import { api, ApiError } from "../lib/api";
import { usePoll, isStale } from "../lib/usePoll";
import { QueueDetail, QueueStats } from "../lib/types";
import { PausedBadge } from "../components/Badges";
import { ApiKeyPrompt, Confirm } from "../components/Modals";

/**
 * Exact vs approximate labeling (contract section 11):
 * depth: approx (XLEN summed across priority streams, drifts under concurrent ingest)
 * pending: approx (XPENDING total)
 * paused: exact
 * retry_scheduled: approx
 * dlq_depth: approx (bounded stream)
 * workers_active: exact (heartbeat-liveness count at query time)
 */

function QueueRow({ q, onSelect }: { q: QueueStats; onSelect: () => void }) {
  return (
    <tr onClick={onSelect} className="clickable">
      <td className="mono">{q.queue}</td>
      <td>
        <PausedBadge paused={q.paused} />
      </td>
      <td>{q.depth}</td>
      <td>{q.pending}</td>
      <td>{q.retry_scheduled}</td>
    </tr>
  );
}

export function Queues() {
  const queues = usePoll(() => api.listQueues(), 10000);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<QueueDetail | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [showKeyPrompt, setShowKeyPrompt] = useState(false);
  const [confirmPause, setConfirmPause] = useState<null | "pause" | "resume">(null);
  const [actionMsg, setActionMsg] = useState<string | null>(null);
  const stale = isStale(queues.lastUpdated);

  const select = async (queue: string) => {
    setSelected(queue);
    setDetail(null);
    setDetailError(null);
    try {
      setDetail(await api.getQueue(queue));
    } catch (err) {
      setDetailError(err instanceof ApiError && err.status === 404 ? "Queue not found" : String(err));
    }
  };

  const doPauseAction = async () => {
    if (!selected || !confirmPause) return;
    setActionMsg(null);
    try {
      if (confirmPause === "pause") await api.pauseQueue(selected);
      else await api.resumeQueue(selected);
      setActionMsg(`Queue ${confirmPause}d.`);
      setDetail(await api.getQueue(selected));
      queues.refresh();
    } catch (err) {
      setActionMsg(err instanceof ApiError ? err.message : String(err));
    } finally {
      setConfirmPause(null);
    }
  };

  return (
    <div>
      <div className="page-head">
        <h2>Queues</h2>
        <div className="row">
          {stale && <span className="stale-tag">stale</span>}
          <button className="btn" onClick={queues.refresh}>
            Refresh
          </button>
        </div>
      </div>
      {queues.error && <div className="banner bad">API error: {queues.error}</div>}
      {queues.data && queues.data.length === 0 && !queues.error ? (
        <div className="empty">No queues yet. Submit a task and one will appear here.</div>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>Queue</th>
              <th>State</th>
              <th title="Approx: sum of XLEN across priority streams">Depth (approx)</th>
              <th title="Approx: XPENDING total">Pending (approx)</th>
              <th title="Approx: scheduled retries">Retries scheduled (approx)</th>
            </tr>
          </thead>
          <tbody>
            {(queues.data ?? []).map((q) => (
              <QueueRow key={q.queue} q={q} onSelect={() => select(q.queue)} />
            ))}
          </tbody>
        </table>
      )}

      {selected && (
        <div className="detail-section">
          <h3 className="mono">{selected}</h3>
          {detailError && <div className="banner bad">{detailError}</div>}
          {detail && (
            <>
              <div className="detail-grid">
                <Field label="State" value={<PausedBadge paused={detail.paused} />} />
                <Field label="Depth (approx)" value={String(detail.depth)} />
                <Field label="Pending (approx)" value={String(detail.pending)} />
                <Field label="Retries scheduled (approx)" value={String(detail.retry_scheduled)} />
                <Field label="DLQ depth (approx)" value={String(detail.dlq_depth)} />
                <Field label="Workers active (exact)" value={String(detail.workers_active)} />
              </div>
              <div className="row">
                {detail.paused ? (
                  <button className="btn primary" onClick={() => setConfirmPause("resume")}>
                    Resume queue
                  </button>
                ) : (
                  <button className="btn danger" onClick={() => setConfirmPause("pause")}>
                    Pause queue
                  </button>
                )}
                <button className="btn" onClick={() => setShowKeyPrompt(true)}>
                  Set API key
                </button>
                {actionMsg && <span className="muted">{actionMsg}</span>}
              </div>
            </>
          )}
        </div>
      )}

      {showKeyPrompt && <ApiKeyPrompt onClose={() => setShowKeyPrompt(false)} />}
      {confirmPause && selected && (
        <Confirm
          title={`${confirmPause === "pause" ? "Pause" : "Resume"} queue "${selected}"?`}
          body={
            confirmPause === "pause"
              ? "Workers will stop polling this queue. In-flight tasks keep running."
              : "Workers will resume polling this queue."
          }
          confirmLabel={confirmPause === "pause" ? "Pause" : "Resume"}
          onConfirm={doPauseAction}
          onCancel={() => setConfirmPause(null)}
        />
      )}
    </div>
  );
}

function Field({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="field">
      <div className="field-label">{label}</div>
      <div className="field-value">{value}</div>
    </div>
  );
}
