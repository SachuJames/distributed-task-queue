import { useState } from "react";
import { api, ApiError } from "../lib/api";
import { usePoll, isStale } from "../lib/usePoll";
import { DlqEntry } from "../lib/types";
import { formatDateTime, shortId } from "../lib/format";
import { ApiKeyPrompt, Confirm } from "../components/Modals";

export function Dlq() {
  const dlq = usePoll(() => api.listDlq(50), 10000);
  const [showKeyPrompt, setShowKeyPrompt] = useState(false);
  const [pending, setPending] = useState<null | { action: "requeue" | "discard" | "purge"; taskId?: string }>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const stale = isStale(dlq.lastUpdated);

  const run = async () => {
    if (!pending || busy) return;
    setBusy(true);
    setMsg(null);
    try {
      if (pending.action === "requeue" && pending.taskId) {
        await api.requeueDlq(pending.taskId);
        setMsg(`Requeued ${shortId(pending.taskId)}: attempt reset to 0, status queued.`);
      } else if (pending.action === "discard" && pending.taskId) {
        await api.discardDlq(pending.taskId);
        setMsg(`Discarded ${shortId(pending.taskId)}: removed from the DLQ stream, audit recorded.`);
      } else if (pending.action === "purge") {
        await api.purgeDlq();
        setMsg("DLQ purged: stream trimmed to zero, audit recorded.");
      }
      dlq.refresh();
    } catch (err) {
      setMsg(err instanceof ApiError ? `Failed: ${err.message}` : String(err));
    } finally {
      setBusy(false);
      setPending(null);
    }
  };

  const entry = (e: DlqEntry) => (
    <tr key={e.task_id}>
      <td className="mono" title={e.task_id}>
        {shortId(e.task_id)}
      </td>
      <td>{e.task_type}</td>
      <td className="mono">{e.original_queue}</td>
      <td>{e.attempts_made}</td>
      <td className="mono">{e.last_error_class ?? "-"}</td>
      <td className="muted" title={e.last_error ?? ""}>
        {(e.last_error ?? "-").slice(0, 80)}
      </td>
      <td className="muted">{formatDateTime(new Date(e.failed_at_ms).toISOString())}</td>
      <td>
        <div className="row">
          <button
            className="btn small primary"
            disabled={busy}
            onClick={() => setPending({ action: "requeue", taskId: e.task_id })}
          >
            Requeue
          </button>
          <button
            className="btn small danger"
            disabled={busy}
            onClick={() => setPending({ action: "discard", taskId: e.task_id })}
          >
            Discard
          </button>
        </div>
      </td>
    </tr>
  );

  return (
    <div>
      <div className="page-head">
        <h2>Dead letter queue</h2>
        <div className="row">
          {stale && <span className="stale-tag">stale</span>}
          <button className="btn" onClick={() => setShowKeyPrompt(true)}>
            Set API key
          </button>
          <button className="btn danger" disabled={busy} onClick={() => setPending({ action: "purge" })}>
            Purge DLQ
          </button>
          <button className="btn" onClick={dlq.refresh}>
            Refresh
          </button>
        </div>
      </div>
      {dlq.error && <div className="banner bad">API error: {dlq.error}</div>}
      {msg && <div className="banner info">{msg}</div>}
      <p className="muted">
        Requeue resets the attempt count to 0 and keeps the original history in
        metadata.previous_attempts. Discard removes the entry from the DLQ stream and records an
        audit entry; the task hash keeps status dead_lettered with metadata.discarded=true.
      </p>
      {dlq.data && dlq.data.length === 0 && !dlq.error ? (
        <div className="empty">DLQ is empty. Permanently failed tasks will land here.</div>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Type</th>
              <th>Original queue</th>
              <th>Attempts</th>
              <th>Error class</th>
              <th>Last error</th>
              <th>Failed at</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>{(dlq.data ?? []).map(entry)}</tbody>
        </table>
      )}

      {showKeyPrompt && <ApiKeyPrompt onClose={() => setShowKeyPrompt(false)} />}
      {pending && (
        <Confirm
          title={
            pending.action === "purge"
              ? "Purge the entire DLQ?"
              : `${pending.action === "requeue" ? "Requeue" : "Discard"} task ${shortId(pending.taskId ?? "")}?`
          }
          body={
            pending.action === "purge"
              ? "This trims the DLQ stream to zero and records an audit entry. It cannot be undone."
              : pending.action === "requeue"
                ? "Attempt resets to 0, original history is kept in metadata.previous_attempts, status becomes queued."
                : "The entry is removed from the DLQ stream and an audit entry is recorded. The task hash stays dead_lettered with metadata.discarded=true."
          }
          confirmLabel={pending.action === "purge" ? "Purge" : pending.action === "requeue" ? "Requeue" : "Discard"}
          onConfirm={run}
          onCancel={() => setPending(null)}
        />
      )}
    </div>
  );
}
