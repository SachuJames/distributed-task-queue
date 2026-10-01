import { useEffect, useMemo, useState } from "react";
import { api, ApiError } from "../lib/api";
import { usePoll, isStale } from "../lib/usePoll";
import { useWebSocket } from "../lib/useWebSocket";
import { Task, TaskEvent, TaskStatus, TERMINAL_STATUSES } from "../lib/types";
import { formatAgo, formatDateTime, formatMs, shortId } from "../lib/format";
import { StatusBadge } from "../components/Badges";

const FILTERS = ["all", "pending", "queued", "running", "retrying", "succeeded", "failed", "dead_lettered", "cancelled", "expired"] as const;

function TaskDrawer({
  taskId,
  events,
  onClose,
}: {
  taskId: string;
  events: TaskEvent[];
  onClose: () => void;
}) {
  const [task, setTask] = useState<Task | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [cancelling, setCancelling] = useState(false);
  const [cancelMsg, setCancelMsg] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api
      .getTask(taskId)
      .then((t) => {
        if (!cancelled) {
          setTask(t);
          setError(null);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof ApiError && err.status === 404 ? "Task not found" : String(err));
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [taskId]);

  const history = useMemo(
    () => events.filter((e) => e.task_id === taskId).slice().reverse(),
    [events, taskId],
  );

  const cancel = async () => {
    if (!task || TERMINAL_STATUSES.includes(task.status)) return;
    setCancelling(true);
    setCancelMsg(null);
    try {
      await api.cancelTask(taskId);
      const refreshed = await api.getTask(taskId);
      setTask(refreshed);
      setCancelMsg("Cancel recorded.");
    } catch (err) {
      setCancelMsg(err instanceof ApiError ? err.message : String(err));
    } finally {
      setCancelling(false);
    }
  };

  return (
    <div className="drawer-backdrop" onClick={onClose}>
      <div className="drawer" onClick={(e) => e.stopPropagation()}>
        <div className="drawer-head">
          <h3 title={taskId}>{shortId(taskId)}</h3>
          <button className="btn" onClick={onClose}>
            Close
          </button>
        </div>
        {loading && <p className="muted">Loading task...</p>}
        {error && <div className="banner bad">{error}</div>}
        {task && (
          <>
            <div className="detail-grid">
              <Field label="Type" value={task.task_type} />
              <Field label="Status" value={<StatusBadge status={task.status} />} />
              <Field label="Queue" value={task.queue} />
              <Field label="Priority" value={String(task.priority)} />
              <Field label="Attempt" value={`${task.attempt} / ${task.max_attempts}`} />
              <Field label="Timeout" value={formatMs(task.timeout_ms)} />
              <Field label="Worker" value={task.worker_id ? shortId(task.worker_id) : "n/a"} />
              <Field label="Duration" value={formatMs(task.duration_ms)} />
              <Field
                label="Idempotency key"
                value={task.idempotency_key ?? "none"}
                mono
              />
              <Field label="Created" value={formatDateTime(task.created_at)} />
              <Field label="Available" value={formatDateTime(task.available_at)} />
              <Field label="Started" value={formatDateTime(task.started_at)} />
              <Field label="Finished" value={formatDateTime(task.finished_at)} />
            </div>
            {(task.error_class || task.error_message) && (
              <div className="detail-section">
                <h4>Final error</h4>
                <p>
                  <code>{task.error_class ?? "unknown"}</code>
                </p>
                <pre className="payload">{task.error_message ?? ""}</pre>
                <p className="muted">
                  Retryable: {task.retryable === null ? "n/a" : task.retryable ? "yes" : "no"}
                  {task.retry_delay_ms !== null && `, last retry delay ${formatMs(task.retry_delay_ms)}`}
                </p>
              </div>
            )}
            <div className="detail-section">
              <h4>Payload</h4>
              <pre className="payload">{JSON.stringify(task.payload, null, 2)}</pre>
            </div>
            <div className="detail-section">
              <h4>Metadata</h4>
              <pre className="payload">{JSON.stringify(task.metadata, null, 2)}</pre>
            </div>
            <div className="detail-section">
              <h4>Event history ({history.length} in live buffer)</h4>
              {history.length === 0 ? (
                <p className="muted">
                  No events for this task in the live buffer. Older events are not retained by the
                  dashboard.
                </p>
              ) : (
                <ul className="event-list">
                  {history.map((e) => (
                    <li key={e.event_id}>
                      <code>{e.type}</code>{" "}
                      <span className="muted">
                        {formatDateTime(e.ts)}
                        {e.worker_id ? ` on ${shortId(e.worker_id)}` : ""}
                        {e.attempt !== null ? ` (attempt ${e.attempt})` : ""}
                      </span>
                    </li>
                  ))}
                </ul>
              )}
            </div>
            <div className="detail-section row">
              <button
                className="btn danger"
                disabled={cancelling || TERMINAL_STATUSES.includes(task.status)}
                onClick={cancel}
              >
                {cancelling ? "Cancelling..." : "Cancel task"}
              </button>
              {cancelMsg && <span className="muted">{cancelMsg}</span>}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function Field({ label, value, mono = false }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="field">
      <div className="field-label">{label}</div>
      <div className={mono ? "field-value mono" : "field-value"}>{value}</div>
    </div>
  );
}

export function Tasks({ ws }: { ws: ReturnType<typeof useWebSocket> }) {
  const [filter, setFilter] = useState<(typeof FILTERS)[number]>("all");
  const [queue, setQueue] = useState("");
  const [selected, setSelected] = useState<string | null>(null);

  const tasks = usePoll(
    () =>
      api.listTasks({
        queue: queue || undefined,
        status: filter === "all" ? undefined : filter,
        limit: 50,
      }),
    10000,
  );

  // Live-merge WS events so task statuses update without waiting for the next poll.
  const statusById = useMemo(() => {
    const m = new Map<string, TaskStatus>();
    for (const e of ws.events) {
      if (!e.task_id) continue;
      const s = eventToStatus(e.type);
      if (s) m.set(e.task_id, s);
    }
    return m;
  }, [ws.events]);

  const merged = useMemo(() => {
    const base = tasks.data ?? [];
    return base.map((t) => {
      const live = statusById.get(t.task_id);
      return live && live !== t.status ? { ...t, status: live } : t;
    });
  }, [tasks.data, statusById]);

  const stale = isStale(tasks.lastUpdated);

  return (
    <div>
      <div className="page-head">
        <h2>Tasks</h2>
        <div className="row">
          {stale && <span className="stale-tag">stale</span>}
          <button className="btn" onClick={tasks.refresh}>
            Refresh
          </button>
        </div>
      </div>
      {tasks.error && <div className="banner bad">API error: {tasks.error}</div>}
      {ws.status !== "CONNECTED" && (
        <div className="banner warn">WebSocket {ws.status.toLowerCase()}: live updates paused.</div>
      )}
      <div className="filters">
        {FILTERS.map((f) => (
          <button
            key={f}
            className={`btn small ${filter === f ? "primary" : ""}`}
            onClick={() => setFilter(f)}
          >
            {f}
          </button>
        ))}
        <input
          className="filter-input"
          placeholder="queue filter"
          value={queue}
          onChange={(e) => setQueue(e.target.value)}
        />
      </div>
      {merged.length === 0 && !tasks.error ? (
        <div className="empty">No tasks match. Submit a task to see it here.</div>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Type</th>
              <th>Queue</th>
              <th>Status</th>
              <th>Attempt</th>
              <th>Priority</th>
              <th>Worker</th>
              <th>Created</th>
            </tr>
          </thead>
          <tbody>
            {merged.map((t) => (
              <tr key={t.task_id} onClick={() => setSelected(t.task_id)} className="clickable">
                <td className="mono" title={t.task_id}>
                  {shortId(t.task_id)}
                </td>
                <td>{t.task_type}</td>
                <td>{t.queue}</td>
                <td>
                  <StatusBadge status={t.status} />
                </td>
                <td>
                  {t.attempt}/{t.max_attempts}
                </td>
                <td>{t.priority}</td>
                <td className="mono">{t.worker_id ? shortId(t.worker_id) : "-"}</td>
                <td className="muted">{formatAgo(t.created_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {selected && (
        <TaskDrawer taskId={selected} events={ws.events} onClose={() => setSelected(null)} />
      )}
    </div>
  );
}

function eventToStatus(type: string): TaskStatus | null {
  switch (type) {
    case "TASK_QUEUED":
    case "TASK_RETRIED":
      return "queued";
    case "TASK_STARTED":
      return "running";
    case "TASK_SUCCEEDED":
      return "succeeded";
    case "TASK_FAILED":
      return "failed";
    case "TASK_RETRY_SCHEDULED":
      return "retrying";
    case "TASK_DEAD_LETTERED":
      return "dead_lettered";
    case "TASK_CANCELLED":
      return "cancelled";
    default:
      return null;
  }
}
