import { TaskStatus } from "../lib/types";

const STATUS_CLASS: Record<TaskStatus, string> = {
  pending: "st-pending",
  queued: "st-queued",
  running: "st-running",
  succeeded: "st-ok",
  retrying: "st-retry",
  failed: "st-bad",
  dead_lettered: "st-bad",
  cancelled: "st-muted",
  expired: "st-muted",
};

export function StatusBadge({ status }: { status: TaskStatus | string }) {
  const cls = STATUS_CLASS[status as TaskStatus] ?? "st-muted";
  return <span className={`badge ${cls}`}>{status}</span>;
}

export function PausedBadge({ paused }: { paused: boolean }) {
  return paused ? <span className="badge st-bad">paused</span> : <span className="badge st-ok">live</span>;
}

export function WorkerStatusBadge({ status, stale }: { status: string; stale: boolean }) {
  if (stale) return <span className="badge st-bad">stale</span>;
  const cls = status === "busy" ? "st-running" : status === "ready" ? "st-ok" : "st-muted";
  return <span className={`badge ${cls}`}>{status}</span>;
}
