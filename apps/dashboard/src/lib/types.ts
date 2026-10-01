/**
 * Domain models mirrored from CONTRACT.md sections 1, 2, 8, 11.
 * No `any` for domain models; payload and metadata fields may be `unknown`.
 */

export type TaskStatus =
  | "pending"
  | "queued"
  | "running"
  | "succeeded"
  | "retrying"
  | "failed"
  | "dead_lettered"
  | "cancelled"
  | "expired";

export const TERMINAL_STATUSES: TaskStatus[] = [
  "succeeded",
  "failed",
  "dead_lettered",
  "cancelled",
  "expired",
];

export interface Task {
  task_id: string;
  idempotency_key: string | null;
  task_type: string;
  payload: unknown;
  created_at: string;
  available_at: string;
  attempt: number;
  max_attempts: number;
  priority: number;
  timeout_ms: number;
  status: TaskStatus;
  queue: string;
  metadata: Record<string, unknown>;
  started_at: string | null;
  finished_at: string | null;
  duration_ms: number | null;
  worker_id: string | null;
  error_class: string | null;
  error_message: string | null;
  retryable: boolean | null;
  retry_delay_ms: number | null;
}

export interface Worker {
  worker_id: string;
  hostname: string;
  started_at: string;
  last_heartbeat: string;
  active_tasks: number;
  concurrency: number;
  tasks_processed: number;
  tasks_failed: number;
  tasks_retried: number;
  status: "starting" | "ready" | "busy" | "draining" | "stopped";
}

/** Contract section 11: GET /api/v1/queues */
export interface QueueStats {
  queue: string;
  depth: number;
  pending: number;
  paused: boolean;
  retry_scheduled: number;
}

/** Contract section 11: GET /api/v1/queues/{queue} */
export interface QueueDetail extends QueueStats {
  dlq_depth: number;
  workers_active: number;
}

/** DLQ stream entry fields, contract section 2. */
export interface DlqEntry {
  task_id: string;
  queue: string;
  task_type: string;
  payload: unknown;
  attempt: number;
  max_attempts: number;
  priority: number;
  timeout_ms: number;
  idempotency_key: string | null;
  failed_at_ms: number;
  attempts_made: number;
  last_error: string | null;
  last_error_class: string | null;
  retryable: boolean | null;
  worker_id: string | null;
  original_queue: string;
}

/** Event envelope, contract section 8. */
export type TaskEventType =
  | "TASK_QUEUED"
  | "TASK_STARTED"
  | "TASK_SUCCEEDED"
  | "TASK_FAILED"
  | "TASK_RETRY_SCHEDULED"
  | "TASK_RETRIED"
  | "TASK_DEAD_LETTERED"
  | "TASK_CANCELLED"
  | "WORKER_STARTED"
  | "WORKER_STOPPED"
  | "WORKER_STALE"
  | "CONFIGURATION_CHANGED";

export interface TaskEvent {
  event_id: string;
  ts: string;
  type: TaskEventType;
  task_id: string | null;
  queue: string | null;
  task_type: string | null;
  attempt: number | null;
  worker_id: string | null;
  metadata?: Record<string, unknown>;
}

/** Ingest response: POST /api/v1/tasks. */
export interface SubmitTaskBody {
  task_type: string;
  payload: Record<string, unknown>;
  queue?: string;
  idempotency_key?: string | null;
  max_attempts?: number;
  priority?: number;
  timeout_ms?: number;
  delay_seconds?: number;
  metadata?: Record<string, unknown>;
}

export interface SubmitTaskResponse {
  task_id: string;
  status: TaskStatus;
  duplicate?: boolean;
}

/** /health and /ready responses. */
export interface HealthResponse {
  status: string;
  [key: string]: unknown;
}

export interface ReadyResponse {
  status: string;
  redis?: unknown;
  groups?: unknown;
  detail?: unknown;
  [key: string]: unknown;
}

/** Parsed summary of the dtq_task_duration_seconds histogram from /metrics. */
export interface DurationStats {
  count: number;
  sumSeconds: number;
  avgMs: number;
  p95Ms: number | null;
}

/** Lists may arrive as an envelope or a bare array; normalize at the edge. */
export function asArray<T>(value: unknown, key: string): T[] {
  if (Array.isArray(value)) return value as T[];
  if (value !== null && typeof value === "object") {
    const nested = (value as Record<string, unknown>)[key];
    if (Array.isArray(nested)) return nested as T[];
    const values = Object.values(value as Record<string, unknown>);
    if (values.length > 0 && values.every((v) => v !== null && typeof v === "object")) {
      return values as T[];
    }
  }
  return [];
}

export function isTaskStatus(value: unknown): value is TaskStatus {
  return (
    typeof value === "string" &&
    [
      "pending",
      "queued",
      "running",
      "succeeded",
      "retrying",
      "failed",
      "dead_lettered",
      "cancelled",
      "expired",
    ].includes(value)
  );
}
