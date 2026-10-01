/**
 * Typed client for the DTQ REST API (contract section 11).
 * Base path is /api/v1; /health, /ready, /metrics and /ws live at the root.
 */
import {
  DlqEntry,
  HealthResponse,
  QueueDetail,
  QueueStats,
  ReadyResponse,
  SubmitTaskBody,
  SubmitTaskResponse,
  Task,
  Worker,
  asArray,
} from "./types";

export const API_BASE = "/api/v1";
const API_KEY_STORAGE = "dtq_api_key";

export function getApiKey(): string {
  return localStorage.getItem(API_KEY_STORAGE) ?? "";
}

export function setApiKey(key: string): void {
  if (key) {
    localStorage.setItem(API_KEY_STORAGE, key);
  } else {
    localStorage.removeItem(API_KEY_STORAGE);
  }
}

export class ApiError extends Error {
  status: number;
  code: string | null;
  constructor(status: number, message: string, code: string | null = null) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
}

interface ErrorBody {
  detail?: unknown;
  code?: unknown;
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  const apiKey = getApiKey();
  if (apiKey) headers.set("X-API-Key", apiKey);
  if (init.body !== undefined && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  let res: Response;
  try {
    res = await fetch(`${API_BASE}${path}`, { ...init, headers });
  } catch (err) {
    throw new ApiError(0, `Network error: API unreachable (${String(err)})`);
  }

  if (res.status === 204) return undefined as T;

  let body: unknown = null;
  const text = await res.text();
  if (text) {
    try {
      body = JSON.parse(text) as unknown;
    } catch {
      body = text;
    }
  }

  if (!res.ok) {
    let message = `Request failed (${res.status})`;
    let code: string | null = null;
    if (body !== null && typeof body === "object") {
      const eb = body as ErrorBody;
      if (typeof eb.detail === "string") message = eb.detail;
      else if (eb.detail !== undefined) message = JSON.stringify(eb.detail);
      if (typeof eb.code === "string") code = eb.code;
    } else if (typeof body === "string" && body.length > 0) {
      message = body;
    }
    throw new ApiError(res.status, message, code);
  }
  return body as T;
}

async function rootRequest<T>(path: string): Promise<T> {
  let res: Response;
  try {
    res = await fetch(path);
  } catch (err) {
    throw new ApiError(0, `Network error: API unreachable (${String(err)})`);
  }
  const text = await res.text();
  let body: unknown = text;
  if (text) {
    try {
      body = JSON.parse(text) as unknown;
    } catch {
      body = text;
    }
  }
  if (!res.ok) {
    const message =
      body !== null && typeof body === "object" && "detail" in body
        ? String((body as { detail: unknown }).detail)
        : `Request failed (${res.status})`;
    throw new ApiError(res.status, message);
  }
  return body as T;
}

async function rootText(path: string): Promise<string> {
  let res: Response;
  try {
    res = await fetch(path);
  } catch (err) {
    throw new ApiError(0, `Network error: API unreachable (${String(err)})`);
  }
  if (!res.ok) throw new ApiError(res.status, `Request failed (${res.status})`);
  return res.text();
}

export interface TaskListParams {
  queue?: string;
  status?: string;
  limit?: number;
}

function toParams(params: TaskListParams): string {
  const sp = new URLSearchParams();
  if (params.queue) sp.set("queue", params.queue);
  if (params.status) sp.set("status", params.status);
  if (params.limit !== undefined) sp.set("limit", String(params.limit));
  const q = sp.toString();
  return q ? `?${q}` : "";
}

export const api = {
  // Tasks
  submitTask(body: SubmitTaskBody, idempotencyKey?: string): Promise<SubmitTaskResponse> {
    const headers: Record<string, string> = {};
    if (idempotencyKey) headers["Idempotency-Key"] = idempotencyKey;
    return request<SubmitTaskResponse>("/tasks", {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });
  },

  async listTasks(params: TaskListParams = {}): Promise<Task[]> {
    const raw = await request<unknown>(`/tasks${toParams(params)}`);
    return asArray<Task>(raw, "tasks");
  },

  getTask(taskId: string): Promise<Task> {
    return request<Task>(`/tasks/${encodeURIComponent(taskId)}`);
  },

  cancelTask(taskId: string): Promise<unknown> {
    return request<unknown>(`/tasks/${encodeURIComponent(taskId)}/cancel`, {
      method: "POST",
    });
  },

  // Queues
  async listQueues(): Promise<QueueStats[]> {
    const raw = await request<unknown>("/queues");
    const items = asArray<Record<string, unknown>>(raw, "queues");
    return items.map((item) => ({
      queue: String(item.queue ?? ""),
      depth: Number(item.depth ?? 0),
      pending: Number(item.pending ?? 0),
      paused: Boolean(item.paused ?? false),
      retry_scheduled: Number(item.retry_scheduled ?? 0),
    }));
  },

  getQueue(queue: string): Promise<QueueDetail> {
    return request<QueueDetail>(`/queues/${encodeURIComponent(queue)}`);
  },

  pauseQueue(queue: string): Promise<unknown> {
    return request<unknown>(`/queue/${encodeURIComponent(queue)}/pause`, {
      method: "POST",
    });
  },

  resumeQueue(queue: string): Promise<unknown> {
    return request<unknown>(`/queue/${encodeURIComponent(queue)}/resume`, {
      method: "POST",
    });
  },

  // Workers
  async listWorkers(): Promise<Worker[]> {
    const raw = await request<unknown>("/workers");
    return asArray<Worker>(raw, "workers");
  },

  getWorker(workerId: string): Promise<Worker> {
    return request<Worker>(`/workers/${encodeURIComponent(workerId)}`);
  },

  // DLQ
  async listDlq(limit = 50): Promise<DlqEntry[]> {
    const raw = await request<unknown>(`/dlq?limit=${limit}`);
    return asArray<DlqEntry>(raw, "entries");
  },

  getDlqTask(taskId: string): Promise<DlqEntry> {
    return request<DlqEntry>(`/dlq/${encodeURIComponent(taskId)}`);
  },

  requeueDlq(taskId: string): Promise<unknown> {
    return request<unknown>(`/dlq/${encodeURIComponent(taskId)}/requeue`, {
      method: "POST",
    });
  },

  discardDlq(taskId: string): Promise<unknown> {
    return request<unknown>(`/dlq/${encodeURIComponent(taskId)}/discard`, {
      method: "POST",
    });
  },

  purgeDlq(): Promise<unknown> {
    return request<unknown>("/dlq/purge", { method: "POST" });
  },

  // Root-level
  getHealth(): Promise<HealthResponse> {
    return rootRequest<HealthResponse>("/health");
  },

  getReady(): Promise<ReadyResponse> {
    return rootRequest<ReadyResponse>("/ready");
  },

  getMetrics(): Promise<string> {
    return rootText("/metrics");
  },
};

/** WebSocket URL for /ws/events, derived from the page origin. */
export function wsEventsUrl(): string {
  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${window.location.host}/ws/events`;
}
