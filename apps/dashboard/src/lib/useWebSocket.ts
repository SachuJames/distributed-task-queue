import { useCallback, useEffect, useRef, useState } from "react";
import { wsEventsUrl } from "./api";
import { TaskEvent, TaskEventType } from "./types";

export type WsStatus = "CONNECTED" | "RECONNECTING" | "DISCONNECTED";

interface Options {
  enabled?: boolean;
  /** Max events kept in the buffer (contract section 7 WS policy). */
  maxBuffer?: number;
  /** Max event ids kept for dedup. */
  maxSeen?: number;
}

const VALID_TYPES: ReadonlySet<string> = new Set([
  "TASK_QUEUED",
  "TASK_STARTED",
  "TASK_SUCCEEDED",
  "TASK_FAILED",
  "TASK_RETRY_SCHEDULED",
  "TASK_RETRIED",
  "TASK_DEAD_LETTERED",
  "TASK_CANCELLED",
  "WORKER_STARTED",
  "WORKER_STOPPED",
  "WORKER_STALE",
  "CONFIGURATION_CHANGED",
]);

function isTaskEvent(value: unknown): value is TaskEvent {
  if (value === null || typeof value !== "object") return false;
  const v = value as Record<string, unknown>;
  return (
    typeof v.event_id === "string" &&
    typeof v.ts === "string" &&
    typeof v.type === "string" &&
    VALID_TYPES.has(v.type)
  );
}

/**
 * Reconnecting WebSocket client for /ws/events.
 * Dedups by event_id and keeps a bounded buffer of recent events.
 * Slow-client policy lives server-side; we also cap locally.
 */
export function useWebSocket(options: Options = {}) {
  const { enabled = true, maxBuffer = 1000, maxSeen = 5000 } = options;
  const [status, setStatus] = useState<WsStatus>("DISCONNECTED");
  const [events, setEvents] = useState<TaskEvent[]>([]);
  const seenRef = useRef<Set<string>>(new Set());
  const seenOrderRef = useRef<string[]>([]);
  const backoffRef = useRef(1000);
  const closedRef = useRef(false);

  const clear = useCallback(() => {
    setEvents([]);
    seenRef.current.clear();
    seenOrderRef.current = [];
  }, []);

  useEffect(() => {
    if (!enabled) return;
    closedRef.current = false;
    backoffRef.current = 1000;
    let ws: WebSocket | null = null;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const connect = () => {
      if (closedRef.current) return;
      setStatus((s) => (s === "CONNECTED" ? s : "RECONNECTING"));
      try {
        ws = new WebSocket(wsEventsUrl());
      } catch {
        scheduleReconnect();
        return;
      }

      ws.onopen = () => {
        backoffRef.current = 1000;
        setStatus("CONNECTED");
      };

      ws.onmessage = (msg: MessageEvent) => {
        let parsed: unknown;
        try {
          parsed = JSON.parse(msg.data as string) as unknown;
        } catch {
          return;
        }
        if (!isTaskEvent(parsed)) return;
        const ev = parsed as TaskEvent;
        if (seenRef.current.has(ev.event_id)) return;
        seenRef.current.add(ev.event_id);
        seenOrderRef.current.push(ev.event_id);
        if (seenOrderRef.current.length > maxSeen) {
          const drop = seenOrderRef.current.splice(0, seenOrderRef.current.length - maxSeen);
          for (const id of drop) seenRef.current.delete(id);
        }
        setEvents((prev) => {
          const next = [...prev, ev];
          if (next.length > maxBuffer) {
            return next.slice(next.length - maxBuffer);
          }
          return next;
        });
      };

      ws.onerror = () => {
        ws?.close();
      };

      ws.onclose = () => {
        if (closedRef.current) return;
        setStatus("RECONNECTING");
        scheduleReconnect();
      };
    };

    const scheduleReconnect = () => {
      if (closedRef.current) return;
      const delay = Math.min(backoffRef.current, 30000);
      backoffRef.current = Math.min(backoffRef.current * 2, 30000);
      timer = setTimeout(connect, delay);
    };

    connect();

    return () => {
      closedRef.current = true;
      if (timer) clearTimeout(timer);
      setStatus("DISCONNECTED");
      ws?.close();
    };
  }, [enabled, maxBuffer, maxSeen]);

  return { status, events, clear };
}

export function isValidEventType(t: string): t is TaskEventType {
  return VALID_TYPES.has(t);
}
