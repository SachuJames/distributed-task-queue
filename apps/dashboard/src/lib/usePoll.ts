import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "./api";

interface PollState<T> {
  data: T | null;
  error: string | null;
  lastUpdated: number | null;
  refresh: () => void;
}

/**
 * Poll a REST endpoint on an interval. Exposes lastUpdated so the UI can
 * mark data stale instead of silently showing it as live.
 */
export function usePoll<T>(fn: () => Promise<T>, intervalMs: number): PollState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);
  const mountedRef = useRef(true);
  const fnRef = useRef(fn);
  fnRef.current = fn;

  const load = useCallback(async () => {
    try {
      const result = await fnRef.current();
      if (!mountedRef.current) return;
      setData(result);
      setError(null);
      setLastUpdated(Date.now());
    } catch (err) {
      if (!mountedRef.current) return;
      const msg = err instanceof ApiError ? err.message : String(err);
      setError(msg);
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    load();
    const id = setInterval(load, intervalMs);
    return () => {
      mountedRef.current = false;
      clearInterval(id);
    };
  }, [load, intervalMs]);

  return { data, error, lastUpdated, refresh: load };
}

/** True when the last successful poll is older than the threshold. */
export function isStale(lastUpdated: number | null, thresholdMs = 30000): boolean {
  if (lastUpdated === null) return true;
  return Date.now() - lastUpdated > thresholdMs;
}
