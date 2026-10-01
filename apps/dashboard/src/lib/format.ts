/** Small formatting helpers. All dates are UTC as produced by the API. */

export function formatDateTime(iso: string | null | undefined): string {
  if (!iso) return "n/a";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toISOString().replace("T", " ").replace("Z", " UTC");
}

export function formatAgo(iso: string | null | undefined): string {
  if (!iso) return "n/a";
  const d = new Date(iso);
  const ms = Date.now() - d.getTime();
  if (Number.isNaN(ms)) return "n/a";
  if (ms < 0) return "in the future";
  const s = Math.floor(ms / 1000);
  if (s < 5) return "just now";
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  const days = Math.floor(h / 24);
  return `${days}d ago`;
}

export function formatMs(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return "n/a";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

export function shortId(id: string): string {
  return id.length > 12 ? `${id.slice(0, 8)}...${id.slice(-4)}` : id;
}

/**
 * Parse dtq_task_duration_seconds histogram from Prometheus text format.
 * Returns avg and p95 (from cumulative buckets), or null stats when absent.
 */
export function parseDurationStats(text: string): {
  count: number;
  sumSeconds: number;
  avgMs: number;
  p95Ms: number | null;
} | null {
  const name = "dtq_task_duration_seconds";
  let count = 0;
  let sum = 0;
  const buckets: { le: number; count: number }[] = [];
  let found = false;

  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    if (trimmed.startsWith(`${name}_count`)) {
      const v = parseFloat(trimmed.split(/\s+/).pop() ?? "");
      if (!Number.isNaN(v)) {
        count = v;
        found = true;
      }
    } else if (trimmed.startsWith(`${name}_sum`)) {
      const v = parseFloat(trimmed.split(/\s+/).pop() ?? "");
      if (!Number.isNaN(v)) sum = v;
    } else if (trimmed.startsWith(`${name}_bucket{`)) {
      const leMatch = trimmed.match(/le="([^"]+)"/);
      const v = parseFloat(trimmed.split(/\s+/).pop() ?? "");
      if (leMatch && !Number.isNaN(v)) {
        const le = leMatch[1] === "+Inf" ? Infinity : parseFloat(leMatch[1]);
        if (!Number.isNaN(le)) buckets.push({ le, count: v });
      }
    }
  }

  if (!found || count === 0) return null;
  buckets.sort((a, b) => a.le - b.le);
  const target = 0.95 * count;
  let p95: number | null = null;
  for (const b of buckets) {
    if (b.count >= target) {
      p95 = Number.isFinite(b.le) ? b.le * 1000 : null;
      break;
    }
  }
  return { count, sumSeconds: sum, avgMs: (sum / count) * 1000, p95Ms: p95 };
}

/** Events per second over the trailing window, from the WS event buffer. */
export function eventRate(
  events: { ts: string; type: string }[],
  types: ReadonlySet<string>,
  windowMs: number,
): number {
  const cutoff = Date.now() - windowMs;
  let n = 0;
  for (const e of events) {
    const t = new Date(e.ts).getTime();
    if (!Number.isNaN(t) && t >= cutoff && types.has(e.type)) n++;
  }
  return n / (windowMs / 1000);
}
