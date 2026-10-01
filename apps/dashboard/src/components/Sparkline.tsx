interface Props {
  /** Values, oldest first. Rendered as a hand-rolled SVG line chart. */
  values: number[];
  width?: number;
  height?: number;
  label: string;
}

/** Minimal SVG sparkline; empty data renders an empty-state, never fake data. */
export function Sparkline({ values, width = 220, height = 48, label }: Props) {
  if (values.length === 0) {
    return (
      <div className="sparkline-empty" style={{ width, height }}>
        no data
      </div>
    );
  }
  const max = Math.max(...values, 0);
  const min = Math.min(...values, 0);
  const span = max - min || 1;
  const step = width / Math.max(values.length - 1, 1);
  const points = values
    .map((v, i) => {
      const x = (i * step).toFixed(1);
      const y = (height - 4 - ((v - min) / span) * (height - 8)).toFixed(1);
      return `${x},${y}`;
    })
    .join(" ");
  const last = values[values.length - 1];
  return (
    <svg width={width} height={height} role="img" aria-label={label} className="sparkline">
      <polyline points={points} fill="none" stroke="#4da3ff" strokeWidth="1.5" />
      <text x={width - 4} y={14} textAnchor="end" className="sparkline-label">
        {last.toFixed(2)}
      </text>
    </svg>
  );
}
