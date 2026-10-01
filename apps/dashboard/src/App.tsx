import { useState } from "react";
import { useWebSocket } from "./lib/useWebSocket";
import { Overview } from "./pages/Overview";
import { Tasks } from "./pages/Tasks";
import { Queues } from "./pages/Queues";
import { Workers } from "./pages/Workers";
import { Dlq } from "./pages/Dlq";
import { System } from "./pages/System";

const TABS = ["overview", "tasks", "queues", "workers", "dlq", "system"] as const;
type Tab = (typeof TABS)[number];

export default function App() {
  const [tab, setTab] = useState<Tab>("overview");
  const ws = useWebSocket({ enabled: true });

  return (
    <div className="app">
      <header className="header">
        <div className="brand">DTQ Dashboard</div>
        <nav className="tabs">
          {TABS.map((t) => (
            <button
              key={t}
              className={`tab ${tab === t ? "active" : ""}`}
              onClick={() => setTab(t)}
            >
              {t}
            </button>
          ))}
        </nav>
        <div className={`ws-pill ${ws.status === "CONNECTED" ? "ok" : "bad"}`}>
          WS {ws.status}
        </div>
      </header>
      <main className="main">
        {tab === "overview" && <Overview ws={ws} />}
        {tab === "tasks" && <Tasks ws={ws} />}
        {tab === "queues" && <Queues />}
        {tab === "workers" && <Workers />}
        {tab === "dlq" && <Dlq />}
        {tab === "system" && <System ws={ws} />}
      </main>
      <footer className="footer">
        <span className="muted">
          Live values are labeled. Rates are computed from WebSocket events (approx).
        </span>
      </footer>
    </div>
  );
}
