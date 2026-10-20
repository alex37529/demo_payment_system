import { useEffect, useState } from "react";
import { fetchStatus } from "./api/client";
import { PaymentLog } from "./components/PaymentLog";
import { UsersTable } from "./components/UsersTable";
import { useSSE } from "./hooks/useSSE";
import type { SSEStatus } from "./types";

// phase-6 spec §4.4: one badge for the orchestrator fraud circuit breaker
// (green CLOSED / red OPEN) + the SSE transport status. CB polled every 5s.
function StatusBadge({ sse }: { sse: SSEStatus }) {
  const [cb, setCb] = useState<{ open: boolean; state?: string } | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        const s = await fetchStatus();
        const fraud = s.circuit_breakers?.fraud as
          | { state?: string; is_open?: boolean }
          | string
          | undefined;
        let open = false;
        let state: string | undefined;
        if (typeof fraud === "string") {
          open = fraud.toLowerCase() === "open";
          state = fraud;
        } else if (fraud && typeof fraud === "object") {
          open = Boolean(fraud.is_open);
          state = fraud.state;
        }
        setCb({ open, state });
      } catch {
        setCb(null);
      }
    };
    load();
    const iv = setInterval(load, 5000);
    return () => clearInterval(iv);
  }, []);

  const label = cb ? (cb.open ? "OPEN" : (cb.state ?? "CLOSED").toUpperCase()) : "…";
  const cls = cb ? (cb.open ? "badge-red" : "badge-green") : "badge-gray";
  return (
    <div className="badge-wrap">
      <span className={cls}>CB: {label}</span>
      <span className={"sse " + sse}>SSE: {sse}</span>
    </div>
  );
}

export function App() {
  const { payments, status, eventCount } = useSSE("/v1/stream");
  return (
    <div className="app">
      <header>
        <h1>Fintech Payments</h1>
        <span className="muted">PoC · 8 services · outbox / saga / CQRS / CB</span>
        <StatusBadge sse={status} />
      </header>
      <main>
        <UsersTable tick={eventCount} />
        <PaymentLog payments={payments.slice(0, 50)} />
      </main>
      <footer>
        <span className="muted">
          Gateway :8000 · Profile :8004 · Notification :8005 · Orchestrator :8002 ·
          Kafka · PG ×4 · Redis
        </span>
      </footer>
    </div>
  );
}
