import { useEffect, useState } from "react";
import { fetchPayments } from "../api/client";
import type { Payment, SSEStatus } from "../types";

const MAX = 100;

// phase-6 spec §4.2: EventSource auto-reconnects built-in (es.onclose -> the
// browser retries). We cap the buffer at the last 100 and top-up from the REST
// feed on first load / after a reconnect (no `Last-Event-ID` resume in the PoC —
// the feed is a recent-window).
export function useSSE(url: string) {
  const [payments, setPayments] = useState<Payment[]>([]);
  const [status, setStatus] = useState<SSEStatus>("connecting");
  const [eventCount, setEventCount] = useState(0); // monotonic — every SSE event

  useEffect(() => {
    fetchPayments(MAX)
      .then((rows) => setPayments((prev) => merge(rows, prev)))
      .catch(() => undefined);

    const es = new EventSource(url);
    es.onopen = () => setStatus("open");
    es.onmessage = (e) => {
      if (!e.data) return;
      try {
        const p = JSON.parse(e.data) as Payment;
        setPayments((prev) => merge([p], prev));
        setEventCount((n) => n + 1); // nudge dependents (UsersTable) on each event
      } catch {
        /* keepalive / malformed: ignore */
      }
    };
    es.onerror = () => {
      setStatus("closed");
      console.warn("SSE disconnected, EventSource will auto-reconnect");
    };
    return () => {
      es.close();
    };
  }, [url]);

  return { payments, status, eventCount };
}

// merge keeps newest-first, dedupes by payment_id+event (so a REST/SSE overlap
// doesn't double a row), caps at MAX.
function merge(next: Payment[], prev: Payment[]): Payment[] {
  const seen = new Set<string>(prev.map(({ payment_id, event, ts }) => `${payment_id}|${event}|${ts}`));
  const out: Payment[] = [];
  for (const p of next) {
    const k = `${p.payment_id}|${p.event}|${p.ts}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(p);
    if (out.length >= MAX) break;
  }
  return [...out, ...prev].slice(0, MAX);
}
