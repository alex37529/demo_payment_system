// REST helpers (phase 6 spec §4.3).
import type { Payment, Status, User } from "../types";

async function getJson<T>(url: string): Promise<T> {
  const r = await fetch(url, { headers: { Accept: "application/json" } });
  if (!r.ok) throw new Error(`${url} -> ${r.status}`);
  return (await r.json()) as T;
}

export function fetchUsers(): Promise<User[]> {
  return getJson<User[]>("/v1/users");
}

export function fetchStatus(): Promise<Status> {
  return getJson<Status>("/v1/_status");
}

// The gateway's REST feed is the catch-up surface (SSE covers live deltas;
// on first load / after a reconnect we top-up from here).
export function fetchPayments(limit = 100): Promise<Payment[]> {
  return getJson<{ count: number; payments: Payment[] }>(`/v1/payments-feed?limit=${limit}`).then(
    (r) => r.payments,
  );
}
