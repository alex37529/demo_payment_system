// Shared API types (phase 6 spec §4, tz_v2 §5).
export interface User {
  id: string;
  name: string;
  email: string;
  currency: string;
  balance: number; // minor units (cents)
}

export interface Payment {
  payment_id: string;
  account_id: string;
  event: string; // FundsReserved | FundsDebited | CancelReservationPosted | ...
  amount: { value: number; currency: string } | number | null;
  new_balance: { value: number; currency: string } | number | null;
  ts: string;
  event_id: string;
}

export type SSEStatus = "open" | "closed" | "connecting";

export interface Status {
  up: boolean;
  circuit_breakers?: {
    fraud?: { state?: string; is_open?: boolean } | string;
    [k: string]: unknown;
  };
}
