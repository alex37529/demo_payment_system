import { useState } from "react";
import { formatMoney, formatTime } from "../format";
import type { Payment } from "../types";

// phase-6 spec §4: the live feed as a table (same style as UsersTable), newest
// first. Columns: Event, Account, Amount, Time. Scrollable inside the fixed-height
// panel. A plain-text filter narrows rows by account_id (case-insensitive substring).
const BADGE: Record<string, string> = {
  FundsDebited: "ok",
  FundsReserved: "info",
  CancelReservationPosted: "warn",
  RefundPosted: "warn",
};

function amountCell(p: Payment): string | null {
  const a = p.amount;
  if (a == null) return null;
  if (typeof a === "number") return formatMoney(a);
  if (a.value != null) return formatMoney(a.value, a.currency);
  return null;
}

export function PaymentLog({ payments }: { payments: Payment[] }) {
  const [filter, setFilter] = useState("");
  const q = filter.trim().toLowerCase();
  const rows = q ? payments.filter((p) => p.account_id.toLowerCase().includes(q)) : payments;

  return (
    <div className="panel">
      <h2>
        Payments{" "}
        <span className="muted">
          ({q ? `${rows.length} / ` : ""}live, last {payments.length})
        </span>
      </h2>
      <div className="toolbar">
        <input
          className="filter"
          type="text"
          placeholder="Фильтр по Account (например U001)"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          aria-label="Фильтр по Account"
        />
      </div>
      {payments.length === 0 ? (
        <div className="empty">Waiting for the first payment…</div>
      ) : rows.length === 0 ? (
        <div className="empty">Нет платежей по «{filter.trim()}»</div>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Event</th>
                <th>Account</th>
                <th className="num">Amount</th>
                <th className="num">Time</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((p) => (
                <tr key={`${p.payment_id}|${p.event}|${p.ts}`}>
                  <td>
                    <span className={"badge " + (BADGE[p.event] ?? "muted")}>{p.event}</span>
                  </td>
                  <td>{p.account_id}</td>
                  <td className="num money">{amountCell(p)}</td>
                  <td className="num muted">{formatTime(p.ts)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
