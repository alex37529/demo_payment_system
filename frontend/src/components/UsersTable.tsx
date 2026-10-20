import { useEffect, useState } from "react";
import { fetchUsers } from "../api/client";
import { formatMoney } from "../format";
import type { User } from "../types";

// phase-6 spec §4.3: read-only table of the 20 seeded users. Refresh on an SSE
// event (so the balance updates live) + a 5s poll as a fallback.
export function UsersTable({ tick }: { tick: number }) {
  const [users, setUsers] = useState<User[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const rows = await fetchUsers();
        if (alive) {
          setUsers(rows);
          setError(null);
        }
      } catch (e) {
        if (alive) setError(e instanceof Error ? e.message : "failed to load users");
      }
    };
    load();
    const iv = setInterval(load, 5000);
    return () => {
      alive = false;
      clearInterval(iv);
    };
  }, [tick]);

  if (error && users.length === 0) {
    return <div className="panel error">Users: {error}</div>;
  }
  return (
    <div className="panel">
      <h2>
        Users <span className="muted">({users.length})</span>
      </h2>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>ID</th>
              <th>Currency</th>
              <th className="num">Balance</th>
            </tr>
          </thead>
          <tbody>
            {users.map((u) => (
              <tr key={u.id}>
                <td>{u.name}</td>
                <td className="muted">{u.id}</td>
                <td>{u.currency}</td>
                <td className="num money">{formatMoney(u.balance, u.currency)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
