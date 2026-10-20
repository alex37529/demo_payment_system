// Format a minor-unit (cents) amount for display: 620000 USD -> "$6,200.00".
export function formatMoney(value: number, currency = "USD"): string {
  const major = value / 100;
  const sym = currency === "EUR" ? "€" : "$";
  return `${sym}${major.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

// Shorten an ISO timestamp to `HH:MM:SS.cc` (hundredths of a second).
// Strips the date and tz offset (+00:00 / Z); keeps the time and rounds the
// fractional part to centiseconds (.152355 -> .15).
export function formatTime(ts: string): string {
  if (!ts) return "";
  const t = ts.replace(/(Z|[-+]\d{2}:\d{2})$/, "").split("T").pop() || ts; // time part only
  const [hms, frac = ""] = t.split(".");
  let [h, m, s] = hms.split(":").map(Number);
  let cs = frac ? Math.round(parseFloat(`0.${frac}`) * 100) : 0; // centiseconds
  if (cs >= 100) { cs -= 100; s += 1; }
  if (s >= 60) { s -= 60; m += 1; }
  if (m >= 60) { m -= 60; h += 1; }
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${pad(h)}:${pad(m)}:${pad(s)}.${pad(cs)}`;
}
