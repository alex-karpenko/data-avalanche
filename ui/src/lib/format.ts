// How the console writes times and sizes: short, in the units a person reads at a glance.

/** A duration in its two largest units: `21 s`, `4m 12s`, `2h 0m`, `1d 1h`. */
export function duration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s} s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86_400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86_400)}d ${Math.floor((s % 86_400) / 3600)}h`;
}

/** How long before `now` (both in ms) `then` was: `8 s ago`, `3 min ago`, `2 h ago`. */
export function ago(then: number, now: number): string {
  const s = Math.max(0, Math.floor((now - then) / 1000));
  if (s < 60) return `${s} s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86_400) return `${Math.floor(s / 3600)} h ago`;
  return `${Math.floor(s / 86_400)} d ago`;
}

const UNITS = ['kB', 'MB', 'GB', 'TB', 'PB'];

/** A size as PostgreSQL's `pg_size_pretty` writes it, in 1024s: `512 bytes`, `3.4 GB`. */
export function bytes(n: number): string {
  if (n < 1024) return `${Math.round(n)} bytes`;
  let v = n / 1024;
  let unit = 0;
  while (v >= 1024 && unit < UNITS.length - 1) {
    v /= 1024;
    unit += 1;
  }
  return `${v < 10 ? v.toFixed(1).replace(/\.0$/, '') : Math.round(v)} ${UNITS[unit]}`;
}

/** The local time of day of `at` (ms): `09:05`, or `09:05:30` with `seconds`. */
export function clock(at: number, seconds = false): string {
  const d = new Date(at);
  const two = (n: number) => String(n).padStart(2, '0');
  const hm = `${two(d.getHours())}:${two(d.getMinutes())}`;
  return seconds ? `${hm}:${two(d.getSeconds())}` : hm;
}
