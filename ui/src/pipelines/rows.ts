// The pipeline list (mockup 05): every pipeline, worst first, with what a person scans for; its
// filters and sort live in the address, so a link shows the same view and Back keeps it.

import { ago, duration } from '../lib/format';
import { attention, shortJob } from '../overview/attention';
import { rows, type Row } from '../overview/derive';
import { at, route, tablesOf, type Facts } from '../model/facts';
import type { Actual, Desired, Job, SourceAt } from '../model/types';

export type Show = 'all' | 'attention' | 'running' | 'halted' | 'paused' | 'stopped' | 'snapshot';
export const SHOWS: { id: Show; label: string }[] = [
  { id: 'all', label: 'All' },
  { id: 'attention', label: '⚠ Needs attention' },
  { id: 'running', label: 'Running' },
  { id: 'halted', label: 'Halted tables' },
  { id: 'paused', label: 'Paused' },
  { id: 'stopped', label: 'Stopped' },
  { id: 'snapshot', label: 'Snapshotting' },
];

export type Column = 'pipeline' | 'state' | 'node' | 'lag' | 'changes' | 'commit';
const COLUMNS: Column[] = ['pipeline', 'state', 'node', 'lag', 'changes', 'commit'];
/** The columns sorted biggest first on their first click. */
const NUMERIC: Column[] = ['lag', 'changes', 'commit'];

/** The list's view, as its address keeps it. */
export interface ListQuery {
  show: Show;
  node: string | null;
  source: SourceAt['type'] | null;
  text: string;
  /** `null`: worst first. */
  sort: Column | null;
  desc: boolean;
}

export function parseQuery(q: URLSearchParams): ListQuery {
  const show = q.get('show');
  const sort = q.get('sort');
  const source = q.get('source');
  return {
    show: SHOWS.some((s) => s.id === show) ? (show as Show) : 'all',
    node: q.get('node') || null,
    source: source === 'postgres' || source === 'mysql' ? source : null,
    text: q.get('q') ?? '',
    sort: COLUMNS.includes(sort as Column) ? (sort as Column) : null,
    desc: q.has('desc'),
  };
}

/** The query of `l`, `''` for the default view. */
export function queryString(l: ListQuery): string {
  const q = new URLSearchParams();
  if (l.show !== 'all') q.set('show', l.show);
  if (l.node !== null) q.set('node', l.node);
  if (l.source !== null) q.set('source', l.source);
  if (l.text !== '') q.set('q', l.text);
  if (l.sort !== null) q.set('sort', l.sort);
  if (l.sort !== null && l.desc) q.set('desc', '');
  const s = q.toString().replace(/desc=(&|$)/, 'desc$1');
  return s === '' ? '' : `?${s}`;
}

/** The view after a click on `column`'s heading: sorted by it, the other way on a second click. */
export function sortBy(l: ListQuery, column: Column): ListQuery {
  if (l.sort === column) return { ...l, desc: !l.desc };
  return { ...l, sort: column, desc: NUMERIC.includes(column) };
}

export interface ListRow extends Row {
  /** Its place worst first. */
  order: number;
  name: string;
  route: string;
  source: SourceAt['type'];
  desired: Desired;
  actual: Actual | null;
  /** Held by a node other than its ring owner. */
  away: boolean;
  /** The changes its tables committed in the last minute; `null` without live figures. */
  changes: number | null;
  tables: number;
  halted: number;
  /** A running job's progress, else its newest ended job. */
  snapshot: string | null;
  /** Its running job's short id. */
  job: string | null;
}

/** The pipeline a "Needs attention" line is about, from where it links. */
export function pipelineOf(href: string): string | null {
  const m = /^\/ui\/(pipelines|tables|snapshots)\/([^/~?]+)/.exec(href);
  return m === null ? null : decodeURIComponent(m[2]);
}

function snapshotOf(jobs: Job[], now: number): { text: string | null; job: string | null } {
  const open = jobs.find((j) => j.status === 'running' || j.status === 'queued' || j.status === 'cancelling');
  if (open !== undefined) {
    if (open.status !== 'running') return { text: open.status, job: shortJob(open.job_id) };
    const pct = open.estimate ? `${Math.min(99, Math.floor((open.rows_done / open.estimate) * 100))}%` : 'running';
    const eta = open.eta_seconds === null ? '' : ` · ETA ${duration(open.eta_seconds)}`;
    return { text: `${pct}${eta}`, job: shortJob(open.job_id) };
  }
  const ended = jobs
    .filter((j) => j.finished_at !== null)
    .sort((a, b) => (at(b.finished_at) ?? 0) - (at(a.finished_at) ?? 0))[0];
  if (ended === undefined) return { text: null, job: null };
  const mark = ended.status === 'completed' ? '✓' : ended.status === 'failed' ? '✕ failed' : ended.status;
  return { text: `${shortJob(ended.job_id)} ${mark} ${ago(at(ended.finished_at)!, now)}`, job: null };
}

/** Every pipeline's row, worst first, its lag line the last 6 hours' (`lines`). */
export function listRows(f: Facts, lines: Record<string, [number, number][]>, now: number): ListRow[] {
  const tables = tablesOf(f);
  return rows(f, now).map((r, order) => {
    const p = f.pipelines.find((x) => x.id === r.id)!;
    const lease = f.cluster?.pipelines.find((l) => l.id === r.id);
    const mine = tables.filter((t) => t.pipeline === r.id);
    const fig = f.figures[r.id];
    const jobs = f.jobs.filter((j) => j.pipeline === r.id);
    const snap = snapshotOf(jobs, now);
    return {
      ...r,
      points: lines[r.id] ?? r.points,
      order,
      name: p.name,
      route: route(p),
      source: p.source.type,
      desired: p.desired_state,
      actual: fig?.actual ?? p.actual_state,
      away: r.node !== null && lease?.ring_owner != null && lease.ring_owner !== r.node,
      changes: fig === undefined ? null : fig.tables.reduce((n, t) => n + t.changes_per_minute, 0),
      tables: mine.length,
      halted: mine.filter((t) => t.halt !== null).length,
      snapshot: snap.text,
      job: snap.job,
    };
  });
}

/** The pipelines "Needs attention" names. */
export function needingAttention(f: Facts, now: number): Set<string> {
  return new Set(
    attention(f, now)
      .map((a) => pipelineOf(a.href))
      .filter((id): id is string => id !== null),
  );
}

function shows(r: ListRow, show: Show, flagged: Set<string>): boolean {
  switch (show) {
    case 'all':
      return true;
    case 'attention':
      return flagged.has(r.id);
    case 'running':
      return r.actual === 'running';
    case 'halted':
      return r.halted > 0;
    case 'paused':
      return r.actual === 'paused';
    case 'stopped':
      return r.actual === 'stopped';
    case 'snapshot':
      return r.job !== null;
  }
}

/** How many rows each filter shows. */
export function counts(all: ListRow[], flagged: Set<string>): Record<Show, number> {
  return Object.fromEntries(SHOWS.map((s) => [s.id, all.filter((r) => shows(r, s.id, flagged)).length])) as Record<
    Show,
    number
  >;
}

const KEY: Record<Column, (r: ListRow) => number | string> = {
  pipeline: (r) => r.id,
  state: (r) => r.order,
  node: (r) => r.node ?? '',
  lag: (r) => r.lag ?? -1,
  changes: (r) => r.changes ?? -1,
  commit: (r) => r.lastCommit ?? 0,
};

/** The rows `l` shows, in its order: worst first unless sorted by a column. */
export function view(all: ListRow[], l: ListQuery, flagged: Set<string>): ListRow[] {
  const text = l.text.trim().toLowerCase();
  const out = all.filter(
    (r) =>
      shows(r, l.show, flagged) &&
      (l.node === null || r.node === l.node) &&
      (l.source === null || r.source === l.source) &&
      (text === '' || r.id.toLowerCase().includes(text) || r.name.toLowerCase().includes(text)),
  );
  if (l.sort === null) return out;
  const key = KEY[l.sort];
  const sign = l.desc ? -1 : 1;
  return out.sort((a, b) => {
    const [x, y] = [key(a), key(b)];
    const c = typeof x === 'number' && typeof y === 'number' ? x - y : String(x).localeCompare(String(y));
    return sign * c || a.order - b.order;
  });
}
