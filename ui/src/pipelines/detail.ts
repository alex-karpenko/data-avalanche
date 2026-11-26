// A pipeline's page (mockup 06), worked out from what the page read and the stream sent: the
// header, the tables, the source, the snapshot, the chart's events and the activity.

import { ago, bytes, clock, duration } from '../lib/format';
import type { PartRead } from '../lib/notices';
import { compact, formatValue, type Mark } from '../lib/series';
import { at, route } from '../model/facts';
import type { ClusterStatus, EventItem, Figures, JobDetail, PipelineDetail, PipelineItem } from '../model/types';
import { shortJob, walPast } from '../overview/attention';
import type { Tone } from '../overview/derive';

/** The events read for the page, and the newest shown in its activity. */
export const ACTIVITY_READ = 50;
export const ACTIVITY_SHOWN = 10;
/** A table's schema change shows for this long. */
export const SCHEMA_SHOWN_S = 86_400;

/** A position as people read it: `3/A41C2B10`, its dialect's tag dropped. */
export function position(tagged: string | null | undefined): string | null {
  if (tagged === null || tagged === undefined) return null;
  const i = tagged.indexOf(':');
  return i === -1 ? tagged : tagged.slice(i + 1);
}

const plural = (n: number, one: string) => `${n} ${one}${n === 1 ? '' : 's'}`;

export interface Header {
  state: { label: string; tone: Tone };
  route: string;
  /** Where it runs and its lease: `on n2 · away from owner n1 · lease epoch 7, expires in 8 s`. */
  where: string;
}

/**
 * `clusterRead`: the cluster's last read; once it fails, the lease's end is only what the last
 * good read saw, so nothing is counted down.
 */
export function header(
  d: PipelineDetail | null,
  item: PipelineItem | undefined,
  fig: Figures | null,
  lease: ClusterStatus['pipelines'][number] | undefined,
  now: number,
  clusterRead: PartRead = { at: null, error: null },
): Header | null {
  const p = d ?? item;
  if (p === undefined) return null;
  const halted = fig !== null ? fig.tables.filter((t) => t.halt !== null).length : (d?.halted_tables.length ?? 0);
  const actual = fig?.actual ?? p.actual_state;
  const state: Header['state'] =
    halted > 0
      ? { label: `${plural(halted, 'table')} halted`, tone: 'red' }
      : actual === 'error'
        ? { label: 'error', tone: 'red' }
        : { label: actual ?? 'unknown', tone: actual === 'running' ? 'green' : 'grey' };
  // A released lease has no holder to count down for (the cluster's status leaves it out).
  if (fig === null && d?.holder?.released === true) return { state, route: route(p, true), where: 'lease released' };
  const node = fig?.node ?? d?.holder?.node ?? item?.holder ?? null;
  const parts = [node === null ? 'no node holds its lease' : `on ${node}`];
  if (node !== null && lease?.ring_owner != null && lease.ring_owner !== node) {
    parts.push(`away from owner ${lease.ring_owner}`);
  }
  const epoch = fig?.owner_epoch ?? d?.holder?.owner_epoch ?? lease?.owner_epoch;
  if (node !== null && epoch !== undefined && epoch !== null) {
    const ends = clusterRead.error === null ? at(lease?.expires_at) : null;
    const left = ends === null ? '' : ends > now ? `, expires in ${duration((ends - now) / 1000)}` : `, expired ${duration((now - ends) / 1000)} ago`;
    parts.push(`lease epoch ${epoch}${left}`);
  }
  return { state, route: route(p, true), where: parts.join(' · ') };
}

export interface TableLine {
  table: string;
  href: string;
  state: { label: string; tone: Tone };
  changes: number | null;
  lastCommit: number | null;
  version: number | null;
  /** The last MERGE's files rewritten, of the table's. */
  files: string | null;
  schema: string;
}

const STATES: Record<string, { label: string; tone: Tone }> = {
  streaming: { label: 'running', tone: 'green' },
  halted: { label: 'halted', tone: 'red' },
  snapshot: { label: 'snapshot', tone: 'green' },
  copied: { label: 'copied', tone: 'grey' },
};

/**
 * A table as the worker's figures name it. MySQL's are bare (`invoices`), while the halts storage
 * keeps and a schema event's object carry `db.table`; PostgreSQL's are `schema.table` everywhere.
 * The server takes a MySQL table in a snapshot request only without its database.
 */
function named(mysql: boolean, table: string): string {
  const dot = table.indexOf('.');
  return mysql && dot !== -1 ? table.slice(dot + 1) : table;
}

/** Each table, halted first: from the worker's figures, else the halts storage keeps. */
export function tableLines(
  id: string,
  fig: Figures | null,
  d: PipelineDetail | null,
  events: EventItem[],
  now: number,
): TableLine[] {
  const mysql = d?.source.type === 'mysql' || fig?.source?.detail?.engine === 'mysql';
  const tables =
    fig?.tables ??
    (d?.halted_tables ?? []).map((h) => ({
      table: named(mysql, h.table),
      state: 'halted',
      halt: { code: h.code, detail: h.detail, since: h.since },
      changes_per_minute: 0,
      last_commit: null,
      version: null,
      files_rewritten: null,
      files: null,
      soft_dropped: 0,
      stale: false,
    }));
  return tables
    .map((t) => {
      const change = events.find(
        (e) =>
          e.kind.startsWith('schema.') && e.object !== undefined && named(mysql, e.object) === t.table && now - (at(e.at) ?? 0) < SCHEMA_SHOWN_S * 1000,
      );
      const schema =
        t.halt !== null
          ? t.halt.detail
          : change !== undefined
            ? change.text
            : t.soft_dropped > 0
              ? `${plural(t.soft_dropped, 'soft-dropped column')}`
              : 'unchanged';
      const state = t.stale ? { label: 'stale', tone: 'orange' as Tone } : (STATES[t.state] ?? { label: t.state, tone: 'grey' as Tone });
      return {
        table: t.table,
        href: `/ui/tables/${encodeURIComponent(id)}/${encodeURIComponent(t.table)}`,
        state,
        changes: fig === null ? null : t.changes_per_minute,
        lastCommit: at(t.last_commit),
        version: t.version,
        files: t.files_rewritten === null ? null : `${t.files_rewritten}${t.files === null ? '' : ` of ${t.files}`}`,
        schema,
      };
    })
    .sort((a, b) => Number(b.state.label === 'halted') - Number(a.state.label === 'halted') || a.table.localeCompare(b.table));
}

export interface SourceLine {
  label: string;
  value: string;
  tone: Tone;
}

/** What the source keeps for the pipeline: its slot and publication, or its binlog position. */
export function sourceLines(fig: Figures | null): SourceLine[] {
  const s = fig?.source;
  const d = s?.detail;
  if (s === null || s === undefined || d === null || d === undefined) return [];
  const line = (label: string, value: string, tone: Tone = 'normal') => ({ label, value, tone });
  if (d.engine === 'postgres') {
    const slot = d.slot ?? null;
    const held = slot?.wal_held_bytes ?? null;
    const max = d.max_slot_wal_keep_size ?? null;
    return [
      slot === null
        ? line('Slot', 'none by its name', 'red')
        : line('Slot', `${slot.name} ${slot.active ? 'active' : 'inactive'}`, slot.active ? 'normal' : 'orange'),
      line(
        'WAL held',
        held === null ? '—' : max === null ? `${bytes(held)} · no max_slot_wal_keep_size` : `${bytes(held)} of max_slot_wal_keep_size ${bytes(max)}`,
        held !== null && walPast(held, max) ? 'orange' : 'normal',
      ),
      line('Confirmed LSN', position(slot?.confirmed_flush) ?? '—'),
      d.publication == null
        ? line('Publication', 'none by its name', 'red')
        : line('Publication', `${d.publication.name} · ${plural(d.publication.tables, 'table')}`),
    ];
  }
  return [
    line('Position', position(d.head) ?? '—'),
    line('Binlogs kept', d.binlog_expire_seconds == null ? 'never expire' : duration(d.binlog_expire_seconds)),
    line('Behind', s.behind_seconds == null ? '—' : formatValue('seconds', s.behind_seconds)),
  ];
}

export interface Snapshot {
  current: { job: string; href: string; what: string; note: string | null; progress: string } | null;
  earlier: { job: string; href: string; text: string }[];
}

const jobHref = (id: string) => `/ui/snapshots/${encodeURIComponent(id)}`;

/** The running (or queued) job with its progress, and the three ended last. */
export function snapshotPanel(jobs: JobDetail[], now: number): Snapshot {
  const newest = [...jobs].sort((a, b) => (at(b.requested_at) ?? 0) - (at(a.requested_at) ?? 0));
  const open = newest.find((j) => j.finished_at === null);
  let current: Snapshot['current'] = null;
  if (open !== undefined) {
    const names = open.tables.map((t) => t.table);
    const tables = names.length <= 2 ? names.join(', ') : `${names.length} tables`;
    const progress =
      open.status === 'running'
        ? [
            open.estimate ? `${Math.min(99, Math.floor((open.rows_done / open.estimate) * 100))}%` : null,
            `${compact(open.rows_done)}${open.estimate ? ` of ~${compact(open.estimate)}` : ''} rows`,
            open.eta_seconds === null ? null : `ETA ${duration(open.eta_seconds)}`,
            open.rate === null ? null : `${compact(open.rate)} rows/s`,
          ]
            .filter((p) => p !== null)
            .join(' · ')
        : open.status === 'queued'
          ? 'queued: the pipeline restarts to take it'
          : open.status;
    current = {
      job: shortJob(open.job_id),
      href: jobHref(open.job_id),
      what: `${tables}${open.truncate ? ' · truncate' : ''} · started by ${open.requested_by} ${clock(at(open.requested_at) ?? now)}`,
      note: open.note,
      progress,
    };
  }
  const earlier = newest
    .filter((j) => j.finished_at !== null)
    .slice(0, 3)
    .map((j) => {
      const when = ago(at(j.finished_at)!, now);
      const how =
        j.status === 'completed'
          ? `✓ ${when}`
          : j.status === 'failed'
            ? `✕ failed ${when}`
            : `cancelled${j.cancel === null ? '' : ` by ${j.cancel.by}`} ${when}`;
      return { job: shortJob(j.job_id), href: jobHref(j.job_id), text: how };
    });
  return { current, earlier };
}

const ACTED: Record<string, string> = {
  'action.pipeline.restart': 'restarted',
  'action.pipeline.pause': 'paused',
  'action.pipeline.resume': 'resumed',
  'action.pipeline.stop': 'stopped',
  'action.pipeline.flush': 'flushed',
};

/** An event's label on the chart, `null` for one it doesn't draw. */
export function markOf(e: EventItem): string | null {
  if (e.kind in ACTED) return e.outcome === 'accepted' ? ACTED[e.kind] : null;
  if (e.kind === 'action.snapshot.start') return e.outcome === 'accepted' ? 'snapshot' : null;
  if (e.kind === 'job.completed') return 'snapshot done';
  if (e.kind === 'job.failed') return 'snapshot failed';
  if (e.kind === 'table.halted') return `${e.object ?? 'a table'} halted`;
  if (e.kind === 'lease.takeover') return 'failover';
  if (e.kind === 'lease.lost') return 'lease lost';
  if (e.kind === 'pipeline.error') return 'error';
  return null;
}

/** The events the chart draws between `since` and now, unix seconds. */
export function marks(events: EventItem[], since: number): Mark[] {
  return events.flatMap((e) => {
    const label = markOf(e);
    const t = at(e.at);
    return label === null || t === null || t / 1000 < since ? [] : [{ key: e.key, at: t / 1000, label }];
  });
}

export interface ActivityLine {
  key: string;
  when: string;
  text: string;
  /** `Oleksii (operator)`, for a person's or a token's action. */
  who: string | null;
  note: string | null;
  severity: EventItem['severity'];
}

/** The newest events, a line each: today's by their time, older ones by their date too. */
export function activity(events: EventItem[], now: number): ActivityLine[] {
  const today = new Date(now).toDateString();
  return events.slice(0, ACTIVITY_SHOWN).map((e) => {
    const t = at(e.at) ?? now;
    const d = new Date(t);
    const date = `${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
    return {
      key: e.key,
      when: d.toDateString() === today ? clock(t) : `${date} ${clock(t)}`,
      text: e.text,
      who: e.actor === undefined ? null : `${e.actor.name} (${e.actor.role})`,
      note: e.note ?? null,
      severity: e.severity,
    };
  });
}
