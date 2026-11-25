// Needs attention (Admin_Console §4.1): what is wrong now, red → orange → yellow, each linking
// to where it's fixed. The thresholds are config-free defaults, stated on the page (Oleksii,
// 2026-11-25: a later release makes them configurable).

import { bytes, duration } from '../lib/format';
import { at, lagOf, tablesOf, type Facts } from './facts';

/** The lag a pipeline is expected to stay under. */
export const LAG_TARGET_S = 60;
/** "Rising": higher now than this long ago. */
export const RISING_OVER_S = 300;
/** A slot holding more than this share of `max_slot_wal_keep_size`… */
export const WAL_SHARE = 0.5;
/** …or more than this, when the server sets no limit. */
export const WAL_UNLIMITED_BYTES = 16 * 1024 ** 3;
/** A pipeline paused or stopped by a person for longer than this. */
export const PAUSED_FOR_S = 3600;
/** A snapshot job that failed within this. */
export const FAILED_WITHIN_S = 86_400;
/** A lease lost or taken over within this. */
export const LEASE_LOSS_WITHIN_S = 3600;
/** A lease with no holder for longer than this: a rebalance hands leases over within it. */
export const UNHELD_FOR_S = 30;

export type Severity = 'red' | 'orange' | 'yellow';

export interface Attention {
  severity: Severity;
  /** What, in a few words (shown bold), then why. */
  what: string;
  why: string;
  /** Since when (ms), when known. */
  since: number | null;
  href: string;
  link: string;
}

const RANK: Record<Severity, number> = { red: 0, orange: 1, yellow: 2 };

const pipelineHref = (id: string) => `/ui/pipelines/${encodeURIComponent(id)}`;

/** Whether a slot's WAL held is past its threshold. */
export function walPast(held: number, limit: number | null | undefined): boolean {
  return limit === null || limit === undefined ? held > WAL_UNLIMITED_BYTES : held > WAL_SHARE * limit;
}

/** Everything that needs attention at `now` (ms), the worst first, then the newest. */
export function attention(f: Facts, now: number): Attention[] {
  const out: Attention[] = [];
  const add = (a: Attention) => out.push(a);

  // Red: a table halted; a pipeline stopped by an error; a lease with no holder; a node down.
  for (const t of tablesOf(f)) {
    if (t.halt === null) continue;
    add({
      severity: 'red',
      what: `${t.pipeline} › ${t.table} halted`,
      why: t.halt.detail,
      since: at(t.halt.since),
      href: `/ui/tables/${encodeURIComponent(t.pipeline)}/${encodeURIComponent(t.table)}`,
      link: 'Open table',
    });
  }
  for (const p of f.pipelines) {
    const live = f.figures[p.id];
    if ((live?.actual ?? p.actual_state) === 'error') {
      add({
        severity: 'red',
        what: `${p.id} stopped by an error`,
        why: live?.error ?? 'it needs a stop, then a resume',
        since: null,
        href: pipelineHref(p.id),
        link: 'Open',
      });
    }
  }
  for (const l of f.cluster?.pipelines ?? []) {
    const since = f.unheld[l.id];
    if ((l.holder === null || l.holder_stale) && since !== undefined && now - since >= UNHELD_FOR_S * 1000) {
      add({
        severity: 'red',
        what: `${l.id} has no node running it`,
        why: l.holder === null ? 'no node holds its lease' : `${l.holder} stopped renewing its lease`,
        since,
        href: pipelineHref(l.id),
        link: 'Open',
      });
    }
  }
  for (const m of f.cluster?.members ?? []) {
    if (m.suspect) {
      add({
        severity: 'red',
        what: `${m.node} is down`,
        why: "the other nodes' probes get no answer",
        since: null,
        href: `/ui/nodes/${encodeURIComponent(m.node)}`,
        link: 'Node',
      });
    }
  }

  // Orange: lag over target and rising; a slot's WAL past its share; lease losses; a failed
  // snapshot job; sink commits failing; a node the console can't reach.
  for (const p of f.pipelines) {
    const lag = lagOf(f, p.id);
    const points = f.lag[p.id] ?? [];
    if (lag === null || lag <= LAG_TARGET_S || points.length === 0) continue;
    const before = points.find(([t]) => t >= now / 1000 - RISING_OVER_S);
    if (before === undefined || lag <= before[1]) continue;
    let since = points[points.length - 1][0];
    for (let i = points.length - 1; i >= 0 && points[i][1] > LAG_TARGET_S; i--) since = points[i][0];
    add({
      severity: 'orange',
      what: `${p.id} lag ${duration(lag)}`,
      why: `and rising (target ${LAG_TARGET_S} s)`,
      since: since * 1000,
      href: pipelineHref(p.id),
      link: 'Open',
    });
  }
  for (const fig of Object.values(f.figures)) {
    const d = fig.source?.detail;
    const held = d?.slot?.wal_held_bytes;
    if (d?.slot && held !== null && held !== undefined && walPast(held, d.max_slot_wal_keep_size)) {
      const limit = d.max_slot_wal_keep_size;
      add({
        severity: 'orange',
        what: `Slot ${d.slot.name} holds ${bytes(held)} of WAL`,
        why:
          limit === null || limit === undefined
            ? `with no max_slot_wal_keep_size · pipeline ${fig.id}`
            : `${Math.round((held / limit) * 100)}% of max_slot_wal_keep_size · pipeline ${fig.id}`,
        since: null,
        href: pipelineHref(fig.id),
        link: 'Open',
      });
    }
    if (fig.worker?.degraded) {
      add({
        severity: 'orange',
        what: `${fig.id} can't commit`,
        why: fig.worker.degraded,
        since: null,
        href: pipelineHref(fig.id),
        link: 'Open',
      });
    }
  }
  for (const e of f.events) {
    const when = at(e.at);
    if (
      (e.kind === 'lease.lost' || e.kind === 'lease.takeover') &&
      when !== null &&
      now - when <= LEASE_LOSS_WITHIN_S * 1000
    ) {
      add({
        severity: 'orange',
        what: e.kind === 'lease.lost' ? `${e.pipeline} lost its lease` : `${e.pipeline} failed over`,
        why: e.text,
        since: when,
        href: pipelineHref(e.pipeline ?? ''),
        link: 'Open',
      });
    }
  }
  for (const j of f.jobs) {
    const ended = at(j.finished_at);
    if (j.status === 'failed' && ended !== null && now - ended <= FAILED_WITHIN_S * 1000) {
      add({
        severity: 'orange',
        what: `Snapshot ${j.job_id.slice(0, 8)} of ${j.pipeline} failed`,
        why: j.error ?? '',
        since: ended,
        href: `/ui/snapshots/${encodeURIComponent(j.job_id)}`,
        link: 'Open',
      });
    }
  }
  for (const [node, since] of Object.entries(f.unreachable)) {
    add({
      severity: 'orange',
      what: `${node} doesn't answer the console`,
      why: 'its pipelines show as last known',
      since,
      href: `/ui/nodes/${encodeURIComponent(node)}`,
      link: 'Node',
    });
  }

  // Yellow: nodes on different config generations; restart-only changes waiting; a pipeline
  // paused or stopped by a person for a while.
  for (const n of f.config?.nodes ?? []) {
    if (f.config !== null && n.generation !== null && n.generation !== f.config.generation) {
      add({
        severity: 'yellow',
        what: `${n.node} runs config generation ${n.generation}`,
        why: `the cluster is on ${f.config.generation}`,
        since: null,
        href: `/ui/nodes/${encodeURIComponent(n.node)}`,
        link: 'Node',
      });
    }
    if (n.restart_pending.length > 0) {
      add({
        severity: 'yellow',
        what: `Restart-only change waiting on ${n.node}`,
        why: n.restart_pending.join(', '),
        since: null,
        href: '/ui/config',
        link: 'Review',
      });
    }
  }
  for (const p of f.pipelines) {
    const a = f.actions[p.id];
    const when = at(a?.at);
    if (
      (p.desired_state === 'paused' || p.desired_state === 'stopped') &&
      a !== undefined &&
      a.actor !== undefined &&
      when !== null &&
      now - when > PAUSED_FOR_S * 1000
    ) {
      add({
        severity: 'yellow',
        what: `${p.id} ${p.desired_state}`,
        why: `by ${a.actor.name}`,
        since: when,
        href: pipelineHref(p.id),
        link: 'Open',
      });
    }
  }

  return out.sort(
    (a, b) => RANK[a.severity] - RANK[b.severity] || (b.since ?? -Infinity) - (a.since ?? -Infinity),
  );
}
