// The Overview's tiles and its pipelines' rows (mockup 01), from what it knows.

import { ago, bytes, duration } from '../lib/format';
import { FAILED_WITHIN_S, LAG_TARGET_S, walPast } from './attention';
import { at, lagOf, tablesOf, type Facts } from '../model/facts';

export type Tone = 'normal' | 'green' | 'orange' | 'red' | 'grey';

export interface Tile {
  key: string;
  value: string;
  detail: string;
  tone: Tone;
}

const ACTUALS = ['running', 'paused', 'stopped', 'retrying', 'starting', 'dumped', 'error'] as const;

/** The five headline tiles: pipelines, worst lag, nodes, snapshots, WAL held by slots. */
export function tiles(f: Facts, now: number): Tile[] {
  const halted = tablesOf(f).filter((t) => t.halt !== null).length;
  const actual = (id: string) =>
    f.figures[id]?.actual ?? f.pipelines.find((p) => p.id === id)?.actual_state;
  const counts = ACTUALS.map((s) => [s, f.pipelines.filter((p) => actual(p.id) === s).length] as const)
    .filter(([, n]) => n > 0)
    .map(([s, n]) => `${n} ${s}`);
  if (halted > 0) counts.push(`${halted} halted ${halted === 1 ? 'table' : 'tables'}`);
  const pipelines: Tile = {
    key: 'Pipelines',
    value: String(f.pipelines.length),
    detail: counts.join(' · '),
    tone: halted > 0 || f.pipelines.some((p) => actual(p.id) === 'error') ? 'red' : 'normal',
  };

  let worst: { id: string; lag: number } | null = null;
  for (const p of f.pipelines) {
    const lag = lagOf(f, p.id);
    if (lag !== null && (worst === null || lag > worst.lag)) worst = { id: p.id, lag };
  }
  const lag: Tile = {
    key: 'Worst lag',
    value: worst === null ? '—' : duration(worst.lag),
    detail: `${worst === null ? 'no pipeline reports one' : worst.id} · target ${LAG_TARGET_S} s`,
    tone: worst !== null && worst.lag > LAG_TARGET_S ? 'orange' : 'normal',
  };

  const members = f.cluster?.members ?? [];
  const older = (f.config?.nodes ?? []).filter(
    (n) => n.generation !== null && f.config !== null && n.generation < f.config.generation,
  ).length;
  const nodes: Tile = {
    key: 'Nodes',
    value: `${members.filter((m) => !m.suspect).length} / ${members.length}`,
    detail:
      older > 0
        ? `${older} on an older config generation`
        : f.config === null
          ? ''
          : `all on config generation ${f.config.generation}`,
    tone: members.some((m) => m.suspect) ? 'red' : 'normal',
  };

  const running = f.jobs.filter((j) => ['queued', 'running', 'cancelling'].includes(j.status)).length;
  const failed = f.jobs.filter((j) => {
    const ended = at(j.finished_at);
    return j.status === 'failed' && ended !== null && now - ended <= FAILED_WITHIN_S * 1000;
  }).length;
  const snapshots: Tile = {
    key: 'Snapshots',
    value: String(running),
    detail: failed > 0 ? `running · ${failed} failed today` : 'running',
    tone: failed > 0 ? 'orange' : 'normal',
  };

  const slots = Object.values(f.figures)
    .map((fig) => fig.source?.detail)
    .filter((d) => d?.slot?.wal_held_bytes !== null && d?.slot?.wal_held_bytes !== undefined)
    .map((d) => ({ name: d!.slot!.name, held: d!.slot!.wal_held_bytes!, limit: d!.max_slot_wal_keep_size }));
  const largest = slots.reduce<(typeof slots)[number] | null>(
    (a, s) => (a === null || s.held > a.held ? s : a),
    null,
  );
  const wal: Tile = {
    key: 'WAL held by slots',
    value: slots.length === 0 ? '—' : bytes(slots.reduce((n, s) => n + s.held, 0)),
    detail: largest === null ? 'no PostgreSQL slot reported' : `largest: ${largest.name} ${bytes(largest.held)}`,
    tone: slots.some((s) => walPast(s.held, s.limit)) ? 'orange' : 'normal',
  };

  return [pipelines, lag, nodes, snapshots, wal];
}

export interface Row {
  id: string;
  /** Source → sink and the tables: `PostgreSQL → Delta · 14 tables`. */
  what: string;
  state: { label: string; tone: Tone; note: string | null };
  node: string | null;
  lag: number | null;
  points: [number, number][];
  lastCommit: number | null;
}

const ENGINES = { postgres: 'PostgreSQL', mysql: 'MySQL' } as const;

/** Each pipeline's row, the worst first: halted or in error, lagging, unknown, the rest. */
export function rows(f: Facts, now: number): Row[] {
  const tables = tablesOf(f);
  const out = f.pipelines.map((p): Row & { rank: number } => {
    const fig = f.figures[p.id];
    const mine = tables.filter((t) => t.pipeline === p.id);
    const halted = mine.filter((t) => t.halt !== null).length;
    const actual = fig?.actual ?? p.actual_state;
    const node = fig?.node ?? p.holder;
    const lag = lagOf(f, p.id);
    const job = f.jobs.find((j) => j.pipeline === p.id && j.status === 'running');
    const detail = fig?.source?.detail;
    const engine = detail ? ENGINES[detail.engine] : null;
    const down = node !== null ? f.unreachable[node] : undefined;
    let state: Row['state'];
    let rank = 3;
    if (down !== undefined) {
      state = {
        label: 'unknown',
        tone: 'grey',
        note: `${node} unreachable ${duration((now - down) / 1000)} · last known: ${actual ?? 'unknown'}`,
      };
      rank = 2;
    } else if (halted > 0) {
      state = { label: `${halted} halted`, tone: 'red', note: null };
      rank = 0;
    } else if (actual === 'error') {
      state = { label: 'error', tone: 'red', note: fig?.error ?? null };
      rank = 0;
    } else if (lag !== null && lag > LAG_TARGET_S) {
      state = { label: 'lagging', tone: 'orange', note: null };
      rank = 1;
    } else if (job !== undefined) {
      const pct = job.estimate ? Math.min(99, Math.floor((job.rows_done / job.estimate) * 100)) : null;
      state = { label: pct === null ? 'snapshot' : `snapshot ${pct}%`, tone: 'green', note: null };
    } else if (actual === 'paused' || actual === 'stopped') {
      const a = f.actions[p.id];
      const when = at(a?.at);
      state = {
        label: actual,
        tone: 'grey',
        note: a?.actor && when !== null ? `by ${a.actor.name}, ${ago(when, now)}` : null,
      };
    } else {
      state = { label: actual ?? 'unknown', tone: actual === 'running' ? 'green' : 'grey', note: null };
    }
    const commits = mine.map((t) => at(t.last_commit)).filter((t): t is number => t !== null);
    return {
      id: p.id,
      what: `${engine ? `${engine} → ` : ''}Delta · ${mine.length} ${mine.length === 1 ? 'table' : 'tables'}`,
      state,
      node,
      lag,
      points: f.lag[p.id] ?? [],
      lastCommit: commits.length > 0 ? Math.max(...commits) : null,
      rank,
    };
  });
  out.sort((a, b) => a.rank - b.rank || (b.lag ?? -1) - (a.lag ?? -1) || a.id.localeCompare(b.id));
  return out.map(({ rank: _rank, ...row }) => row);
}
