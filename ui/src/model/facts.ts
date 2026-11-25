// What the console knows, from its reads and the stream, and what follows from it for one
// pipeline.

import type {
  ClusterStatus,
  ConfigNodes,
  EventItem,
  Figures,
  Job,
  PipelineItem,
  SinkAt,
  SourceAt,
  TableItem,
} from './types';

export interface Facts {
  pipelines: PipelineItem[];
  cluster: ClusterStatus | null;
  config: ConfigNodes | null;
  jobs: Job[];
  /** Every pipeline's tables, as `GET /v1/tables` read them. */
  tables: TableItem[];
  /** Each pipeline's live figures, from the node that runs it. */
  figures: Record<string, Figures>;
  /** Each pipeline's lag over the last hour: `[unix seconds, seconds]`, oldest first. */
  lag: Record<string, [number, number][]>;
  /** The newest events, newest first. */
  events: EventItem[];
  /** Each pipeline's newest accepted pause, stop or resume. */
  actions: Record<string, EventItem>;
  /** The leases lost or taken over in the last hour, newest first. */
  leaseLosses: EventItem[];
  /** The nodes the stream can't follow, since when (ms). */
  unreachable: Record<string, number>;
  /** The pipelines whose lease no node holds (or renews), since when the page saw it (ms). */
  unheld: Record<string, number>;
}

export function empty(): Facts {
  return {
    pipelines: [],
    cluster: null,
    config: null,
    jobs: [],
    tables: [],
    figures: {},
    lag: {},
    events: [],
    actions: {},
    leaseLosses: [],
    unreachable: {},
    unheld: {},
  };
}

/** Every pipeline's tables: from its live figures where the stream sent them, else as read. */
export function tablesOf(f: Facts): TableItem[] {
  const live = new Set(Object.keys(f.figures));
  return [
    ...f.tables.filter((t) => !live.has(t.pipeline)),
    ...Object.values(f.figures).flatMap((fig) => fig.tables.map((t) => ({ ...t, pipeline: fig.id }))),
  ];
}

/** A pipeline's lag now: its live figures', else the last sample's; `null` when unknown. */
export function lagOf(f: Facts, id: string): number | null {
  const live = f.figures[id]?.worker?.lag_seconds;
  if (live !== undefined && live !== null) return live;
  const points = f.lag[id];
  return points !== undefined && points.length > 0 ? points[points.length - 1][1] : null;
}

/** When an RFC 3339 time is (ms), `null` for none. */
export function at(t: string | null | undefined): number | null {
  if (t === null || t === undefined) return null;
  const ms = Date.parse(t);
  return Number.isNaN(ms) ? null : ms;
}

const SHORT = { postgres: 'PG', mysql: 'MY', delta: 'Δ', iceberg: 'Iceberg' } as const;
const LONG = { postgres: 'PostgreSQL', mysql: 'MySQL', delta: 'Delta', iceberg: 'Iceberg' } as const;

/** Where a pipeline reads and writes: `PG orders@pg-prod → Δ lake/orders`, the engines' names
 * written out with `long`. */
export function route(p: { source: SourceAt; sink: SinkAt }, long = false): string {
  const n = long ? LONG : SHORT;
  const lake = [p.sink.bucket, p.sink.prefix.replace(/^\/+|\/+$/g, '')].filter((s) => s !== '').join('/');
  return `${n[p.source.type]} ${p.source.db}@${p.source.host} → ${n[p.sink.type]} ${lake}`;
}
