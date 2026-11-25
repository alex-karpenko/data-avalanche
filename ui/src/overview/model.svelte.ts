// The Overview's data (Admin_Console §8): read at once, each part within its own time limit, then
// kept current by the live stream. A part that can't be read keeps what it last had and says why.

import type { Client } from '../lib/api';
import type { Live } from '../lib/live.svelte';
import { empty, type Facts } from './facts';
import type {
  Actual,
  ClusterStatus,
  ConfigNodes,
  EventItem,
  Figures,
  Job,
  PipelineItem,
  Series,
  Tables,
} from './types';

/** The events the Overview lists. */
export const EVENTS_SHOWN = 8;
/** The events read for the rules (lease losses of the last hour). */
export const EVENTS_READ = 100;
/** The span of the pipelines' lag lines. */
export const LAG_SPAN_S = 3600;
/** A burst of events reads their parts again once, after this. */
export const REREAD_AFTER_MS = 1_000;

export type Part = 'pipelines' | 'cluster' | 'config' | 'jobs' | 'tables' | 'events' | 'actions' | 'lag';
export const PARTS: Part[] = ['pipelines', 'cluster', 'config', 'jobs', 'tables', 'events', 'actions', 'lag'];

/** When a part was last read (ms), and why its last read failed. */
export interface Read {
  at: number | null;
  error: string | null;
}

const ACTIONS = 'action.pipeline.pause,action.pipeline.stop,action.pipeline.resume';

/** The parts an event's kind changes. */
function partsOf(kind: string): Part[] {
  if (kind.startsWith('job.')) return ['jobs'];
  if (kind.startsWith('config.')) return ['config'];
  if (kind.startsWith('member.') || kind.startsWith('lease.')) return ['cluster', 'pipelines'];
  if (kind.startsWith('table.') || kind.startsWith('schema.')) return ['tables'];
  if (kind.startsWith('pipeline.')) return ['pipelines'];
  if (kind.startsWith('action.snapshot.')) return ['jobs'];
  return [];
}

export class Overview {
  facts = $state<Facts>(empty());
  read = $state<Record<Part, Read>>(
    Object.fromEntries(PARTS.map((p) => [p, { at: null, error: null }])) as Record<Part, Read>,
  );

  #api: Client;
  #pending = new Set<Part>();
  #timer: ReturnType<typeof setTimeout> | undefined;

  constructor(api: Client) {
    this.#api = api;
  }

  /** Reads `parts` (every part by default), each on its own. */
  async load(parts: Part[] = PARTS): Promise<void> {
    await Promise.all(parts.map((p) => this.#loadPart(p)));
  }

  /** Follows `live`; returns the way to stop. */
  follow(live: Live): () => void {
    const stops = [
      live.onResync(() => void this.load()),
      live.on('pipeline', (d) => this.pipeline(d as PipelineMsg)),
      live.on('figures', (d) => this.figures(d as Figures)),
      live.on('samples', (d) => this.samples(d as SamplesMsg)),
      live.on('event', (d) => this.event(d as EventItem)),
      live.on('members', () => this.#reread(['cluster'])),
      live.on('peer', (d) => this.peer(d as PeerMsg)),
    ];
    return () => {
      for (const stop of stops) stop();
      clearTimeout(this.#timer);
    };
  }

  /** A pipeline's state changed on the node that runs it; `standby` when it stopped running it. */
  pipeline(m: PipelineMsg): void {
    const p = this.facts.pipelines.find((x) => x.id === m.pipeline);
    if (p === undefined || m.actual === 'standby') return;
    const current = this.facts.figures[m.pipeline];
    if (current !== undefined && current.owner_epoch > m.owner_epoch) return;
    p.actual_state = m.actual;
    p.holder = m.node;
    if (current !== undefined) {
      current.actual = m.actual;
      current.error = m.error;
    }
  }

  /** A pipeline's figures: the newest owner's win (a node killed sends no `standby`). */
  figures(m: Figures): void {
    const current = this.facts.figures[m.id];
    if (current !== undefined && current.owner_epoch > m.owner_epoch) return;
    this.facts.figures[m.id] = m;
  }

  /** A pipeline's chart sample: its lag joins the line, which keeps the last hour. */
  samples(m: SamplesMsg): void {
    const lag = m.series?.lag_seconds;
    if (m.scope !== 'pipeline' || m.pipeline === undefined || lag === undefined) return;
    const points = [...(this.facts.lag[m.pipeline] ?? []), [m.at, lag] as [number, number]];
    this.facts.lag[m.pipeline] = points.filter(([t]) => t > m.at - LAG_SPAN_S);
  }

  /** An event, once written: the newest first, and the parts it changes read again. */
  event(e: EventItem): void {
    if (this.facts.events.some((x) => x.key === e.key)) return;
    this.facts.events = [e, ...this.facts.events].slice(0, EVENTS_READ);
    if (ACTIONS.split(',').includes(e.kind) && e.pipeline !== undefined) {
      this.facts.actions[e.pipeline] = e;
    }
    this.#reread(partsOf(e.kind));
  }

  /** Another node followed again, or not reachable. */
  peer(m: PeerMsg): void {
    if (m.state === 'unreachable') {
      this.facts.unreachable[m.peer] ??= Date.now();
    } else {
      delete this.facts.unreachable[m.peer];
    }
  }

  #reread(parts: Part[]): void {
    for (const p of parts) this.#pending.add(p);
    if (this.#timer !== undefined || this.#pending.size === 0) return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      const parts = [...this.#pending];
      this.#pending.clear();
      void this.load(parts);
    }, REREAD_AFTER_MS);
  }

  async #loadPart(part: Part): Promise<void> {
    try {
      const f = this.facts;
      switch (part) {
        case 'pipelines':
          f.pipelines = (await this.#api.get<{ pipelines: PipelineItem[] }>('/v1/pipelines')).pipelines;
          break;
        case 'cluster': {
          f.cluster = await this.#api.get<ClusterStatus>('/v1/cluster/status');
          const unheld: Record<string, number> = {};
          for (const l of f.cluster.pipelines) {
            if (l.holder === null || l.holder_stale) unheld[l.id] = f.unheld[l.id] ?? Date.now();
          }
          f.unheld = unheld;
          break;
        }
        case 'config':
          f.config = await this.#api.get<ConfigNodes>('/v1/config/nodes');
          break;
        case 'jobs':
          f.jobs = (await this.#api.get<{ jobs: Job[] }>('/v1/snapshots')).jobs;
          break;
        case 'tables':
          f.tables = (await this.#api.get<Tables>('/v1/tables')).tables;
          break;
        case 'events':
          f.events = (
            await this.#api.get<{ events: EventItem[] }>(`/v1/events?limit=${EVENTS_READ}`)
          ).events;
          break;
        case 'actions': {
          const { events } = await this.#api.get<{ events: EventItem[] }>(
            `/v1/events?kind=${ACTIONS}&limit=1000`,
          );
          const newest: Record<string, EventItem> = {};
          for (const e of events) if (e.pipeline !== undefined) newest[e.pipeline] ??= e;
          f.actions = newest;
          break;
        }
        case 'lag': {
          const { series } = await this.#api.get<{ series: Series[] }>('/v1/history?series=lag_seconds');
          f.lag = Object.fromEntries(
            series.filter((s) => s.pipeline !== undefined).map((s) => [s.pipeline!, s.points]),
          );
          break;
        }
      }
      this.read[part] = { at: Date.now(), error: null };
    } catch (e) {
      this.read[part] = { at: this.read[part].at, error: e instanceof Error ? e.message : String(e) };
    }
  }
}

/** The stream's `pipeline` message. */
export interface PipelineMsg {
  pipeline: string;
  actual: Actual | 'standby';
  error: string | null;
  node: string;
  owner_epoch: number;
}

/** The stream's `samples` message. */
export interface SamplesMsg {
  scope: 'pipeline' | 'node';
  pipeline?: string;
  at: number;
  series?: Record<string, number>;
}

/** The stream's `peer` message. */
export interface PeerMsg {
  peer: string;
  state: 'following' | 'unreachable';
  error?: string;
}
