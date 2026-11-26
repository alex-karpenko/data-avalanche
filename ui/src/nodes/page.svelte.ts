// A node's page (Admin_Console §4, mockup 04): its detail from the node itself, its charts over 6
// hours from the chart history, and what it recorded. Each part is read on its own and keeps
// what it had when a read fails; the stream adds what the node records as it comes.

import { HttpError, type Client } from '../lib/api';
import type { Live } from '../lib/live.svelte';
import type { PartRead } from '../lib/notices';
import type { EventItem, NodeDetail, Series } from '../model/types';

export type NodePart = 'detail' | 'history' | 'events';
export const NODE_PARTS: NodePart[] = ['detail', 'history', 'events'];
/** Each part as people read it, for the notice that says which couldn't be read. */
export const NODE_PART_NAMES: Record<NodePart, string> = {
  detail: 'its detail',
  history: 'its charts',
  events: 'its activity',
};

/** The charts cover 6 hours, a minute a point. */
export const SPAN_S = 6 * 3600;
export const STEP_S = 60;
/** The node series its charts draw. */
export const NODE_SERIES = ['lease_renew_seconds', 'memory_bytes', 'cpu_cores'] as const;
/** The events read, and kept as they come. */
export const EVENTS_READ = 50;

export class NodePage {
  readonly name: string;
  detail = $state<NodeDetail | null>(null);
  /** Each series' points, `[unix seconds, value]`, oldest first. */
  series = $state<Record<string, [number, number][]>>({});
  /** What the node recorded, newest first. */
  events = $state<EventItem[]>([]);
  read = $state<Record<NodePart, PartRead>>(
    Object.fromEntries(NODE_PARTS.map((p) => [p, { at: null, error: null }])) as Record<NodePart, PartRead>,
  );
  /** No member has this name in the view of the node that answered. */
  missing = $state(false);

  #api: Client;
  #now: () => number;
  #at: string;

  constructor(api: Client, name: string, now: () => number = Date.now) {
    this.#api = api;
    this.name = name;
    this.#now = now;
    this.#at = encodeURIComponent(name);
  }

  /** Reads `parts` (every part by default), each on its own. */
  async load(parts: NodePart[] = NODE_PARTS): Promise<void> {
    await Promise.all(parts.map((p) => this.#loadPart(p)));
  }

  /** Follows `live`; returns the way to stop. */
  follow(live: Live): () => void {
    const stops = [live.onResync(() => void this.load()), live.on('event', (d) => this.event(d as EventItem))];
    return () => {
      for (const stop of stops) stop();
    };
  }

  /** An event the node recorded: added once, at the top. */
  event(e: EventItem): void {
    if (e.node !== this.name || this.events.some((x) => x.key === e.key)) return;
    this.events = [e, ...this.events].slice(0, EVENTS_READ);
  }

  async #loadPart(part: NodePart): Promise<void> {
    try {
      switch (part) {
        case 'detail':
          try {
            this.detail = await this.#api.get<NodeDetail>(`/v1/nodes/${this.#at}`);
            this.missing = false;
          } catch (e) {
            if (!(e instanceof HttpError) || e.status !== 404) throw e;
            this.missing = true;
          }
          break;
        case 'history': {
          const since = new Date(this.#now() - SPAN_S * 1000).toISOString();
          const { series } = await this.#api.get<{ series: Series[] }>(
            `/v1/history?node=${this.#at}&series=${NODE_SERIES.join(',')}&since=${since}&step=${STEP_S}`,
          );
          this.series = Object.fromEntries(series.map((s) => [s.name, s.points]));
          break;
        }
        case 'events':
          this.events = (await this.#api.get<{ events: EventItem[] }>(`/v1/events?node=${this.#at}&limit=${EVENTS_READ}`)).events;
          break;
      }
      this.read[part] = { at: this.#now(), error: null };
    } catch (e) {
      this.read[part] = { at: this.read[part].at, error: e instanceof Error ? e.message : String(e) };
    }
  }
}
