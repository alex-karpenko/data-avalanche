// A pipeline's page (Admin_Console §4, §8; mockup 06): what the console knows already is shown at
// once (its row, the figures the stream sent), then each part is read on its own and followed
// through the stream. A part that can't be read keeps what it had and says why.

import { HttpError, type Client } from '../lib/api';
import type { Live } from '../lib/live.svelte';
import { REREAD_AFTER_MS, type PipelineMsg, type Read } from '../model/model.svelte';
import type { EventItem, Figures, JobDetail, PipelineDetail } from '../model/types';
import { ACTIVITY_READ } from './detail';
import { LagHistory } from './lag.svelte';

export type PagePart = 'detail' | 'live' | 'jobs' | 'events';
export const PAGE_PARTS: PagePart[] = ['detail', 'live', 'jobs', 'events'];
/** What people call each part, in the notice that says one couldn't be read. */
export const PART_NAMES: Record<PagePart, string> = {
  detail: 'the pipeline',
  live: 'its figures',
  jobs: 'its snapshot jobs',
  events: 'its activity',
};

/** The parts an event about the pipeline changes. */
function partsOf(kind: string): PagePart[] {
  if (kind.startsWith('job.') || kind.startsWith('action.snapshot.')) return ['jobs'];
  if (kind.startsWith('table.') || kind.startsWith('schema.')) return ['detail', 'live'];
  if (kind.startsWith('pipeline.') || kind.startsWith('lease.') || kind.startsWith('action.pipeline.')) {
    return ['detail'];
  }
  return [];
}

export class PipelinePage {
  readonly id: string;
  detail = $state<PipelineDetail | null>(null);
  figures = $state<Figures | null>(null);
  jobs = $state<JobDetail[]>([]);
  /** The newest events about it, newest first. */
  events = $state<EventItem[]>([]);
  read = $state<Record<PagePart, Read>>(
    Object.fromEntries(PAGE_PARTS.map((p) => [p, { at: null, error: null }])) as Record<PagePart, Read>,
  );
  /** The generation this node runs has no such pipeline. */
  missing = $state(false);
  readonly lag: LagHistory;

  #api: Client;
  #path: string;
  #pending = new Set<PagePart>();
  #timer: ReturnType<typeof setTimeout> | undefined;
  #now: () => number;

  /** `seed`: the figures the stream sent already, shown before the page's own read. */
  constructor(api: Client, id: string, seed: Figures | null = null, now: () => number = Date.now) {
    this.#api = api;
    this.id = id;
    this.#path = `/v1/pipelines/${encodeURIComponent(id)}`;
    this.figures = seed;
    this.#now = now;
    this.lag = new LagHistory(api, id, now);
  }

  /** Reads every part, and the lag's 6 hours. */
  async load(): Promise<void> {
    await Promise.all([this.#loadParts(PAGE_PARTS), this.lag.load()]);
  }

  async #loadParts(parts: PagePart[]): Promise<void> {
    await Promise.all(parts.map((p) => this.#loadPart(p)));
  }

  /** Follows `live` for this pipeline; returns the way to stop. */
  follow(live: Live): () => void {
    const stops = [
      // The lag's line reads itself again.
      live.onResync(() => void this.#loadParts(PAGE_PARTS)),
      live.on('figures', (d) => this.figuresMsg(d as Figures)),
      live.on('pipeline', (d) => this.pipelineMsg(d as PipelineMsg)),
      live.on('event', (d) => this.event(d as EventItem)),
      this.lag.follow(live),
    ];
    return () => {
      for (const stop of stops) stop();
      clearTimeout(this.#timer);
    };
  }

  /** Its figures, from the node that runs it: the newest owner's win. */
  figuresMsg(m: Figures): void {
    if (m.id !== this.id) return;
    if (this.figures !== null && this.figures.owner_epoch > m.owner_epoch) return;
    this.figures = m;
  }

  pipelineMsg(m: PipelineMsg): void {
    if (m.pipeline !== this.id || m.actual === 'standby') return;
    const f = this.figures;
    if (f !== null && f.owner_epoch > m.owner_epoch) return;
    if (f !== null) {
      f.actual = m.actual;
      f.error = m.error;
      f.node = m.node;
    }
    this.#reread(['detail']);
  }

  event(e: EventItem): void {
    if (e.pipeline !== this.id || this.events.some((x) => x.key === e.key)) return;
    this.events = [e, ...this.events].slice(0, ACTIVITY_READ);
    this.#reread(partsOf(e.kind));
  }

  #reread(parts: PagePart[]): void {
    for (const p of parts) this.#pending.add(p);
    if (this.#timer !== undefined || this.#pending.size === 0) return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      const parts = [...this.#pending];
      this.#pending.clear();
      void this.#loadParts(parts);
    }, REREAD_AFTER_MS);
  }

  async #loadPart(part: PagePart): Promise<void> {
    try {
      switch (part) {
        case 'detail':
          this.detail = await this.#api.get<PipelineDetail>(this.#path);
          this.missing = false;
          break;
        case 'live':
          this.figuresMsg(await this.#api.get<Figures>(`${this.#path}/live`));
          break;
        case 'jobs':
          this.jobs = (await this.#api.get<{ jobs: JobDetail[] }>(`${this.#path}/snapshots`)).jobs;
          break;
        case 'events':
          this.events = (
            await this.#api.get<{ events: EventItem[] }>(
              `/v1/events?object=${encodeURIComponent(this.id)}&limit=${ACTIVITY_READ}`,
            )
          ).events;
          break;
      }
      this.read[part] = { at: this.#now(), error: null };
    } catch (e) {
      if (part === 'detail' && e instanceof HttpError && e.status === 404) this.missing = true;
      this.read[part] = { at: this.read[part].at, error: e instanceof Error ? e.message : String(e) };
    }
  }
}
