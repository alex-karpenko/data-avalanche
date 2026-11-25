// Pipelines' lag over the last 6 hours (mockups 05, 06): read once from `/v1/history`, then each
// sample the stream sends joins its line. A read that fails keeps the lines and says why.

import type { Client } from '../lib/api';
import type { Live } from '../lib/live.svelte';
import type { SamplesMsg } from '../model/model.svelte';
import type { Series } from '../model/types';

/** The span the lines cover. */
export const SPAN_S = 6 * 3600;
/** Seconds between the points read: 360 a line. */
export const STEP_S = 60;

export class LagHistory {
  /** Each pipeline's line, `[unix seconds, seconds]`, oldest first. */
  lines = $state<Record<string, [number, number][]>>({});
  /** Why the last read failed, `null` when it didn't. */
  error = $state<string | null>(null);

  #api: Client;
  #pipeline: string | null;
  #now: () => number;
  /** Asked once: what the stream missed is read again; never asked, nothing is. */
  #asked = false;

  /** Every pipeline's lines, or `pipeline`'s alone. */
  constructor(api: Client, pipeline: string | null = null, now: () => number = Date.now) {
    this.#api = api;
    this.#pipeline = pipeline;
    this.#now = now;
  }

  /** Reads the lines unless they were read already; a read that failed is tried again. */
  async ensure(): Promise<void> {
    if (!this.#asked || this.error !== null) await this.load();
  }

  async load(): Promise<void> {
    this.#asked = true;
    const since = new Date(this.#now() - SPAN_S * 1000).toISOString();
    const only = this.#pipeline === null ? '' : `&pipeline=${encodeURIComponent(this.#pipeline)}`;
    try {
      const { series } = await this.#api.get<{ series: Series[] }>(
        `/v1/history?series=lag_seconds${only}&since=${since}&step=${STEP_S}`,
      );
      this.lines = Object.fromEntries(
        series.filter((s) => s.pipeline !== undefined).map((s) => [s.pipeline!, s.points]),
      );
      this.error = null;
    } catch (e) {
      this.error = e instanceof Error ? e.message : String(e);
    }
  }

  /** Follows `live`'s samples, and reads again after a gap; returns the way to stop. */
  follow(live: Live): () => void {
    const stops = [
      live.onResync(() => {
        if (this.#asked) void this.load();
      }),
      live.on('samples', (d) => this.sample(d as SamplesMsg)),
    ];
    return () => stops.forEach((s) => s());
  }

  sample(m: SamplesMsg): void {
    const lag = m.series?.lag_seconds;
    if (m.scope !== 'pipeline' || m.pipeline === undefined || lag === undefined) return;
    if (this.#pipeline !== null && m.pipeline !== this.#pipeline) return;
    const line = [...(this.lines[m.pipeline] ?? []), [m.at, lag] as [number, number]];
    this.lines[m.pipeline] = line.filter(([t]) => t > m.at - SPAN_S);
  }
}
