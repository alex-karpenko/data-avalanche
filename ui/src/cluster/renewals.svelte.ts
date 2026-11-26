// Each member's lease renewals over the last hour (Admin_Console §4, mockup 03's "Lease renewal"),
// read from the chart history one node at a time: a node's series are asked of it by name.

import type { Client } from '../lib/api';
import type { PartRead } from '../lib/notices';
import type { Series } from '../model/types';

/** The node series the history keeps of its lease renewals (seconds, the step's slowest). */
export const RENEWALS_SERIES = 'lease_renew_seconds';

export class Renewals {
  /** Each member's slowest renewal in the last hour (s); a member with no sample has none. */
  max = $state<Record<string, number>>({});
  read = $state<Record<string, PartRead>>({});

  #api: Client;
  #now: () => number;

  constructor(api: Client, now: () => number = Date.now) {
    this.#api = api;
    this.#now = now;
  }

  /** Reads `nodes`' renewals, each on its own; forgets a node not among them. */
  async load(nodes: string[]): Promise<void> {
    const keep = new Set(nodes);
    this.max = Object.fromEntries(Object.entries(this.max).filter(([n]) => keep.has(n)));
    this.read = Object.fromEntries(Object.entries(this.read).filter(([n]) => keep.has(n)));
    await Promise.all(nodes.map((n) => this.#loadOne(n)));
  }

  async #loadOne(node: string): Promise<void> {
    const path = `/v1/history?node=${encodeURIComponent(node)}&series=${RENEWALS_SERIES}`;
    try {
      const { series } = await this.#api.get<{ series: Series[] }>(path);
      const points = series.find((s) => s.name === RENEWALS_SERIES)?.points ?? [];
      if (points.length === 0) delete this.max[node];
      else this.max[node] = Math.max(...points.map(([, v]) => v));
      this.read[node] = { at: this.#now(), error: null };
    } catch (e) {
      this.read[node] = { at: this.read[node]?.at ?? null, error: e instanceof Error ? e.message : String(e) };
    }
  }
}
