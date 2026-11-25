// What a page says when it isn't showing current data (Admin_Console §8): the live stream
// stopped, or a part it shows couldn't be read. One wording for every page.

import { ago, clock, duration } from './format';

/** When a part was last read (ms), and why its last read failed. */
export interface PartRead {
  at: number | null;
  error: string | null;
}

/** The notice's start, before the parts that couldn't be read. */
export const FAILING_PREFIX = "Couldn't be read; showing what was last read: ";

/**
 * The parts that failed their last read, each as people read it: `the snapshot jobs (HTTP 503),
 * last read 20 min ago`, or `…, never read`. `parts` pairs a part's name with its read.
 */
export function failures(parts: [name: string, read: PartRead][], now: number): string[] {
  return parts
    .filter(([, r]) => r.error !== null)
    .map(([name, r]) => `${name} (${r.error}), ${r.at === null ? 'never read' : `last read ${ago(r.at, now)}`}`);
}

/** The full notice for `entries`, `null` when nothing failed. */
export function failingNotice(entries: string[]): string | null {
  return entries.length === 0 ? null : `${FAILING_PREFIX}${entries.join('; ')}`;
}

/** The stale notice: how old the numbers are, or that no live update came at all. */
export function staleNotice(last: number | null, now: number): string {
  return last === null
    ? 'Not current: no live update yet'
    : `Not current: as of ${clock(last, true)} · ${duration((now - last) / 1000)} old`;
}
