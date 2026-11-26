// The console's own status (Admin_Console §7, §8; mockup 11), shown from the user menu: whether
// the live updates come, how many nodes answer, the built-in history, Prometheus and the event
// store.

import { at, type Facts } from '../model/facts';
import type { Part, Read } from '../model/model.svelte';
import { ago, clock, duration } from './format';
import type { State } from './live.svelte';

export type StatusTone = 'ok' | 'yellow' | 'red' | 'muted' | null;

export interface StatusRow {
  label: string;
  /** A short state shown as a chip, before the text. */
  chip: string | null;
  text: string;
  tone: StatusTone;
}

export interface StatusInput {
  state: State;
  /** The node the live updates come through. */
  via: string | null;
  /** When the last message came (ms). */
  last: number | null;
  facts: Facts;
  read: Record<Part, Read>;
}

/** A retention as people say it: `24 h`, `30 days`, else as a duration. */
export function retention(seconds: number): string {
  if (seconds >= 2 * 86_400 && seconds % 86_400 === 0) return `${seconds / 86_400} days`;
  if (seconds % 3_600 === 0) return `${seconds / 3_600} h`;
  return duration(seconds);
}

const row = (label: string, chip: string | null, text: string, tone: StatusTone): StatusRow => ({
  label,
  chip,
  text,
  tone,
});

function liveRow(i: StatusInput, now: number): StatusRow {
  const since = i.last === null ? '' : `last update ${ago(i.last, now)}`;
  switch (i.state) {
    case 'live':
      return row('Live updates', 'connected', i.via === null ? '' : `via ${i.via}`, 'ok');
    case 'reconnecting':
      return row('Live updates', 'reconnecting', since, 'yellow');
    case 'offline':
      return row('Live updates', 'offline', since, 'red');
    case 'connecting':
      return row('Live updates', 'connecting', since, null);
  }
}

function nodesRow(f: Facts, now: number): StatusRow {
  if (f.cluster === null) return row('Nodes answering', null, 'not read yet', 'muted');
  const names = f.cluster.members.map((m) => m.node);
  const down = names.filter((n) => n in f.unreachable);
  const text = [
    `${names.length - down.length} of ${names.length}`,
    ...down.map((n) => `${n} ${duration((now - f.unreachable[n]) / 1000)}`),
  ].join(' · ');
  return row('Nodes answering', null, text, down.length > 0 ? 'yellow' : null);
}

function historyRow(f: Facts, now: number): StatusRow {
  const kept = f.cluster?.settings?.history_retention_seconds;
  const sample = f.lastSample === null ? 'no sample yet' : `last sample ${ago(f.lastSample, now)}`;
  const text = [kept === undefined ? null : retention(kept), sample].filter((s) => s !== null).join(' · ');
  return row('Built-in history', null, text, kept === undefined && f.lastSample === null ? 'muted' : null);
}

function prometheusRow(f: Facts, read: Read): StatusRow {
  const p = f.prometheus;
  if (p === null) {
    return read.at === null
      ? row('Prometheus', null, 'not read yet', 'muted')
      : row('Prometheus', null, 'not configured: charts show the built-in history only', 'muted');
  }
  const asked = at(p.at);
  const when = asked === null ? null : `last asked ${clock(asked)}`;
  switch (p.state) {
    case 'unasked':
      return row('Prometheus', null, "configured; no chart has reached past the built-in history yet", 'muted');
    case 'ok':
      return row('Prometheus', 'ok', when ?? '', 'ok');
    case 'failing':
      return row('Prometheus', 'failing', [p.error ?? null, when].filter((s) => s !== null).join(' · '), 'red');
  }
}

function eventsRow(f: Facts, read: Read): StatusRow {
  if (read.error !== null) return row('Event store', 'failing', read.error, 'red');
  if (read.at === null) return row('Event store', null, 'not read yet', 'muted');
  const kept = f.cluster?.settings?.events_retention_seconds;
  return row('Event store', 'ok', kept === undefined ? '' : retention(kept), 'ok');
}

/** The console's status rows, in the user menu's order. */
export function consoleStatus(i: StatusInput, now: number): StatusRow[] {
  return [
    liveRow(i, now),
    nodesRow(i.facts, now),
    historyRow(i.facts, now),
    prometheusRow(i.facts, i.read.lag),
    eventsRow(i.facts, i.read.events),
  ];
}
