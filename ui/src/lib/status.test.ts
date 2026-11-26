import { describe, expect, it } from 'vitest';
import { facts, iso, NOW } from '../model/fixtures';
import { PARTS, type Part, type Read } from '../model/model.svelte';
import { consoleStatus, retention, type StatusInput } from './status';

const ASKED = new Date(2026, 10, 25, 13, 40).getTime();

function reads(over: Partial<Record<Part, Read>> = {}): Record<Part, Read> {
  return { ...(Object.fromEntries(PARTS.map((p) => [p, { at: NOW - 1_000, error: null }])) as Record<Part, Read>), ...over };
}

function input(over: Partial<StatusInput> = {}): StatusInput {
  const f = facts();
  f.unreachable = { n3: NOW - 40_000 };
  f.lastSample = NOW - 6_000;
  f.prometheus = { state: 'failing', at: iso(ASKED), error: 'connection refused' };
  return { state: 'live', via: 'n1', last: NOW - 2_000, facts: f, read: reads(), ...over };
}

describe("the console's status", () => {
  it('says how the live updates, the nodes, the history, Prometheus and the event store are', () => {
    expect(consoleStatus(input(), NOW)).toEqual([
      { label: 'Live updates', chip: 'connected', text: 'via n1', tone: 'ok' },
      { label: 'Nodes answering', chip: null, text: '2 of 3 · n3 40 s', tone: 'yellow' },
      { label: 'Built-in history', chip: null, text: '24 h · last sample 6 s ago', tone: null },
      { label: 'Prometheus', chip: 'failing', text: 'connection refused · last asked 13:40', tone: 'red' },
      { label: 'Event store', chip: 'ok', text: '30 days', tone: 'ok' },
    ]);
  });

  it('says when the live updates stopped, and since when', () => {
    const row = (i: Partial<StatusInput>) => consoleStatus(input(i), NOW)[0];
    expect(row({ state: 'reconnecting', last: NOW - 14_000 })).toEqual({
      label: 'Live updates',
      chip: 'reconnecting',
      text: 'last update 14 s ago',
      tone: 'yellow',
    });
    expect(row({ state: 'offline', last: NOW - 120_000 })).toEqual({
      label: 'Live updates',
      chip: 'offline',
      text: 'last update 2 min ago',
      tone: 'red',
    });
    expect(row({ state: 'connecting', last: null })).toEqual({ label: 'Live updates', chip: 'connecting', text: '', tone: null });
  });

  it('says what it has not read yet, and what is not configured', () => {
    const f = facts();
    f.cluster = null;
    f.prometheus = null;
    const unread = consoleStatus(
      input({ facts: f, read: reads({ lag: { at: null, error: null }, events: { at: null, error: null } }) }),
      NOW,
    );
    expect(unread.map((r) => [r.label, r.text, r.tone])).toEqual([
      ['Live updates', 'via n1', 'ok'],
      ['Nodes answering', 'not read yet', 'muted'],
      ['Built-in history', 'no sample yet', 'muted'],
      ['Prometheus', 'not read yet', 'muted'],
      ['Event store', 'not read yet', 'muted'],
    ]);
    const plain = consoleStatus(input({ facts: { ...facts(), prometheus: null, lastSample: null } }), NOW);
    expect(plain[1]).toEqual({ label: 'Nodes answering', chip: null, text: '3 of 3', tone: null });
    expect(plain[2].text).toBe('24 h · no sample yet');
    expect(plain[3]).toEqual({ label: 'Prometheus', chip: null, text: 'not configured: charts show the built-in history only', tone: 'muted' });
  });

  it("says how Prometheus answered when it did, or that no chart has asked it yet", () => {
    const prom = (p: NonNullable<StatusInput['facts']['prometheus']>) =>
      consoleStatus(input({ facts: { ...facts(), prometheus: p } }), NOW)[3];
    expect(prom({ state: 'ok', at: iso(ASKED), error: null })).toEqual({
      label: 'Prometheus',
      chip: 'ok',
      text: 'last asked 13:40',
      tone: 'ok',
    });
    expect(prom({ state: 'unasked' })).toEqual({
      label: 'Prometheus',
      chip: null,
      text: 'configured; no chart has reached past the built-in history yet',
      tone: 'muted',
    });
  });

  it("says why the event store can't be read", () => {
    const rows = consoleStatus(input({ read: reads({ events: { at: NOW - 60_000, error: 'the request timed out' } }) }), NOW);
    expect(rows[4]).toEqual({ label: 'Event store', chip: 'failing', text: 'the request timed out', tone: 'red' });
  });

  it('writes a retention in days or hours', () => {
    expect([86_400, 2_592_000, 604_800, 172_800, 3_600, 5_400].map(retention)).toEqual([
      '24 h',
      '30 days',
      '7 days',
      '2 days',
      '1 h',
      '1h 30m',
    ]);
  });
});
