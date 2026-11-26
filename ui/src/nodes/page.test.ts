import { describe, expect, it } from 'vitest';
import { HttpError, type Client } from '../lib/api';
import { event, iso, NOW } from '../model/fixtures';
import { nodeDetail, nodeEvents, nodeSeries } from './fixtures';
import { NODE_SERIES, NodePage, SPAN_S, STEP_S } from './page.svelte';

const HISTORY = `/v1/history?node=n2&series=${NODE_SERIES.join(',')}&since=${iso(NOW - SPAN_S * 1000)}&step=${STEP_S}`;

function api(answers: Record<string, unknown>): Client & { asked: string[] } {
  const asked: string[] = [];
  return {
    asked,
    get: async <T,>(path: string) => {
      asked.push(path);
      const a = answers[path];
      if (a instanceof HttpError) throw a;
      if (a === undefined) throw new HttpError(503, `${path} is unavailable`);
      return a as T;
    },
    send: async () => {
      throw new Error('nothing is changed');
    },
  };
}

const READS = {
  '/v1/nodes/n2': nodeDetail(),
  [HISTORY]: { series: Object.entries(nodeSeries()).map(([name, points]) => ({ name, node: 'n2', points })) },
  '/v1/events?node=n2&limit=50': { events: nodeEvents() },
};

describe("a node's page, as read", () => {
  it('reads its detail, its charts over 6 hours and what it recorded, each on its own', async () => {
    const a = api(READS);
    const p = new NodePage(a, 'n2', () => NOW);
    await p.load();
    expect(a.asked.sort()).toEqual(['/v1/events?node=n2&limit=50', '/v1/nodes/n2', HISTORY].sort());
    expect(p.detail?.build).toBe('1.1.0');
    expect(p.series).toEqual(nodeSeries());
    expect(p.events.map((e) => e.key)).toEqual(['e3', 'e2', 'e1']);
    expect(Object.values(p.read).every((r) => r.at === NOW && r.error === null)).toBe(true);
    expect([SPAN_S, STEP_S]).toEqual([6 * 3600, 60]);
  });

  it('says when no member has its name, and keeps what a failed part had', async () => {
    const answers: Record<string, unknown> = { ...READS, '/v1/nodes/n2': new HttpError(404, 'no member "n2"') };
    const p = new NodePage(api(answers), 'n2', () => NOW);
    await p.load();
    expect(p.missing).toBe(true);
    delete answers[HISTORY];
    await p.load(['history']);
    expect(p.series).toEqual(nodeSeries());
    expect(p.read.history.error).toBe(`${HISTORY} is unavailable`);
  });

  it("adds what the node records as it comes, once, and nothing another node records", () => {
    const p = new NodePage(api(READS), 'n2', () => NOW);
    const e = event({ key: 'e4', kind: 'lease.acquired', at: iso(NOW), node: 'n2', text: 'n2 acquired crm-pg' });
    p.event(e);
    p.event(e);
    p.event(event({ key: 'e5', kind: 'lease.acquired', at: iso(NOW), node: 'n1', text: 'n1 acquired billing' }));
    expect(p.events.map((x) => x.key)).toEqual(['e4']);
  });
});
