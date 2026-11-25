import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { HttpError, type Client } from '../lib/api';
import { event, facts, figures, iso, NOW } from './fixtures';
import { Overview, REREAD_AFTER_MS } from './model.svelte';

/** A `Client` answering each path from `answers`; a path not there fails. */
function api(answers: Record<string, unknown>): Client & { asked: string[] } {
  const asked: string[] = [];
  return {
    asked,
    get: async <T,>(path: string) => {
      asked.push(path);
      if (!(path in answers)) throw new HttpError(503, `${path} is unavailable`);
      return answers[path] as T;
    },
    send: async () => {
      throw new Error('the Overview changes nothing');
    },
  };
}

const READS = {
  '/v1/pipelines': { pipelines: facts().pipelines },
  '/v1/cluster/status': facts().cluster,
  '/v1/config/nodes': facts().config,
  '/v1/snapshots': { jobs: facts().jobs, unreadable: [] },
  '/v1/tables': { as_of: iso(NOW), tables: facts().tables, unreachable: [] },
  '/v1/events?limit=100': { events: facts().events },
  '/v1/events?kind=action.pipeline.pause,action.pipeline.stop,action.pipeline.resume&limit=1000': {
    events: [
      event({ kind: 'action.pipeline.resume', at: iso(NOW - 60_000), pipeline: 'b' }),
      facts().actions.inventory,
      event({ kind: 'action.pipeline.pause', at: iso(NOW - 120_000), pipeline: 'b' }),
    ],
  },
  '/v1/history?series=lag_seconds': {
    series: [{ name: 'lag_seconds', pipeline: 'orders-pg', points: [[NOW / 1000, 21]] }],
  },
};

describe("the Overview's data", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  it('is read part by part; a part that fails keeps what it had and says why', async () => {
    const o = new Overview(api(READS));
    await o.load();
    expect(o.facts.pipelines.map((p) => p.id)).toEqual(['orders-pg', 'billing-mysql', 'inventory', 'customers']);
    expect(o.facts.actions.b.kind).toBe('action.pipeline.resume');
    expect(o.facts.lag['orders-pg']).toEqual([[NOW / 1000, 21]]);
    expect(Object.values(o.read).every((r) => r.at === NOW && r.error === null)).toBe(true);
    const { '/v1/snapshots': _, ...without } = READS;
    const later = new Overview(api(without));
    later.facts.jobs = facts().jobs;
    await later.load();
    expect(later.read.jobs).toEqual({ at: null, error: '/v1/snapshots is unavailable' });
    expect(later.facts.jobs).toHaveLength(2);
    expect(later.read.pipelines.error).toBeNull();
  });

  it("follows the stream: the newest owner's figures, a state, the lag's last hour", () => {
    const o = new Overview(api({}));
    o.facts = facts();
    o.figures(figures({ id: 'billing-mysql', node: 'n2', owner_epoch: 2, worker: { lag_seconds: 1, degraded: null } }));
    expect(o.facts.figures['billing-mysql'].node).toBe('n1');
    o.figures(figures({ id: 'billing-mysql', node: 'n2', owner_epoch: 4, worker: { lag_seconds: 1, degraded: null } }));
    expect(o.facts.figures['billing-mysql'].node).toBe('n2');
    o.pipeline({ pipeline: 'billing-mysql', actual: 'standby', error: null, node: 'n1', owner_epoch: 3 });
    expect(o.facts.pipelines[1].actual_state).toBe('running');
    o.pipeline({ pipeline: 'billing-mysql', actual: 'paused', error: null, node: 'n2', owner_epoch: 4 });
    expect([o.facts.pipelines[1].actual_state, o.facts.figures['billing-mysql'].actual]).toEqual(['paused', 'paused']);
    o.samples({ scope: 'pipeline', pipeline: 'orders-pg', at: NOW / 1000 + 3600, series: { lag_seconds: 30 } });
    expect(o.facts.lag['orders-pg'].at(-1)).toEqual([NOW / 1000 + 3600, 30]);
    expect(o.facts.lag['orders-pg'][0][0]).toBeGreaterThan(NOW / 1000);
    o.peer({ peer: 'n3', state: 'unreachable', error: 'it answered 503' });
    expect(o.facts.unreachable).toEqual({ n3: NOW });
    o.peer({ peer: 'n3', state: 'following' });
    expect(o.facts.unreachable).toEqual({});
  });

  it('remembers since when a lease has had no holder, until one holds it', async () => {
    const status = structuredClone(facts().cluster!);
    status.pipelines[3].holder = null;
    const o = new Overview(api({ '/v1/cluster/status': status }));
    await o.load(['cluster']);
    expect(o.facts.unheld).toEqual({ customers: NOW });
    vi.setSystemTime(NOW + 20_000);
    status.pipelines[2].holder_stale = true;
    await o.load(['cluster']);
    expect(o.facts.unheld).toEqual({ customers: NOW, inventory: NOW + 20_000 });
    status.pipelines[3].holder = 'n2';
    await o.load(['cluster']);
    expect(o.facts.unheld).toEqual({ inventory: NOW + 20_000 });
  });

  it('lists an event once, and reads again what it changed, a burst at once', async () => {
    const client = api(READS);
    const o = new Overview(client);
    o.facts = facts();
    const paused = event({ kind: 'action.pipeline.pause', at: iso(NOW), pipeline: 'customers', actor: { name: 'ci-bot', role: 'operator' } });
    o.event(paused);
    o.event(paused);
    o.event(event({ kind: 'job.completed', at: iso(NOW), pipeline: 'customers' }));
    o.event(event({ kind: 'config.published', at: iso(NOW) }));
    expect(o.facts.events.filter((e) => e.key === paused.key)).toHaveLength(1);
    expect(o.facts.events[0].kind).toBe('config.published');
    expect(o.facts.actions.customers).toEqual(paused);
    expect(client.asked).toEqual([]);
    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
    expect(client.asked.sort()).toEqual(['/v1/config/nodes', '/v1/snapshots']);
  });
});
