import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { HttpError, type Client } from '../lib/api';
import { event, facts, figures, iso, nodeFigures, NOW } from './fixtures';
import { Model, PART_NAMES, REREAD_AFTER_MS } from './model.svelte';

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
      throw new Error('the model changes nothing');
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
      // A refused stop (a 409) is recorded under its action's kind too.
      event({ kind: 'action.pipeline.stop', at: iso(NOW - 30_000), pipeline: 'customers', severity: 'warning', outcome: 'refused' }),
      event({ kind: 'action.pipeline.resume', at: iso(NOW - 60_000), pipeline: 'customers', outcome: 'accepted' }),
      facts().actions.inventory,
      event({ kind: 'action.pipeline.pause', at: iso(NOW - 120_000), pipeline: 'customers', outcome: 'accepted' }),
    ],
  },
  [`/v1/events?kind=lease.lost,lease.takeover&since=${iso(NOW - 3_600_000)}&limit=1000`]: {
    events: [event({ kind: 'lease.takeover', at: iso(NOW - 50 * 60_000), pipeline: 'customers', text: 'customers taken over from n1 by n3' })],
  },
  '/v1/cluster/removals': {
    removals: [{ id: 'payments', generation: 41, removed_at: iso(NOW - 4 * 60_000) }],
  },
  '/v1/history?series=lag_seconds': {
    series: [{ name: 'lag_seconds', pipeline: 'orders-pg', points: [[NOW / 1000, 21]] }],
  },
};

describe("the console's model", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  it('is read part by part; a part that fails keeps what it had and says why', async () => {
    const o = new Model(api(READS));
    await o.load();
    expect(o.facts.pipelines.map((p) => p.id)).toEqual(['orders-pg', 'billing-mysql', 'inventory', 'customers']);
    expect(o.facts.actions.customers.kind).toBe('action.pipeline.resume');
    expect(o.facts.leaseLosses.map((e) => e.text)).toEqual(['customers taken over from n1 by n3']);
    expect(o.facts.lag['orders-pg']).toEqual([[NOW / 1000, 21]]);
    expect(o.facts.removals.map((r) => r.id)).toEqual(['payments']);
    expect(o.facts.cluster?.settings?.lease_seconds).toBe(15);
    expect(Object.values(o.read).every((r) => r.at === NOW && r.error === null)).toBe(true);
    const { '/v1/snapshots': _, ...without } = READS;
    const later = new Model(api(without));
    later.facts.jobs = facts().jobs;
    await later.load();
    expect(later.read.jobs).toEqual({ at: null, error: '/v1/snapshots is unavailable' });
    expect(later.facts.jobs).toHaveLength(2);
    expect(later.read.pipelines.error).toBeNull();
  });

  it("follows the stream: the newest owner's figures, a state, the lag's last hour", () => {
    const o = new Model(api({}));
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

  it('reads the gap when a node is followed again, and forgets a node gone from the cluster', async () => {
    const client = api(READS);
    const o = new Model(client);
    o.facts = facts();
    o.peer({ peer: 'n3', state: 'following' });
    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
    expect(client.asked).toEqual([]);
    o.peer({ peer: 'n3', state: 'unreachable', error: 'it answered 503' });
    o.peer({ peer: 'n3', state: 'following' });
    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
    expect(client.asked.sort()).toEqual([
      '/v1/cluster/status',
      `/v1/events?kind=lease.lost,lease.takeover&since=${iso(NOW - 3_600_000 + 2 * REREAD_AFTER_MS)}&limit=1000`,
      '/v1/events?limit=100',
      '/v1/pipelines',
    ]);
    // A member removed gets no more `peer`: the cluster's members say it's gone.
    o.peer({ peer: 'n4', state: 'unreachable', error: "it can't be reached" });
    o.peer({ peer: 'n2', state: 'unreachable', error: "it can't be reached" });
    await o.load(['cluster']);
    expect(Object.keys(o.facts.unreachable)).toEqual(['n2']);
  });

  it('keeps only accepted pauses, stops and resumes, read or streamed', () => {
    const o = new Model(api({}));
    o.facts = facts();
    const before = o.facts.actions.inventory;
    o.event(event({ key: 'refused', kind: 'action.pipeline.resume', at: iso(NOW), pipeline: 'inventory', severity: 'warning', outcome: 'refused', actor: { name: 'ci-bot', role: 'operator' } }));
    expect(o.facts.actions.inventory).toEqual(before);
    const resumed = event({ kind: 'action.pipeline.resume', at: iso(NOW), pipeline: 'inventory', outcome: 'accepted', actor: { name: 'ci-bot', role: 'operator' } });
    o.event(resumed);
    expect(o.facts.actions.inventory).toEqual(resumed);
  });

  it("forgets a removed pipeline's figures, lag, actions and lease", async () => {
    const o = new Model(api({ '/v1/pipelines': { pipelines: facts().pipelines.filter((p) => p.id !== 'orders-pg') } }));
    o.facts = facts();
    o.facts.actions['orders-pg'] = o.facts.actions.inventory;
    o.facts.unheld = { 'orders-pg': NOW, customers: NOW };
    await o.load(['pipelines']);
    expect(Object.keys(o.facts.figures).sort()).toEqual(['billing-mysql', 'inventory']);
    expect(Object.keys(o.facts.lag)).toEqual(['billing-mysql']);
    expect(Object.keys(o.facts.actions)).toEqual(['inventory']);
    expect(o.facts.unheld).toEqual({ customers: NOW });
  });

  it("keeps the hour's lease losses: a streamed one joins them once", async () => {
    const client = api(READS);
    const o = new Model(client);
    o.facts = facts();
    const lost = event({ kind: 'lease.lost', at: iso(NOW), pipeline: 'customers', text: 'n1 lost the lease of customers' });
    o.event(lost);
    o.event(lost);
    o.event(event({ kind: 'lease.acquired', at: iso(NOW), pipeline: 'customers' }));
    expect(o.facts.leaseLosses).toEqual([lost]);
    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
    expect(client.asked).toContain(
      `/v1/events?kind=lease.lost,lease.takeover&since=${iso(NOW - 3_600_000 + REREAD_AFTER_MS)}&limit=1000`,
    );
  });

  it('remembers since when a lease has had no holder, until one holds it', async () => {
    const status = structuredClone(facts().cluster!);
    status.pipelines[3].holder = null;
    const o = new Model(api({ '/v1/cluster/status': status }));
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
    const o = new Model(client);
    o.facts = facts();
    const paused = event({ kind: 'action.pipeline.pause', at: iso(NOW), pipeline: 'customers', actor: { name: 'ci-bot', role: 'operator' }, outcome: 'accepted' });
    o.event(paused);
    o.event(paused);
    o.event(event({ kind: 'job.completed', at: iso(NOW), pipeline: 'customers' }));
    o.event(event({ kind: 'config.published', at: iso(NOW) }));
    expect(o.facts.events.filter((e) => e.key === paused.key)).toHaveLength(1);
    expect(o.facts.events[0].kind).toBe('config.published');
    expect(o.facts.actions.customers).toEqual(paused);
    expect(client.asked).toEqual([]);
    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
    // A person's pause, stop or resume changes the pipeline's desired state: its row reads again.
    expect(client.asked.sort()).toEqual(['/v1/cluster/removals', '/v1/config/nodes', '/v1/pipelines', '/v1/snapshots']);
  });
  it("keeps each member's own figures, the newest a node sent, and drops a node no longer a member", async () => {
    const o = new Model(api(READS));
    o.node(nodeFigures('n2', { at: iso(NOW - 5_000), pipelines: ['orders-pg'] }));
    o.node(nodeFigures('n2', { at: iso(NOW - 10_000), pipelines: [] }));
    expect(o.facts.nodes.n2.pipelines).toEqual(['orders-pg']);
    o.node(nodeFigures('n4'));
    await o.load(['cluster']);
    expect(Object.keys(o.facts.nodes)).toEqual(['n2']);
  });

  it('reads the pending removals again when a pipeline is removed or a generation published', async () => {
    const a = api(READS);
    const o = new Model(a);
    o.event(event({ kind: 'pipeline.removed', at: iso(NOW), pipeline: 'payments' }));
    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
    expect(a.asked).toContain('/v1/cluster/removals');
    a.asked.length = 0;
    o.event(event({ kind: 'config.published', at: iso(NOW), text: 'generation 43 published by ops' }));
    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
    expect(a.asked).toEqual(expect.arrayContaining(['/v1/config/nodes', '/v1/cluster/removals']));
  });

  it("keeps how Prometheus last answered the node, from the lag lines' read, and when the last sample came", async () => {
    const o = new Model(
      api({
        ...READS,
        '/v1/history?series=lag_seconds': {
          series: [],
          prometheus: { state: 'failing', at: iso(NOW - 60_000), error: 'connection refused' },
        },
      }),
    );
    await o.load(['lag']);
    expect(o.facts.prometheus).toEqual({ state: 'failing', at: iso(NOW - 60_000), error: 'connection refused' });
    const plain = new Model(api(READS));
    await plain.load(['lag']);
    expect(plain.facts.prometheus).toBeNull();
    expect(plain.facts.lastSample).toBeNull();
    plain.samples({ scope: 'node', at: NOW / 1000 - 6, series: { memory_bytes: 1 } });
    plain.samples({ scope: 'pipeline', pipeline: 'orders-pg', at: NOW / 1000 - 20, series: { lag_seconds: 3 } });
    expect(plain.facts.lastSample).toBe(NOW - 6_000);
  });

  it('names its parts as people read them, for the notices that say which failed', () => {
    expect(PART_NAMES).toEqual({
      pipelines: 'the pipelines',
      cluster: 'the cluster',
      config: "the nodes' config",
      jobs: 'the snapshot jobs',
      tables: 'the tables',
      events: 'the events',
      actions: "the pipelines' last actions",
      leases: "the hour's lease hand-offs",
      lag: 'the lag lines',
      removals: 'the pending removals',
    });
  });
});
