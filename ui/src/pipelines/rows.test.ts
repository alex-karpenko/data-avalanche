import { describe, expect, it } from 'vitest';
import { facts, NOW } from '../model/fixtures';
import {
  counts,
  listRows,
  needingAttention,
  parseQuery,
  pipelineOf,
  queryString,
  sortBy,
  view,
  type ListQuery,
} from './rows';

const all = () => listRows(facts(), {}, NOW);
const base: ListQuery = { show: 'all', node: null, source: null, text: '', sort: null, desc: false };

describe("the pipeline list's rows", () => {
  it('are a pipeline each, worst first, with what a person scans for', () => {
    const rows = all();
    expect(rows.map((r) => r.id)).toEqual(['orders-pg', 'billing-mysql', 'customers', 'inventory']);
    const [orders, billing, customers, inventory] = rows;
    expect(orders).toMatchObject({
      route: 'PG orders@pg-prod-replica → Δ lake/orders-pg',
      node: 'n2',
      away: true,
      changes: 1840,
      tables: 2,
      halted: 1,
      snapshot: null,
    });
    expect(orders.state).toEqual({ label: '1 halted', tone: 'red', note: null });
    expect(billing).toMatchObject({ away: false, changes: 2210, snapshot: '019ab4f2 ✕ failed 3 h ago', job: null });
    // No live figures yet: its changes aren't known.
    expect(customers).toMatchObject({ changes: null, snapshot: '62% · ETA 7m 0s', job: '019ab5d0' });
    expect(inventory).toMatchObject({ desired: 'paused', actual: 'paused', changes: 0 });
  });

  it("draw each lag line from the last 6 hours' when they have them", () => {
    const line: [number, number][] = [
      [NOW / 1000 - 6 * 3600, 3],
      [NOW / 1000, 21],
    ];
    expect(listRows(facts(), { 'orders-pg': line }, NOW)[0].points).toEqual(line);
  });

  it('are counted for each filter; "Needs attention" names those its lines are about', () => {
    const flagged = needingAttention(facts(), NOW);
    expect([...flagged].sort()).toEqual(['billing-mysql', 'inventory', 'orders-pg']);
    expect(counts(all(), flagged)).toEqual({
      all: 4,
      attention: 3,
      running: 3,
      halted: 1,
      paused: 1,
      stopped: 0,
      snapshot: 1,
    });
  });

  it('are filtered by state, node, source and name, worst first unless a column sorts them', () => {
    const flagged = needingAttention(facts(), NOW);
    const ids = (q: Partial<ListQuery>) => view(all(), { ...base, ...q }, flagged).map((r) => r.id);
    expect(ids({ show: 'attention' })).toEqual(['orders-pg', 'billing-mysql', 'inventory']);
    expect(ids({ node: 'n1' })).toEqual(['billing-mysql', 'customers']);
    expect(ids({ source: 'mysql' })).toEqual(['billing-mysql']);
    expect(ids({ text: 'ORD' })).toEqual(['orders-pg']);
    expect(ids({ sort: 'lag', desc: true })).toEqual(['billing-mysql', 'orders-pg', 'customers', 'inventory']);
    expect(ids({ sort: 'pipeline' })).toEqual(['billing-mysql', 'customers', 'inventory', 'orders-pg']);
  });

  it('are sorted by a column biggest first when it is a number, the other way on a second click', () => {
    expect(sortBy(base, 'lag')).toMatchObject({ sort: 'lag', desc: true });
    expect(sortBy({ ...base, sort: 'lag', desc: true }, 'lag')).toMatchObject({ sort: 'lag', desc: false });
    expect(sortBy(base, 'pipeline')).toMatchObject({ sort: 'pipeline', desc: false });
  });

  it('keep their view in the address, read back as written', () => {
    const q: ListQuery = { show: 'halted', node: 'n2', source: 'postgres', text: 'ord', sort: 'lag', desc: true };
    expect(queryString(q)).toBe('?show=halted&node=n2&source=postgres&q=ord&sort=lag&desc');
    expect(parseQuery(new URLSearchParams(queryString(q)))).toEqual(q);
    expect(queryString(base)).toBe('');
    expect(parseQuery(new URLSearchParams('show=nonsense&sort=nope&source=oracle'))).toEqual(base);
  });

  it('find the pipeline a "Needs attention" line links to', () => {
    expect(pipelineOf('/ui/pipelines/orders%20pg')).toBe('orders pg');
    expect(pipelineOf('/ui/tables/orders-pg/public.refunds')).toBe('orders-pg');
    expect(pipelineOf('/ui/snapshots/billing-mysql~019ab4f2')).toBe('billing-mysql');
    expect(pipelineOf('/ui/config')).toBeNull();
    expect(pipelineOf('/ui/cluster/nodes/n1')).toBeNull();
  });
});
