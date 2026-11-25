import { describe, expect, it } from 'vitest';
import { route } from './facts';
import { facts } from './fixtures';

describe('a pipeline\'s route', () => {
  it('names the source and the sink, short in a list and written out on its page', () => {
    const [orders, billing] = facts().pipelines;
    expect(route(orders)).toBe('PG orders@pg-prod-replica → Δ lake/orders-pg');
    expect(route(billing)).toBe('MY billing@mysql-8 → Δ lake/billing-mysql');
    expect(route(orders, true)).toBe('PostgreSQL orders@pg-prod-replica → Delta lake/orders-pg');
  });

  it('writes the lake path without the prefix\'s slashes', () => {
    const p = facts().pipelines[0];
    expect(route({ ...p, sink: { ...p.sink, prefix: '/orders/v2/' } })).toBe('PG orders@pg-prod-replica → Δ lake/orders/v2');
    expect(route({ ...p, sink: { ...p.sink, prefix: '' } })).toBe('PG orders@pg-prod-replica → Δ lake');
  });
});
