// The tests' cluster: mockup 01's, in the API's shapes.

import type { Facts } from './facts';
import type { Actual, ClusterStatus, Desired, EventItem, Figures, PipelineItem, SourceAt } from './types';

export const NOW = Date.UTC(2026, 10, 25, 14, 2, 0);
const MIN = 60_000;
const GIB = 1024 ** 3;
export const iso = (ms: number) => new Date(ms).toISOString();

export function figures(over: Partial<Figures> & Pick<Figures, 'id' | 'node'>): Figures {
  return {
    owner_epoch: 2,
    actual: 'running',
    as_of: iso(NOW),
    error: null,
    source: { detail: null },
    tables: [],
    worker: { lag_seconds: 1, degraded: null },
    ...over,
  };
}

export function event(over: Partial<EventItem> & Pick<EventItem, 'kind' | 'at'>): EventItem {
  return { key: `${over.at}-${over.kind}`, node: 'n1', severity: 'info', text: over.kind, ...over };
}

const pause = event({
  kind: 'action.pipeline.pause',
  at: iso(NOW - 120 * MIN),
  pipeline: 'inventory',
  text: 'pause inventory',
  actor: { name: 'Oleksii', role: 'operator' },
  outcome: 'accepted',
});

/** Every 5 min over the last hour: `f(minutes ago)`. */
const line = (f: (ago: number) => number): [number, number][] =>
  Array.from({ length: 13 }, (_, i) => 60 - i * 5).map((ago) => [(NOW - ago * MIN) / 1000, f(ago)]);

const pg = (db: string, host: string): SourceAt => ({ type: 'postgres', host, port: 5432, db });

function item(id: string, desired: Desired, actual: Actual, holder: string, source: SourceAt): PipelineItem {
  return {
    id,
    name: id,
    desired_state: desired,
    actual_state: actual,
    holder,
    holder_stale: false,
    source,
    sink: { type: 'delta', bucket: 'lake', prefix: id },
  };
}

function lease(id: string, holder: string, owner_epoch: number, ring_owner: string): ClusterStatus['pipelines'][number] {
  return { id, holder, holder_stale: false, owner_epoch, expires_at: iso(NOW + 8_000), ring_owner };
}

export function facts(): Facts {
  return {
    pipelines: [
      item('orders-pg', 'running', 'running', 'n2', pg('orders', 'pg-prod-replica')),
      item('billing-mysql', 'running', 'running', 'n1', { type: 'mysql', host: 'mysql-8', port: 3306, db: 'billing' }),
      item('inventory', 'paused', 'paused', 'n3', pg('inventory', 'pg-prod')),
      item('customers', 'running', 'running', 'n1', pg('crm', 'pg-crm')),
    ],
    cluster: {
      node: 'n1',
      members: ['n1', 'n2', 'n3'].map((node) => ({ node, phase: 'active', suspect: false, config_generation: 42 })),
      pipelines: [
        // Held by n2 away from its ring owner, since a failover.
        lease('orders-pg', 'n2', 2, 'n1'),
        lease('billing-mysql', 'n1', 3, 'n1'),
        lease('inventory', 'n3', 1, 'n3'),
        lease('customers', 'n1', 1, 'n1'),
      ],
    },
    config: {
      generation: 42,
      nodes: [
        { node: 'n1', generation: 42, file: 'same', restart_pending: [] },
        { node: 'n2', generation: 42, file: 'same', restart_pending: ['globals.api.address'] },
        { node: 'n3', generation: 41, file: 'differs', restart_pending: [] },
      ],
    },
    jobs: [
      { job_id: 'customers~019ab5d07c3a7d5e8f0a1b2c3d4e5f60', pipeline: 'customers', status: 'running', rows_done: 62, estimate: 100, requested_at: iso(NOW - 20 * MIN), finished_at: null, error: null },
      { job_id: 'billing-mysql~019ab4f25b1e7f40a3c2d1e0f9a8b7c6', pipeline: 'billing-mysql', status: 'failed', rows_done: 10, estimate: 50, requested_at: iso(NOW - 200 * MIN), finished_at: iso(NOW - 180 * MIN), error: 'the source connection was lost' },
    ],
    tables: [
      { pipeline: 'orders-pg', table: 'public.refunds', state: 'halted', halt: { code: 'SCHEMA_REFUSED', detail: "amount changed int → text, which can't be applied in place", since: iso(NOW - 12 * MIN) }, last_commit: iso(NOW - 15 * MIN) },
      { pipeline: 'orders-pg', table: 'public.orders', state: 'streaming', halt: null, last_commit: iso(NOW - 8_000) },
      { pipeline: 'billing-mysql', table: 'billing.invoices', state: 'streaming', halt: null, last_commit: iso(NOW - 3 * MIN) },
      { pipeline: 'inventory', table: 'public.items', state: 'streaming', halt: null, last_commit: iso(NOW - 120 * MIN) },
      { pipeline: 'customers', table: 'public.customers', state: 'snapshot', halt: null, last_commit: iso(NOW - 12_000) },
    ],
    figures: {
      'billing-mysql': figures({
        id: 'billing-mysql',
        node: 'n1',
        owner_epoch: 3,
        source: { detail: { engine: 'mysql' } },
        tables: [{ table: 'billing.invoices', state: 'streaming', halt: null, last_commit: iso(NOW - 3 * MIN) }],
        worker: { lag_seconds: 252, degraded: null },
      }),
      inventory: figures({
        id: 'inventory',
        node: 'n3',
        actual: 'paused',
        owner_epoch: 1,
        source: { detail: { engine: 'postgres', slot: { name: 'da_inventory', wal_held_bytes: 3.2 * GIB }, max_slot_wal_keep_size: 4 * GIB } },
        tables: [{ table: 'public.items', state: 'streaming', halt: null, last_commit: iso(NOW - 120 * MIN) }],
        worker: { lag_seconds: null, degraded: null },
      }),
      'orders-pg': figures({
        id: 'orders-pg',
        node: 'n2',
        source: { detail: { engine: 'postgres', slot: { name: 'da_orders', wal_held_bytes: 0.2 * GIB }, max_slot_wal_keep_size: null } },
        tables: [
          { table: 'public.refunds', state: 'halted', halt: { code: 'SCHEMA_REFUSED', detail: "amount changed int → text, which can't be applied in place", since: iso(NOW - 12 * MIN) }, last_commit: iso(NOW - 15 * MIN) },
          { table: 'public.orders', state: 'streaming', halt: null, last_commit: iso(NOW - 8_000) },
        ],
        worker: { lag_seconds: 21, degraded: null },
      }),
    },
    lag: {
      // Over the target since 10 min ago, and rising.
      'billing-mysql': line((ago) => (ago > 10 ? 10 : 252 - ago * 19)),
      'orders-pg': line(() => 21),
    },
    events: [
      event({ kind: 'lease.acquired', at: iso(NOW - 7 * MIN), pipeline: 'orders-pg', text: 'orders-pg runs on n2 (owner epoch 2)' }),
      pause,
    ],
    actions: { inventory: pause },
    leaseLosses: [],
    unreachable: {},
    unheld: {},
  };
}
