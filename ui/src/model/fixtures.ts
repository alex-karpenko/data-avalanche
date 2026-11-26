// The tests' cluster: mockup 01's, in the API's shapes.

import type { Facts } from './facts';
import type { Actual, ClusterStatus, Desired, EventItem, Figures, NodeFigures, PipelineItem, SourceAt, TableItem } from './types';

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

/** A node's live figures: alive peers, a recent probe, 1.3 GiB of a 2 GiB limit, and `pipelines`. */
export function nodeFigures(node: string, over: Partial<NodeFigures> = {}): NodeFigures {
  return {
    node,
    at: iso(NOW - 2_000),
    phase: 'active',
    process: { memory_bytes: 1.3 * GIB, memory_limit_bytes: 2 * GIB, cpu_seconds: 5400, cpu_cores: 0.6, cpu_limit_cores: 2 },
    gossip: {
      peers: ['n1', 'n2', 'n3'].filter((n) => n !== node).map((n) => ({ node: n, state: 'alive' as const, phase: 'active' })),
      last_probe: { node: node === 'n1' ? 'n2' : 'n1', at: iso(NOW - 400) },
      probe_rtt_ms: 1.2,
      suspicions: { raised: [], received: 0, raised_more: 0, received_more: 0 },
    },
    pipelines: [],
    ...over,
  };
}

/** A table with nothing to say but what `over` says: streaming, no figures yet. */
export function table(pipeline: string, name: string, over: Partial<TableItem> = {}): TableItem {
  return {
    pipeline,
    table: name,
    state: 'streaming',
    halt: null,
    snapshot: null,
    changes_per_minute: 0,
    last_commit: null,
    version: null,
    files_rewritten: null,
    files: null,
    soft_dropped: 0,
    fresh_as_of: null,
    stale: false,
    ...over,
  };
}

/** A table as its pipeline's figures show it. */
const live = ({ pipeline: _p, ...t }: TableItem) => t;

const refunds = table('orders-pg', 'public.refunds', {
  state: 'halted',
  halt: { code: 'SCHEMA_REFUSED', detail: "amount changed int → text, which can't be applied in place", since: iso(NOW - 12 * MIN) },
  last_commit: iso(NOW - 15 * MIN),
  version: 2210,
});
const orders = table('orders-pg', 'public.orders', {
  changes_per_minute: 1840,
  last_commit: iso(NOW - 8_000),
  version: 18044,
  files_rewritten: 6,
  files: 820,
});

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
      members: ['n1', 'n2', 'n3'].map((node, i) => ({
        node,
        addr: `10.0.1.1${i + 1}:7946`,
        phase: 'active',
        suspect: false,
        config_generation: 42,
        build: '1.1.0',
        api_url: `https://10.0.1.1${i + 1}:8080`,
        // n3 restarted two hours ago; the others have been up three days.
        started_at: iso(node === 'n3' ? NOW - 2 * 60 * MIN : NOW - 3 * 24 * 60 * MIN),
      })),
      pipelines: [
        // Held by n2 away from its ring owner, since a failover.
        lease('orders-pg', 'n2', 2, 'n1'),
        lease('billing-mysql', 'n1', 3, 'n1'),
        lease('inventory', 'n3', 1, 'n3'),
        lease('customers', 'n1', 1, 'n1'),
      ],
      settings: {
        tokens: 128,
        lock_backend: 'storage',
        lease_seconds: 15,
        rebalance_stabilization_seconds: 15,
        max_concurrent_handoffs: 1,
        drain_timeout_seconds: 60,
        history_retention_seconds: 86_400,
        events_retention_seconds: 2_592_000,
      },
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
      { job_id: 'customers~019ab5d07c3a7d5e8f0a1b2c3d4e5f60', pipeline: 'customers', status: 'running', rows_done: 62, estimate: 100, eta_seconds: 420, requested_at: iso(NOW - 20 * MIN), finished_at: null, error: null },
      { job_id: 'billing-mysql~019ab4f25b1e7f40a3c2d1e0f9a8b7c6', pipeline: 'billing-mysql', status: 'failed', rows_done: 10, estimate: 50, eta_seconds: null, requested_at: iso(NOW - 200 * MIN), finished_at: iso(NOW - 180 * MIN), error: 'the source connection was lost' },
    ],
    tables: [
      refunds,
      orders,
      table('billing-mysql', 'billing.invoices', { last_commit: iso(NOW - 3 * MIN) }),
      table('inventory', 'public.items', { last_commit: iso(NOW - 120 * MIN) }),
      table('customers', 'public.customers', { state: 'snapshot', snapshot: { rows_done: 62, waiting: false }, last_commit: iso(NOW - 12_000) }),
    ],
    figures: {
      'billing-mysql': figures({
        id: 'billing-mysql',
        node: 'n1',
        owner_epoch: 3,
        source: { detail: { engine: 'mysql' } },
        tables: [live(table('billing-mysql', 'billing.invoices', { changes_per_minute: 2210, last_commit: iso(NOW - 3 * MIN) }))],
        worker: { lag_seconds: 252, degraded: null },
      }),
      inventory: figures({
        id: 'inventory',
        node: 'n3',
        actual: 'paused',
        owner_epoch: 1,
        source: { detail: { engine: 'postgres', slot: { name: 'da_inventory', wal_held_bytes: 3.2 * GIB }, max_slot_wal_keep_size: 4 * GIB } },
        tables: [live(table('inventory', 'public.items', { last_commit: iso(NOW - 120 * MIN) }))],
        worker: { lag_seconds: null, degraded: null },
      }),
      'orders-pg': figures({
        id: 'orders-pg',
        node: 'n2',
        source: { detail: { engine: 'postgres', slot: { name: 'da_orders', wal_held_bytes: 0.2 * GIB }, max_slot_wal_keep_size: null } },
        tables: [live(refunds), live(orders)],
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
    removals: [],
    nodes: Object.fromEntries(['n1', 'n2', 'n3'].map((n) => [n, nodeFigures(n)])),
  };
}
