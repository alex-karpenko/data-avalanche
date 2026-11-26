// The tests' pipeline page: mockup 06's orders-pg, in the API's shapes.

import { event, facts, iso, NOW } from '../model/fixtures';
import type { EventItem, Figures, JobDetail, PipelineDetail } from '../model/types';

const MIN = 60_000;

export function detail(over: Partial<PipelineDetail> = {}): PipelineDetail {
  const p = facts().pipelines[0];
  return {
    id: p.id,
    name: p.name,
    desired_state: 'running',
    actual_state: 'running',
    source: p.source,
    sink: p.sink,
    error: null,
    degraded: null,
    holder: { node: 'n2', owner_epoch: 2, renewed_at: iso(NOW - 2_000), released: false, stale: false, suspect: false },
    position: 'pg:3/A41C2B10',
    halted_tables: [{ table: 'public.refunds', code: 'SCHEMA_REFUSED', detail: "amount changed int → text, which can't be applied in place", since: iso(NOW - 12 * MIN) }],
    ...over,
  };
}

/** orders-pg's figures, with its source's detail. */
export function liveFigures(): Figures {
  const f = facts().figures['orders-pg'];
  return {
    ...f,
    source: {
      detail: {
        engine: 'postgres',
        head: 'pg:3/A41C9000',
        slot: { name: 'da_orders', active: true, confirmed_flush: 'pg:3/A41C2B10', wal_held_bytes: 210 * 1024 ** 2 },
        max_slot_wal_keep_size: 3 * 1024 ** 3,
        publication: { name: 'da_orders', tables: 14 },
      },
      read_at: iso(NOW - 4_000),
    },
  };
}

export function job(over: Partial<JobDetail> & Pick<JobDetail, 'job_id' | 'status'>): JobDetail {
  return {
    pipeline: 'orders-pg',
    rows_done: 0,
    estimate: null,
    eta_seconds: null,
    requested_at: iso(NOW - 60 * MIN),
    finished_at: null,
    error: null,
    tables: [{ table: 'public.order_items', status: 'copying', rows_done: 0, estimate: null }],
    rate: null,
    requested_by: 'Oleksii',
    requested_role: 'operator',
    note: null,
    truncate: false,
    cancel: null,
    ...over,
  };
}

export function jobs(): JobDetail[] {
  return [
    job({ job_id: 'orders-pg~019aa0a1b2c37d5e8f0a1b2c3d4e5f60', status: 'completed', requested_at: iso(NOW - 3 * 86_400_000), finished_at: iso(NOW - 2 * 86_400_000) }),
    job({
      job_id: 'orders-pg~019aa1c2d3e47d5e8f0a1b2c3d4e5f60',
      status: 'cancelled',
      requested_at: iso(NOW - 26 * 3_600_000),
      finished_at: iso(NOW - 25 * 3_600_000),
      cancel: { by: 'ci-bot', role: 'operator', at: iso(NOW - 25 * 3_600_000), note: null },
    }),
    job({
      job_id: 'orders-pg~0192f0c47d5e7d5e8f0a1b2c3d4e5f60',
      status: 'running',
      requested_at: iso(Date.UTC(2026, 10, 25, 13, 10)),
      rows_done: 2_600_000,
      estimate: 4_200_000,
      eta_seconds: 420,
      rate: 41_000,
      note: 'backfill after the 12:40 outage',
    }),
  ];
}

export function events(): EventItem[] {
  return [
    event({ kind: 'lease.takeover', at: iso(NOW - 7 * MIN), pipeline: 'orders-pg', text: 'orders-pg taken over by n2 from n1 · epoch 1 → 2' }),
    event({ kind: 'table.halted', at: iso(NOW - 12 * MIN), pipeline: 'orders-pg', object: 'public.refunds', severity: 'error', text: 'public.refunds halted: SCHEMA_REFUSED' }),
    event({ kind: 'schema.applied', at: iso(NOW - 40 * MIN), pipeline: 'orders-pg', object: 'public.orders', text: 'public.orders: column reason added' }),
    event({
      kind: 'action.snapshot.start',
      at: iso(NOW - 52 * MIN),
      pipeline: 'orders-pg',
      text: 'started job 0192f0c4 on orders-pg',
      actor: { name: 'Oleksii', role: 'operator' },
      note: 'backfill after the 12:40 outage',
      outcome: 'accepted',
    }),
    event({ kind: 'action.pipeline.restart', at: iso(NOW - 3 * 3_600_000), pipeline: 'orders-pg', text: 'restart orders-pg', actor: { name: 'Oleksii', role: 'operator' }, outcome: 'refused' }),
    event({ kind: 'action.pipeline.restart', at: iso(NOW - 4 * 3_600_000), pipeline: 'orders-pg', text: 'restart orders-pg', actor: { name: 'Oleksii', role: 'operator' }, note: 'apply sink.prefix', outcome: 'accepted' }),
    event({ kind: 'action.pipeline.pause', at: iso(NOW - 7 * 3_600_000), pipeline: 'orders-pg', text: 'pause orders-pg', outcome: 'accepted' }),
  ];
}
