import { describe, expect, it } from 'vitest';
import { event, facts, iso, NOW, table } from '../model/fixtures';
import { activity, header, marks, position, snapshotPanel, sourceLines, tableLines } from './detail';
import { detail, events, jobs, liveFigures } from './fixtures';

const lease = () => facts().cluster!.pipelines[0];

describe("a pipeline's page", () => {
  it("heads with its state, its route written out, its node, away from its owner, and its lease", () => {
    expect(header(detail(), undefined, liveFigures(), lease(), NOW)).toEqual({
      state: { label: '1 table halted', tone: 'red' },
      route: 'PostgreSQL orders@pg-prod-replica → Delta lake/orders-pg',
      where: 'on n2 · away from owner n1 · lease epoch 2, expires in 8 s',
    });
  });

  it('says a lease expired when its end has passed, not that it expires in 0 s', () => {
    const where = (now: number) => header(detail(), undefined, liveFigures(), lease(), now)!.where;
    expect(where(NOW + 8_000)).toBe('on n2 · away from owner n1 · lease epoch 2, expired 0 s ago');
    expect(where(NOW + 23_000)).toBe('on n2 · away from owner n1 · lease epoch 2, expired 15 s ago');
    expect(where(NOW + 7_000)).toContain('expires in 1 s');
  });

  it('says a released lease is released, with no epoch to count down', () => {
    const d = detail({ holder: { ...detail().holder!, released: true } });
    expect(header(d, undefined, null, undefined, NOW)!.where).toBe('lease released');
  });

  it("heads with its list row's alone until its own read, and says when no node holds it", () => {
    const item = { ...facts().pipelines[3], holder: null };
    expect(header(null, item, null, undefined, NOW)).toEqual({
      state: { label: 'running', tone: 'green' },
      route: 'PostgreSQL crm@pg-crm → Delta lake/customers',
      where: 'no node holds its lease',
    });
    expect(header(null, undefined, null, undefined, NOW)).toBeNull();
  });

  it('lists its tables halted first, with figures, cost, and their last schema change', () => {
    const lines = tableLines('orders-pg', liveFigures(), detail(), events(), NOW);
    expect(lines.map((t) => [t.table, t.state.label, t.changes, t.version, t.files, t.schema])).toEqual([
      ['public.refunds', 'halted', 0, 2210, null, "amount changed int → text, which can't be applied in place"],
      ['public.orders', 'running', 1840, 18044, '6 of 820', 'public.orders: column reason added'],
    ]);
    expect(lines[0].href).toBe('/ui/tables/orders-pg/public.refunds');
  });

  it('lists the halts storage keeps while no worker shows its tables', () => {
    const lines = tableLines('orders-pg', null, detail(), [], NOW);
    expect(lines.map((t) => [t.table, t.state.label, t.changes, t.schema])).toEqual([
      ['public.refunds', 'halted', null, "amount changed int → text, which can't be applied in place"],
    ]);
  });

  it("names a MySQL pipeline's tables as its figures do, however storage and events spell them", () => {
    const base = detail();
    const d = detail({
      id: 'billing-mysql',
      source: { type: 'mysql', host: 'mysql-8', port: 3306, db: 'billing' },
      halted_tables: [{ table: 'billing.refunds', code: 'SCHEMA_REFUSED', detail: 'refused', since: base.halted_tables[0].since }],
    });
    // Storage's halts carry `db.table`.
    expect(tableLines('billing-mysql', null, d, [], NOW).map((t) => [t.table, t.href])).toEqual([
      ['refunds', '/ui/tables/billing-mysql/refunds'],
    ]);
    // The figures carry the bare name; a schema event's object carries `db.table`.
    const fig = liveFigures();
    fig.source = { detail: { engine: 'mysql' } };
    const { pipeline: _p, ...invoices } = table('billing-mysql', 'invoices');
    fig.tables = [invoices];
    const schema = event({ kind: 'schema.applied', at: iso(NOW - 60_000), object: 'billing.invoices', text: 'billing.invoices: column note added' });
    const lines = tableLines('billing-mysql', fig, d, [schema], NOW);
    expect(lines.map((t) => [t.table, t.schema])).toEqual([['invoices', 'billing.invoices: column note added']]);
  });

  it("shows PostgreSQL's slot, WAL held against its limit, the confirmed LSN and the publication", () => {
    expect(sourceLines(liveFigures()).map((l) => [l.label, l.value, l.tone])).toEqual([
      ['Slot', 'da_orders active', 'normal'],
      ['WAL held', '210 MB of max_slot_wal_keep_size 3 GB', 'normal'],
      ['Confirmed LSN', '3/A41C2B10', 'normal'],
      ['Publication', 'da_orders · 14 tables', 'normal'],
    ]);
    const f = liveFigures();
    f.source!.detail!.slot = { name: 'da_orders', active: false, confirmed_flush: null, wal_held_bytes: 2 * 1024 ** 3 };
    f.source!.detail!.publication = null;
    expect(sourceLines(f).map((l) => [l.value, l.tone])).toEqual([
      ['da_orders inactive', 'orange'],
      ['2 GB of max_slot_wal_keep_size 3 GB', 'orange'],
      ['—', 'normal'],
      ['none by its name', 'red'],
    ]);
  });

  it("shows MySQL's position, how long it keeps binlogs, and how far behind it reads; nothing without a worker", () => {
    const f = liveFigures();
    f.source = { detail: { engine: 'mysql', head: 'mysql-gtid:3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77', binlog_expire_seconds: 604_800 }, behind_seconds: 3.2 };
    expect(sourceLines(f).map((l) => [l.label, l.value])).toEqual([
      ['Position', '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77'],
      ['Binlogs kept', '7d 0h'],
      ['Behind', '3 s'],
    ]);
    expect(sourceLines({ ...f, source: null })).toEqual([]);
    expect(position('binlog:mysql-bin.000042:1337')).toBe('mysql-bin.000042:1337');
  });

  it('shows the running job with its progress, and the three ended last', () => {
    const s = snapshotPanel(jobs(), NOW);
    expect(s.current).toEqual({
      job: '0192f0c4',
      href: '/ui/snapshots/orders-pg~0192f0c47d5e7d5e8f0a1b2c3d4e5f60',
      what: `public.order_items · started by Oleksii ${new Date(Date.UTC(2026, 10, 25, 13, 10)).toTimeString().slice(0, 5)}`,
      note: 'backfill after the 12:40 outage',
      progress: '61% · 2.6 M of ~4.2 M rows · ETA 7m 0s · 41 k rows/s',
    });
    expect(s.earlier.map((j) => `${j.job} ${j.text}`)).toEqual(['019aa1c2 cancelled by ci-bot 1 d ago', '019aa0a1 ✓ 2 d ago']);
  });

  it("draws the last 6 hours' events it labels on the chart: actions carried out, halts, failovers, jobs", () => {
    const since = NOW / 1000 - 6 * 3600;
    expect(marks(events(), since).map((m) => m.label)).toEqual(['failover', 'public.refunds halted', 'snapshot', 'restarted']);
  });

  it('lists the activity, who did each action and why', () => {
    const lines = activity(events(), NOW);
    expect(lines[3]).toMatchObject({ text: 'started job 0192f0c4 on orders-pg', who: 'Oleksii (operator)', note: 'backfill after the 12:40 outage' });
    expect(lines[0].when).toMatch(/^\d\d:\d\d$/);
  });
});
