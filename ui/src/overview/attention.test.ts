import { describe, expect, it } from 'vitest';
import { attention, LAG_TARGET_S } from './attention';
import { event, facts, figures, iso, NOW } from './fixtures';

const MIN = 60_000;
const GIB = 1024 ** 3;
const lines = (f = facts(), now = NOW) => attention(f, now).map((a) => `${a.severity} ${a.what}: ${a.why}`);

describe('needs attention', () => {
  it("lists mockup 01's cluster red, then orange, then yellow, the newest first", () => {
    expect(lines()).toEqual([
      "red orders-pg › public.refunds halted: amount changed int → text, which can't be applied in place",
      `orange billing-mysql lag 4m 12s: and rising (target ${LAG_TARGET_S} s)`,
      'orange Snapshot 5b1e2f40 of billing-mysql failed: the source connection was lost',
      'orange Slot da_inventory holds 3.2 GB of WAL: 80% of max_slot_wal_keep_size · pipeline inventory',
      'yellow inventory paused: by Oleksii',
      'yellow Restart-only change waiting on n2: globals.api.address',
      'yellow n3 runs config generation 41: the cluster is on 42',
    ]);
    const [halt, lag] = attention(facts(), NOW);
    expect([halt.since, halt.href]).toEqual([NOW - 12 * MIN, '/ui/tables/orders-pg/public.refunds']);
    expect([lag.since, lag.href]).toEqual([NOW - 10 * MIN, '/ui/pipelines/billing-mysql']);
  });

  it('says when a pipeline errs, a lease has no holder, a node is down, or one is out of reach', () => {
    const f = facts();
    f.figures['billing-mysql'].actual = 'error';
    f.figures['billing-mysql'].error = 'the slot was invalidated';
    f.cluster!.pipelines[3].holder = null;
    f.unheld = { customers: NOW - 40_000 };
    f.cluster!.members[2].suspect = true;
    f.unreachable = { n2: NOW - 40_000 };
    f.figures['orders-pg'].worker!.degraded = 'a commit failed: 503 from the store';
    f.events.unshift(event({ kind: 'lease.takeover', at: iso(NOW - 5 * MIN), pipeline: 'customers', text: 'customers taken over from n1 by n3' }));
    expect(lines(f).filter((l) => !l.includes('refunds') && !l.startsWith('yellow'))).toEqual([
      'red customers has no node running it: no node holds its lease',
      'red billing-mysql stopped by an error: the slot was invalidated',
      "red n3 is down: the other nodes' probes get no answer",
      "orange n2 doesn't answer the console: its pipelines show as last known",
      'orange customers failed over: customers taken over from n1 by n3',
      `orange billing-mysql lag 4m 12s: and rising (target ${LAG_TARGET_S} s)`,
      'orange Snapshot 5b1e2f40 of billing-mysql failed: the source connection was lost',
      'orange Slot da_inventory holds 3.2 GB of WAL: 80% of max_slot_wal_keep_size · pipeline inventory',
      "orange orders-pg can't commit: a commit failed: 503 from the store",
    ]);
  });

  it('keeps to its thresholds', () => {
    const quiet = (change: (f: ReturnType<typeof facts>) => void) => {
      const f = facts();
      change(f);
      return lines(f);
    };
    // The target itself isn't over it; over it but falling isn't rising.
    expect(quiet((f) => (f.figures['billing-mysql'].worker!.lag_seconds = 60)).join()).not.toContain('lag');
    expect(
      quiet((f) => {
        f.lag['billing-mysql'] = f.lag['billing-mysql'].map(([t], i) => [t, 300 - i * 10]);
        f.figures['billing-mysql'].worker!.lag_seconds = 180;
      }).join(),
    ).not.toContain('lag');
    // Half of max_slot_wal_keep_size isn't past it; without a limit, 16 GB is the line.
    const slot = (held: number, limit: number | null) => (f: ReturnType<typeof facts>) => {
      f.figures.inventory.source = {
        detail: { engine: 'postgres', slot: { name: 'da_inventory', wal_held_bytes: held }, max_slot_wal_keep_size: limit },
      };
    };
    expect(quiet(slot(2 * GIB, 4 * GIB)).join()).not.toContain('Slot');
    expect(quiet(slot(16 * GIB, null)).join()).not.toContain('Slot');
    expect(quiet(slot(16 * GIB + 1, null)).join()).toContain('Slot da_inventory holds 16 GB of WAL: with no max_slot_wal_keep_size');
    // Paused under an hour ago, or by no person; a job failed over a day ago.
    expect(quiet((f) => (f.actions.inventory.at = iso(NOW - 59 * MIN))).join()).not.toContain('inventory paused');
    expect(quiet((f) => delete f.actions.inventory.actor).join()).not.toContain('inventory paused');
    expect(quiet((f) => (f.jobs[1].finished_at = iso(NOW - 25 * 60 * MIN))).join()).not.toContain('Snapshot');
    // A lease without a holder for under 30 s is a hand-over; seen only now, it's one too.
    const unheld = (since: number | null) => (f: ReturnType<typeof facts>) => {
      f.cluster!.pipelines[3].holder = null;
      if (since !== null) f.unheld = { customers: since };
    };
    expect(quiet(unheld(NOW - 29_000)).join()).not.toContain('no node running it');
    expect(quiet(unheld(null)).join()).not.toContain('no node running it');
    expect(quiet(unheld(NOW - 30_000)).join()).toContain('customers has no node running it');
    // A lease taken over two hours ago is history.
    expect(
      quiet((f) => f.events.unshift(event({ kind: 'lease.lost', at: iso(NOW - 61 * MIN), pipeline: 'customers' }))).join(),
    ).not.toContain('lease');
  });

  it('takes a pipeline that is starting: no worker, no source yet', () => {
    const f = facts();
    f.figures.customers = figures({ id: 'customers', node: 'n1', worker: null, source: null });
    expect(lines(f)).toEqual(lines());
  });

  it('is empty when nothing is wrong', () => {
    const f = facts();
    f.tables = [];
    f.figures = { x: figures({ id: 'x', node: 'n1' }) };
    f.lag = {};
    f.jobs = [];
    f.config!.nodes = [];
    f.actions = {};
    expect(attention(f, NOW)).toEqual([]);
  });
});
