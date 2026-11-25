import { describe, expect, it } from 'vitest';
import { rows, tiles } from './derive';
import { facts, figures, NOW } from './fixtures';

describe("the Overview's", () => {
  it("tiles are mockup 01's", () => {
    expect(tiles(facts(), NOW).map((t) => [t.key, t.value, t.detail, t.tone])).toEqual([
      ['Pipelines', '4', '3 running · 1 paused · 1 halted table', 'red'],
      ['Worst lag', '4m 12s', 'billing-mysql · target 60 s', 'orange'],
      ['Nodes', '3 / 3', '1 on an older config generation', 'normal'],
      ['Snapshots', '1', 'running · 1 failed today', 'orange'],
      ['WAL held by slots', '3.4 GB', 'largest: da_inventory 3.2 GB', 'orange'],
    ]);
  });

  it('pipelines are listed the worst first, each with its state, node, lag and last commit', () => {
    expect(rows(facts(), NOW).map((r) => [r.id, r.what, r.state.label, r.state.note, r.node, r.lag, r.lastCommit])).toEqual([
      ['orders-pg', 'PostgreSQL → Delta · 2 tables', '1 halted', null, 'n2', 21, NOW - 8_000],
      ['billing-mysql', 'MySQL → Delta · 1 table', 'lagging', null, 'n1', 252, NOW - 180_000],
      ['customers', 'Delta · 1 table', 'snapshot 62%', null, 'n1', null, NOW - 12_000],
      ['inventory', 'PostgreSQL → Delta · 1 table', 'paused', 'by Oleksii, 2 h ago', 'n3', null, NOW - 7_200_000],
    ]);
  });

  it("rows of a node out of reach say it, and what was last known", () => {
    const f = facts();
    f.unreachable = { n1: NOW - 40_000 };
    const billing = rows(f, NOW).find((r) => r.id === 'billing-mysql');
    expect(billing?.state).toEqual({ label: 'unknown', tone: 'grey', note: 'n1 unreachable 40 s · last known: running' });
  });

  it('tiles say what nothing reports yet', () => {
    const f = facts();
    f.figures = {};
    f.lag = {};
    f.config = null;
    const [, lag, nodes, , wal] = tiles(f, NOW);
    expect([lag.value, lag.detail, nodes.detail, wal.value, wal.detail]).toEqual([
      '—',
      'no pipeline reports one · target 60 s',
      '',
      '—',
      'no PostgreSQL slot reported',
    ]);
  });

  it('tiles and rows take a pipeline that is starting: no worker, no source yet', () => {
    const f = facts();
    f.figures.customers = figures({ id: 'customers', node: 'n1', worker: null, source: null });
    expect(tiles(f, NOW).find((t) => t.key === 'WAL held by slots')?.value).toBe('3.4 GB');
    const customers = (f: ReturnType<typeof facts>) => rows(f, NOW).find((r) => r.id === 'customers');
    expect(customers(f)?.state).toEqual(customers(facts())?.state);
  });
});
