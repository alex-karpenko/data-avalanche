import { flushSync, mount, unmount } from 'svelte';
import { afterEach, describe, expect, it } from 'vitest';
import type { Client } from '../lib/api';
import { clock } from '../lib/format';
import { facts, NOW } from './fixtures';
import { Overview as Model } from './model.svelte';
import Overview from './Overview.svelte';

let mounted: ReturnType<typeof mount> | undefined;

function render(model: Model, stale = false): HTMLElement {
  const target = document.createElement('div');
  document.body.replaceChildren(target);
  mounted = mount(Overview, { target, props: { model, now: NOW, stale, last: NOW - 34_000 } });
  flushSync();
  return target;
}

function model(): Model {
  const m = new Model({} as Client);
  m.facts = facts();
  return m;
}

/** An element's words, its elements' apart. */
function text(e: Element | null | undefined): string | undefined {
  if (e === null || e === undefined) return undefined;
  const walker = document.createTreeWalker(e, NodeFilter.SHOW_TEXT);
  const words: string[] = [];
  for (let n = walker.nextNode(); n !== null; n = walker.nextNode()) {
    const t = n.textContent?.replace(/\s+/g, ' ').trim();
    if (t) words.push(t);
  }
  return words.join(' ');
}

describe('the Overview', () => {
  afterEach(() => {
    if (mounted) unmount(mounted);
  });

  it("shows mockup 01's page: tiles, attention, pipelines, events", () => {
    const page = render(model());
    expect([...page.querySelectorAll('.tile')].map(text)).toEqual([
      'Pipelines 4 3 running · 1 paused · 1 halted table',
      'Worst lag 4m 12s billing-mysql · target 60 s',
      'Nodes 3 / 3 1 on an older config generation',
      'Snapshots 1 running · 1 failed today',
      'WAL held by slots 3.4 GB largest: da_inventory 3.2 GB',
    ]);
    expect(text(page.querySelector('h3'))).toBe('Needs attention · 7');
    const first = page.querySelector('.att li');
    expect(text(first)).toBe(
      "orders-pg › public.refunds halted : amount changed int → text, which can't be applied in place 12 min ago Open table →",
    );
    expect(first?.querySelector('a')?.getAttribute('href')).toBe('/ui/tables/orders-pg/public.refunds');
    expect(text(page.querySelector('.rules'))).toContain('Lag target 60 s, rising over 5 min');
    const rows = [...page.querySelectorAll('tbody tr')];
    expect(rows.map((r) => r.querySelector('a')?.getAttribute('href'))).toEqual([
      '/ui/pipelines/orders-pg',
      '/ui/pipelines/billing-mysql',
      '/ui/pipelines/customers',
      '/ui/pipelines/inventory',
    ]);
    expect(text(rows[3])).toBe('inventory PostgreSQL → Delta · 1 table paused by Oleksii, 2 h ago n3 — 2 h ago');
    expect(rows[3].querySelectorAll('a')[1]?.getAttribute('href')).toBe('/ui/cluster/nodes/n3');
    expect(rows[1].querySelector('polyline')).not.toBeNull();
    expect([...page.querySelectorAll('.ev li')].map(text)[1]).toBe(
      `${clock(NOW - 7_200_000)} pause inventory by Oleksii (operator)`,
    );
  });

  it("says when its numbers aren't current, and which part couldn't be read", () => {
    const m = model();
    m.read.jobs = { at: NOW - 60_000, error: 'the request timed out' };
    const page = render(m, true);
    expect(text(page.querySelector('.stale-note'))).toMatch(/^Not current: as of \d\d:\d\d:\d\d · 34 s old$/);
    expect(page.querySelector('.tiles')?.classList.contains('stale')).toBe(true);
    expect(text(page.querySelector('.failing'))).toBe(
      "Couldn't be read, shown as last read: jobs (the request timed out)",
    );
  });

  it("shows the page when a node doesn't answer the config read", () => {
    const m = model();
    m.facts.config!.nodes[1] = { node: 'n2', generation: 42, file: 'same', restart_pending: null, error: 'n2 answered 504' };
    const page = render(m);
    expect(text(page.querySelector('h3'))).toBe('Needs attention · 6');
    expect(page.querySelectorAll('tbody tr')).toHaveLength(4);
  });

  it('says when nothing needs attention', () => {
    const m = model();
    m.facts.tables = [];
    m.facts.figures = {};
    m.facts.lag = {};
    m.facts.jobs = [];
    m.facts.config = null;
    m.facts.actions = {};
    expect(text(render(m).querySelector('.att + p, h3 + p'))).toBe('Nothing needs attention.');
  });
});
