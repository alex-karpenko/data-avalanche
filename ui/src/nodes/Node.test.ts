import { flushSync, mount, tick, unmount } from 'svelte';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Client } from '../lib/api';
import { facts, NOW } from '../model/fixtures';
import { Model } from '../model/model.svelte';
import { LagHistory } from '../pipelines/lag.svelte';
import { nodeDetail, nodeEvents, nodeSeries } from './fixtures';
import Node from './Node.svelte';
import { NodePage } from './page.svelte';

vi.mock('uplot', () => ({
  default: class {
    bbox = { left: 0, top: 0, width: 600, height: 180 };
    ctx = { save() {}, restore() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() {}, setLineDash() {}, lineWidth: 0, strokeStyle: '' };
    cursor = { idx: null, left: -10 };
    #opts: { hooks: { draw: ((u: unknown) => void)[] } };
    constructor(opts: never) {
      this.#opts = opts;
      this.redraw();
    }
    valToPos(v: number) {
      return v % 600;
    }
    setData() {}
    setScale() {}
    setSize() {}
    redraw() {
      for (const h of this.#opts.hooks.draw) h(this);
    }
    destroy() {}
  },
}));

let mounted: ReturnType<typeof mount> | undefined;
afterEach(() => {
  if (mounted !== undefined) unmount(mounted);
  mounted = undefined;
});

const api: Client = {
  get: async <T,>() => ({}) as T,
  send: async () => {
    throw new Error('nothing is changed');
  },
};

async function render(over: { missing?: boolean; stale?: boolean; read?: Partial<NodePage['read']> } = {}): Promise<HTMLElement> {
  const page = new NodePage(api, 'n2', () => NOW);
  page.detail = nodeDetail();
  page.series = nodeSeries();
  page.events = nodeEvents();
  page.missing = over.missing ?? false;
  Object.assign(page.read, over.read ?? {});
  const model = new Model(api);
  model.facts = facts();
  const lag = new LagHistory(api);
  const target = document.createElement('div');
  document.body.replaceChildren(target);
  mounted = mount(Node, { target, props: { page, model, lag, now: NOW, stale: over.stale ?? false, last: NOW - 34_000 } });
  await vi.dynamicImportSettled();
  await tick();
  flushSync();
  return target;
}

const text = (e: Element | null | undefined) => e?.textContent?.replace(/\s+/g, ' ').trim();

describe("a node's page", () => {
  it("shows mockup 04's page: its header, its pipelines, its charts, its activity and its gossip", async () => {
    const t = await render();
    expect(text(t.querySelector('h2'))).toBe('n2 alive');
    expect(text(t.querySelector('.where'))).toBe('10.0.1.12:8080 · gossip 10.0.1.12:7946 · 1.1.0 · config gen 42 · started 3 d ago');
    const row = [...t.querySelectorAll('section.pipelines tbody tr td')].map((c) => text(c));
    expect([row[0], row[1], row[3]]).toEqual(['orders-pg', '1 halted', 'epoch 2 · away from owner n1']);
    expect([...t.querySelectorAll('figure.chart figcaption span:first-child')].map((c) => text(c))).toEqual([
      'Lease renewal, 6 h',
      'Memory, 6 h',
      'CPU, 6 h',
    ]);
    expect(text(t.querySelector('section.charts'))).toContain('1.3 GB of 2 GB limit');
    expect([...t.querySelectorAll('.activity li')].map((li) => text(li))).toHaveLength(3);
    expect([...t.querySelectorAll('section.gossip dt')].map((d) => text(d))).toEqual(['Sees', 'Last probe', 'Suspicions, 24 h']);
  });

  it('says when no member has its name', async () => {
    const t = await render({ missing: true });
    expect(text(t.querySelector('[role="alert"]'))).toBe("There is no member n2 in this node's view of the cluster.");
  });

  it("says which part couldn't be read and how old it is", async () => {
    const t = await render({ read: { history: { at: null, error: 'HTTP 503' } } });
    expect(text(t.querySelector('.notice.failing'))).toBe("Couldn't be read; showing what was last read: its charts (HTTP 503), never read");
  });

  it('dims every panel and says how old they are while the stream is stale', async () => {
    const t = await render({ stale: true });
    expect(text(t.querySelector('.notice'))).toMatch(/^Not current: as of \d\d:\d\d:\d\d · 34 s old$/);
    expect([...t.querySelectorAll('section')].every((s) => s.classList.contains('stale'))).toBe(true);
  });
});
