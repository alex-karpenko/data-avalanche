import { flushSync, mount, tick, unmount } from 'svelte';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Client } from '../lib/api';
import type { Role, Session } from '../lib/session';
import { facts, NOW } from '../model/fixtures';
import { CLUSTER_REREAD_MS, Model } from '../model/model.svelte';
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

const sent: unknown[][] = [];
const asked: string[] = [];
const api: Client = {
  get: async <T,>(path: string) => (asked.push(path), (path === '/v1/cluster/status' ? facts().cluster : {}) as T),
  send: async <T,>(method: string, path: string, body?: unknown) => (sent.push([method, path, body]), {} as T),
};

const session = (role: Role): Session => ({ name: 'Oleksii', role, auth: 'basic', expires: null, signed_in: null, csrf: 't' });

async function render(
  over: { missing?: boolean; stale?: boolean; read?: Partial<NodePage['read']>; role?: Role; offline?: boolean; via?: string } = {},
): Promise<HTMLElement> {
  sent.length = 0;
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
  mounted = mount(Node, {
    target,
    props: {
      page,
      model,
      lag,
      session: session(over.role ?? 'admin'),
      api,
      via: over.via ?? 'n1',
      now: NOW,
      stale: over.stale ?? false,
      last: NOW - 34_000,
      offline: over.offline ?? false,
    },
  });
  await vi.dynamicImportSettled();
  await tick();
  flushSync();
  return target;
}

const text = (e: Element | null | undefined) => e?.textContent?.replace(/\s+/g, ' ').trim();
const settle = async () => {
  for (let i = 0; i < 5; i++) await tick();
  flushSync();
};
const paragraphs = (t: HTMLElement) => [...t.querySelectorAll('[role="dialog"] p')].map((p) => text(p));
function button(t: HTMLElement, label: string): HTMLButtonElement {
  return [...t.querySelectorAll<HTMLButtonElement>('button')].find((b) => text(b) === label)!;
}

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

  it('offers an admin Leave and Shut down, and a viewer or operator neither', async () => {
    const buttons = (t: HTMLElement) => [...t.querySelectorAll('.actions button')].map((b) => text(b));
    expect(buttons(await render())).toEqual(['Leave…', 'Shut down…']);
    expect(text((await render()).querySelector('.actions + p'))).toContain('There is no in-place restart');
    expect(buttons(await render({ role: 'operator' }))).toEqual([]);
    expect(buttons(await render({ role: 'viewer' }))).toEqual([]);
  });

  it('leaves through its dialog: what it hands off, the note, and the line it is recorded as', async () => {
    const t = await render();
    button(t, 'Leave…').click();
    flushSync();
    const dialog = () => t.querySelector('[role="dialog"]');
    expect(text(dialog()?.querySelector('h3'))).toBe('n2 leaves the cluster?');
    expect(text(dialog())).toContain('Its pipelines: orders-pg.');
    expect(paragraphs(t)).toContain('Recorded as: n2 left the cluster, asked by Oleksii (admin)');
    const note = dialog()!.querySelector<HTMLTextAreaElement>('textarea')!;
    note.value = 'node pool upgrade';
    note.dispatchEvent(new Event('input'));
    button(t, 'Leave').click();
    await settle();
    expect(sent).toEqual([['POST', '/v1/nodes/n2/leave', { note: 'node pool upgrade' }]]);
    expect(dialog()).toBeNull();
  });

  it('shuts down only once its name is typed', async () => {
    const t = await render();
    button(t, 'Shut down…').click();
    flushSync();
    const confirm = button(t, 'Shut down n2');
    expect(confirm.disabled).toBe(true);
    expect(paragraphs(t)).toContain('Type n2 to confirm.');
    const typed = t.querySelector<HTMLInputElement>('[role="dialog"] input.typed')!;
    typed.value = 'n2';
    typed.dispatchEvent(new Event('input'));
    flushSync();
    expect(confirm.disabled).toBe(false);
    confirm.click();
    await settle();
    expect(sent).toEqual([['POST', '/v1/nodes/n2/shutdown', undefined]]);
  });

  it('reads the cluster again every 5 s while open: a renewal sends no event', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    try {
      await render();
      asked.length = 0;
      await vi.advanceTimersByTimeAsync(CLUSTER_REREAD_MS);
      expect(asked).toEqual(['/v1/cluster/status']);
      unmount(mounted!);
      mounted = undefined;
      await vi.advanceTimersByTimeAsync(3 * CLUSTER_REREAD_MS);
      expect(asked).toEqual(['/v1/cluster/status']);
    } finally {
      vi.useRealTimers();
    }
  });

  it('waits while offline: its actions are disabled', async () => {
    const t = await render({ offline: true });
    expect([...t.querySelectorAll<HTMLButtonElement>('.actions button')].map((b) => b.disabled)).toEqual([true, true]);
  });
});
