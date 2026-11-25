import { flushSync, mount, tick, unmount } from 'svelte';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Client } from '../lib/api';
import type { Role, Session } from '../lib/session';
import { facts, NOW } from '../model/fixtures';
import { Model } from '../model/model.svelte';
import { detail, events, jobs, liveFigures } from './fixtures';
import { PipelinePage } from './page.svelte';
import Pipeline from './Pipeline.svelte';

// The chart's uPlot: a second a pixel, drawing its hooks once.
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

function client(): Client & { sent: unknown[][] } {
  const sent: unknown[][] = [];
  return {
    sent,
    get: async <T,>() => ({}) as T,
    send: async <T,>(method: string, path: string, body?: unknown) => {
      sent.push([method, path, body]);
      return {} as T;
    },
  };
}

const session = (role: Role): Session => ({ name: 'Oleksii', role, auth: 'basic', expires: null, signed_in: null, csrf: 't' });

async function render(
  over: { role?: Role; offline?: boolean; missing?: boolean; stale?: boolean; last?: number | null; read?: Partial<PipelinePage['read']>; api?: Client } = {},
) {
  const api = over.api ?? client();
  const page = new PipelinePage(api, 'orders-pg', liveFigures(), () => NOW);
  page.detail = detail();
  page.jobs = jobs();
  page.events = events();
  page.missing = over.missing ?? false;
  Object.assign(page.read, over.read ?? {});
  const model = new Model(api);
  model.facts = facts();
  const target = document.createElement('div');
  document.body.replaceChildren(target);
  mounted = mount(Pipeline, {
    target,
    props: { page, model, session: session(over.role ?? 'operator'), api, now: NOW, stale: over.stale ?? false, last: over.last === undefined ? NOW : over.last, offline: over.offline ?? false },
  });
  await vi.dynamicImportSettled();
  await tick();
  flushSync();
  return target;
}

const settle = async () => {
  for (let i = 0; i < 5; i++) await tick();
  flushSync();
};

function button(t: HTMLElement, text: string): HTMLButtonElement {
  return [...t.querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent?.trim() === text)!;
}

describe("a pipeline's page", () => {
  it("reads the cluster's leases again every 5 s while shown, and stops when it goes", async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    try {
      const asked: string[] = [];
      const api: Client = { ...client(), get: async <T,>(path: string) => (asked.push(path), {} as T) };
      await render({ api });
      expect(asked).toEqual([]);
      vi.advanceTimersByTime(5_000);
      expect(asked).toEqual(['/v1/cluster/status']);
      vi.advanceTimersByTime(10_000);
      expect(asked).toHaveLength(3);
      unmount(mounted!);
      mounted = undefined;
      vi.advanceTimersByTime(10_000);
      expect(asked).toHaveLength(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it('shows its header, tables, source, snapshot and activity', async () => {
    const t = await render();
    expect(t.querySelector('h2')?.textContent).toContain('orders-pg');
    expect(t.querySelector('h2 .chip')?.textContent).toBe('1 table halted');
    expect(t.querySelector('.where')?.textContent).toBe(
      'PostgreSQL orders@pg-prod-replica → Delta lake/orders-pg · on n2 · away from owner n1 · lease epoch 2, expires in 8 s',
    );
    expect([...t.querySelectorAll('tbody tr td:first-child a')].map((a) => a.textContent)).toEqual(['public.refunds', 'public.orders']);
    expect(t.textContent).toContain('da_orders · 14 tables');
    expect(t.textContent).toContain('61% · 2.6 M of ~4.2 M rows · ETA 7m 0s · 41 k rows/s');
    expect(t.querySelectorAll('.activity li')).toHaveLength(7);
  });

  it('offers an operator the actions its state takes, and a viewer none', async () => {
    let t = await render();
    expect([...t.querySelectorAll('.actions button')].map((b) => b.textContent?.trim())).toEqual([
      'Flush',
      'Pause',
      'Restart',
      'Stop',
      'Snapshot…',
    ]);
    unmount(mounted!);
    t = await render({ role: 'viewer' });
    expect(t.querySelector('.actions')).toBeNull();
  });

  it('says what a restart does to it, and sends the note', async () => {
    const api = client();
    const t = await render({ api });
    button(t, 'Restart').click();
    flushSync();
    const dialog = t.querySelector('[role="dialog"]')!;
    expect(dialog.textContent).toContain('orders-pg stops on n2,');
    expect(dialog.textContent).toContain('Its running snapshot 0192f0c4 carries on from its last chunk.');
    expect(dialog.textContent).toContain('Recorded as: restarted by Oleksii (operator)');
    const area = dialog.querySelector('textarea')!;
    area.value = 'apply sink.prefix';
    area.dispatchEvent(new Event('input'));
    dialog.querySelector<HTMLButtonElement>('.go')!.click();
    await settle();
    expect(api.sent).toEqual([['POST', '/v1/pipelines/orders-pg/restart', { note: 'apply sink.prefix' }]]);
    expect(t.querySelector('[role="dialog"]')).toBeNull();
  });

  it('starts a snapshot job of the tables chosen, truncating them when asked', async () => {
    const api = client();
    const t = await render({ api });
    button(t, 'Snapshot…').click();
    flushSync();
    const dialog = t.querySelector('[role="dialog"]')!;
    expect(dialog.querySelector<HTMLButtonElement>('.go')!.disabled).toBe(true);
    expect(dialog.textContent).toContain('Choose the tables to copy.');
    const boxes = dialog.querySelectorAll<HTMLInputElement>('input[type="checkbox"]');
    boxes[0].click();
    boxes[boxes.length - 1].click();
    flushSync();
    expect(dialog.textContent).toContain('Truncate empties each table first');
    dialog.querySelector<HTMLButtonElement>('.go')!.click();
    await settle();
    expect(api.sent).toEqual([['POST', '/v1/pipelines/orders-pg/snapshots', { tables: ['public.refunds'], truncate: true }]]);
  });

  it('opens the snapshot dialog with nothing chosen, whatever a cancelled one had', async () => {
    const t = await render();
    button(t, 'Snapshot…').click();
    flushSync();
    let dialog = t.querySelector('[role="dialog"]')!;
    dialog.querySelectorAll<HTMLInputElement>('input[type="checkbox"]').forEach((b) => b.click());
    flushSync();
    expect(dialog.textContent).toContain('Truncate empties each table first');
    button(t, 'Cancel').click();
    flushSync();
    button(t, 'Snapshot…').click();
    flushSync();
    dialog = t.querySelector('[role="dialog"]')!;
    expect([...dialog.querySelectorAll<HTMLInputElement>('input[type="checkbox"]')].map((b) => b.checked)).toEqual([false, false, false]);
    expect(dialog.textContent).toContain('Choose the tables to copy.');
  });

  it("highlights an event on the chart while its activity line is hovered", async () => {
    const t = await render();
    const takeover = t.querySelectorAll('.activity li')[0];
    takeover.dispatchEvent(new MouseEvent('mouseenter'));
    flushSync();
    expect(t.querySelector('.label.on')?.textContent).toBe('failover');
    expect(takeover.classList.contains('on')).toBe(true);
  });

  it('keeps its actions from being taken while offline', async () => {
    const t = await render({ offline: true });
    expect(button(t, 'Restart').disabled).toBe(true);
  });

  it("says when its source's detail isn't read yet, apart from when no worker runs it", async () => {
    const api = client();
    const page = new PipelinePage(api, 'orders-pg', { ...liveFigures(), source: { detail: null, read_at: null } }, () => NOW);
    const model = new Model(api);
    model.facts = facts();
    const target = document.createElement('div');
    document.body.replaceChildren(target);
    mounted = mount(Pipeline, {
      target,
      props: { page, model, session: session('operator'), api, now: NOW, stale: false, last: NOW, offline: false },
    });
    flushSync();
    expect(target.querySelector('aside')?.textContent).toContain("Not read yet: the node that runs it reads the source's detail at each heartbeat");
    page.figures = { ...liveFigures(), source: null, worker: null };
    flushSync();
    expect(target.querySelector('aside')?.textContent).toContain('No worker runs it now');
  });

  it('says when the generation has no such pipeline', async () => {
    const t = await render({ missing: true });
    expect(t.querySelector('[role="alert"]')?.textContent).toBe('There is no pipeline orders-pg in the generation this node runs.');
  });

  it('says how old a part that failed to read is: its last read, which the page keeps', async () => {
    const t = await render({ read: { jobs: { at: NOW - 20 * 60_000, error: 'HTTP 503' } } });
    expect(t.querySelector('.notice.failing')?.textContent).toBe(
      "Couldn't be read; showing what was last read: its snapshot jobs (HTTP 503), last read 20 min ago",
    );
  });

  it('says a part that failed and was never read is never read', async () => {
    const t = await render({ read: { events: { at: null, error: 'HTTP 503' } } });
    expect(t.querySelector('.notice.failing')?.textContent).toContain('its activity (HTTP 503), never read');
  });

  it('says when no live update came at all, instead of an age since the epoch', async () => {
    const t = await render({ stale: true, last: null });
    expect(t.querySelector('.notice')?.textContent).toBe('Not current: no live update yet');
  });

  it('dims every panel while the stream is stale, the snapshot and the activity too', async () => {
    const t = await render({ stale: true });
    const panel = (title: string) => [...t.querySelectorAll('section')].find((s) => s.querySelector('h3')?.textContent?.startsWith(title));
    for (const title of ['Tables', 'Source', 'Snapshot', 'Activity']) {
      expect(panel(title)?.classList.contains('stale'), title).toBe(true);
    }
  });

  it("shows how old the source's read is, under its lines", async () => {
    const t = await render();
    expect(t.querySelector('aside')?.textContent).toContain('read 4 s ago');
  });
});
