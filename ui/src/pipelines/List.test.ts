import { flushSync, mount, tick, unmount } from 'svelte';
import { afterEach, describe, expect, it } from 'vitest';
import { HttpError, type Client } from '../lib/api';
import { Router } from '../lib/router.svelte';
import type { Role, Session } from '../lib/session';
import { facts, NOW } from '../model/fixtures';
import { Model } from '../model/model.svelte';
import { LagHistory } from './lag.svelte';
import List from './List.svelte';

let mounted: ReturnType<typeof mount> | undefined;
afterEach(() => {
  if (mounted !== undefined) unmount(mounted);
  mounted = undefined;
});

/** A client whose actions answer from `fail` (a pipeline's error), and record what was sent. */
function api(fail: Record<string, HttpError> = {}): Client & { sent: unknown[][] } {
  const sent: unknown[][] = [];
  return {
    sent,
    get: async <T,>() => ({ series: [] }) as T,
    send: async <T,>(method: string, path: string, body?: unknown) => {
      sent.push([method, path, body]);
      const id = decodeURIComponent(path.split('/')[3]);
      if (id in fail) throw fail[id];
      return {} as T;
    },
  };
}

const session = (role: Role): Session => ({ name: 'Oleksii', role, auth: 'basic', expires: null, signed_in: null, csrf: 't' });

function render(
  over: { role?: Role; path?: string; client?: Client; offline?: boolean; read?: Partial<Model['read']>; stale?: boolean; last?: number | null } = {},
) {
  history.replaceState(null, '', over.path ?? '/ui/pipelines');
  const router = new Router();
  const client = over.client ?? api();
  const model = new Model(client);
  model.facts = facts();
  Object.assign(model.read, over.read ?? {});
  const target = document.createElement('div');
  document.body.replaceChildren(target);
  mounted = mount(List, {
    target,
    props: {
      model,
      lag: new LagHistory(client, null, () => NOW),
      session: session(over.role ?? 'operator'),
      api: client,
      router,
      now: NOW,
      stale: over.stale ?? false,
      last: over.last === undefined ? NOW : over.last,
      offline: over.offline ?? false,
    },
  });
  flushSync();
  return { t: target, router };
}

const ids = (t: HTMLElement) => [...t.querySelectorAll('tbody a.open')].map((a) => a.textContent);
const settle = async () => {
  for (let i = 0; i < 5; i++) await tick();
  flushSync();
};

function check(t: HTMLElement, id: string) {
  const box = t.querySelector<HTMLInputElement>(`input[aria-label="Select ${id}"]`)!;
  box.click();
  flushSync();
}

function button(t: HTMLElement, text: string): HTMLButtonElement {
  return [...t.querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent?.trim() === text)!;
}

describe('the pipeline list', () => {
  it('lists every pipeline worst first, with its route, node, lag against the target and snapshot', () => {
    const { t } = render();
    expect(ids(t)).toEqual(['orders-pg', 'billing-mysql', 'customers', 'inventory']);
    const first = t.querySelector('tbody tr')!;
    expect(first.textContent).toContain('PG orders@pg-prod-replica → Δ lake/orders-pg');
    expect(first.textContent).toContain('n2 away');
    expect(first.textContent).toContain('21 s / 60 s');
    expect(first.textContent).toContain('1 of 2');
    expect(t.querySelector('.chips [aria-current="true"]')?.textContent).toContain('All 4');
  });

  it('shows the view its address names, and changes the address in place', () => {
    const { t, router } = render({ path: '/ui/pipelines?show=attention&sort=pipeline' });
    expect(ids(t)).toEqual(['billing-mysql', 'inventory', 'orders-pg']);
    const before = history.length;
    [...t.querySelectorAll<HTMLAnchorElement>('.chips a')].find((a) => a.textContent?.includes('Paused'))!.click();
    flushSync();
    expect(router.path).toBe('/ui/pipelines?show=paused&sort=pipeline');
    expect(history.length).toBe(before);
    expect(ids(t)).toEqual(['inventory']);
  });

  it('opens a row from a click anywhere on it, and moves between rows with ↑ and ↓', () => {
    const { t, router } = render();
    const rows = t.querySelectorAll('tbody tr');
    (rows[1].querySelector('td:nth-child(4)') as HTMLElement).click();
    flushSync();
    expect(router.path).toBe('/ui/pipelines/billing-mysql');
    router.go('/ui/pipelines');
    flushSync();
    const links = t.querySelectorAll<HTMLAnchorElement>('tbody a.open');
    links[0].focus();
    links[0].dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true }));
    expect(document.activeElement).toBe(links[1]);
    links[1].dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowUp', bubbles: true }));
    expect(document.activeElement).toBe(links[0]);
  });

  it("offers a viewer nothing to select or act on", () => {
    const { t } = render({ role: 'viewer' });
    expect(t.querySelectorAll('input[type="checkbox"]')).toHaveLength(0);
  });

  it('acts on several, one at a time, naming those the action skips, and stops at the first failure', async () => {
    const client = api({ 'billing-mysql': new HttpError(409, 'no node holds its lease') });
    const { t } = render({ client });
    for (const id of ['orders-pg', 'billing-mysql', 'inventory', 'customers']) check(t, id);
    expect(t.querySelector('.acting')?.textContent).toContain('4 selected');
    button(t, 'Restart…').click();
    flushSync();
    const dialog = t.querySelector('[role="dialog"]')!;
    expect(dialog.querySelector('h3')?.textContent).toBe('Restart 3 pipelines?');
    expect(dialog.textContent).toContain('orders-pg on n2 · billing-mysql on n1 · customers on n1');
    expect(dialog.textContent).toContain("customers's running snapshot 019ab5d0 carries on from its last chunk after the restart.");
    expect(dialog.textContent).toContain("Not asked, restart doesn't apply: inventory (paused).");
    expect(dialog.textContent).toContain('Recorded as: restarted by Oleksii (operator), on each pipeline');
    const area = dialog.querySelector('textarea')!;
    area.value = 'rotate the TLS certificate';
    area.dispatchEvent(new Event('input'));
    button(t, 'Restart 3').click();
    await settle();
    expect(client.sent).toEqual([
      ['POST', '/v1/pipelines/orders-pg/restart', { note: 'rotate the TLS certificate' }],
      ['POST', '/v1/pipelines/billing-mysql/restart', { note: 'rotate the TLS certificate' }],
    ]);
    expect(t.querySelector('[role="dialog"] [role="alert"]')?.textContent).toBe(
      'billing-mysql: no node holds its lease. Asked already: orders-pg. Not asked: customers.',
    );
    // What was asked is no longer selected.
    expect(t.querySelector('.acting')?.textContent).toContain('3 selected');
  });

  it('acts only on the selected pipelines its filters show, and keeps the rest selected', () => {
    const { t, router } = render();
    check(t, 'orders-pg');
    check(t, 'billing-mysql');
    expect(t.querySelector('.acting')?.textContent).toContain('2 selected');
    router.replace('/ui/pipelines?source=mysql');
    flushSync();
    expect(ids(t)).toEqual(['billing-mysql']);
    expect(t.querySelector('.acting')?.textContent).toContain('1 selected');
    button(t, 'Restart…').click();
    flushSync();
    const dialog = t.querySelector('[role="dialog"]')!;
    expect(dialog.querySelector('h3')?.textContent).toBe('Restart billing-mysql?');
    expect(dialog.textContent).not.toContain('orders-pg');
  });

  it('keeps actions from being confirmed while offline', () => {
    const { t } = render({ offline: true });
    check(t, 'orders-pg');
    expect(button(t, 'Flush…').disabled).toBe(true);
  });

  it("says which parts couldn't be read and how old what it shows is", () => {
    const { t } = render({
      read: {
        pipelines: { at: NOW - 3 * 60_000, error: 'HTTP 502' },
        cluster: { at: null, error: 'the request timed out' },
        jobs: { at: NOW - 20 * 60_000, error: 'HTTP 503' },
        tables: { at: NOW - 90_000, error: 'HTTP 500' },
        events: { at: null, error: 'not shown here' },
      },
    });
    expect(t.querySelector('.notice.failing')?.textContent).toBe(
      "Couldn't be read; showing what was last read: the pipelines (HTTP 502), last read 3 min ago; " +
        'the cluster (the request timed out), never read; the snapshot jobs (HTTP 503), last read 20 min ago; ' +
        'the tables (HTTP 500), last read 1 min ago',
    );
  });

  it('says nothing about the parts when all were read, and when no live update came', () => {
    const { t } = render({ stale: true, last: null });
    expect(t.querySelector('.notice.failing')).toBeNull();
    expect(t.querySelector('.notice')?.textContent).toBe('Not current: no live update yet');
  });
});
