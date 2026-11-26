import { flushSync, mount, tick, unmount } from 'svelte';
import { afterEach, describe, expect, it } from 'vitest';
import type { Client } from '../lib/api';
import { facts, iso, NOW } from '../model/fixtures';
import { Model } from '../model/model.svelte';
import Cluster from './Cluster.svelte';
import { Renewals } from './renewals.svelte';

let mounted: ReturnType<typeof mount> | undefined;
afterEach(() => {
  if (mounted !== undefined) unmount(mounted);
  mounted = undefined;
});

const api: Client = {
  get: async <T,>() => ({ series: [] }) as T,
  send: async () => {
    throw new Error('nothing is changed');
  },
};

async function render(over: { stale?: boolean; prepare?: (m: Model, r: Renewals) => void } = {}): Promise<HTMLElement> {
  const model = new Model(api);
  model.facts = facts();
  const renewals = new Renewals(api, () => NOW);
  renewals.max = { n1: 0.038, n2: 0.61 };
  over.prepare?.(model, renewals);
  const target = document.createElement('div');
  document.body.replaceChildren(target);
  mounted = mount(Cluster, { target, props: { model, renewals, now: NOW, stale: over.stale ?? false, last: NOW - 34_000 } });
  await tick();
  flushSync();
  return target;
}

const text = (e: Element | null | undefined) => e?.textContent?.replace(/\s+/g, ' ').trim();
const rows = (t: HTMLElement, section: string) =>
  [...t.querySelectorAll(`section.${section} tbody tr`)].map((r) => [...r.querySelectorAll('td')].map((c) => text(c)));

describe('the Cluster page', () => {
  it("shows mockup 03's page: tiles, members, away from owner, leases", async () => {
    const t = await render();
    expect(text(t.querySelector('h2'))).toBe('Cluster');
    expect([...t.querySelectorAll('.tile .k')].map((k) => text(k))).toEqual([
      'Members',
      'Leases',
      'Config generation',
      'Away from owner',
      'Cluster',
    ]);
    expect(rows(t, 'members')[0]).toEqual(['n1', 'alive', '10.0.1.11:8080', '1.1.0', '42', '2 held · owns 3, 1 away · 1 snapshot', '38 ms', '3d 0h']);
    expect(t.querySelector<HTMLAnchorElement>('section.members tbody a')?.getAttribute('href')).toBe('/ui/cluster/nodes/n1');
    expect(rows(t, 'away')).toEqual([['orders-pg', 'n2', 'n1', '—', 'away for over an hour: no hand-off since', 'once n1 has been in the ring 15 s, 1 hand-off at a time']]);
    expect(rows(t, 'leases').map((r) => r[0])).toEqual(['orders-pg', 'billing-mysql', 'customers', 'inventory']);
    expect(t.querySelector('section.removals')).toBeNull();
  });

  it('shows no away panel while every pipeline is on its owner', async () => {
    const t = await render({ prepare: (m) => (m.facts.cluster!.pipelines[0].ring_owner = 'n2') });
    expect(t.querySelector('section.away')).toBeNull();
  });

  it('lists the pending removals, with what the rule waits for', async () => {
    const t = await render({ prepare: (m) => (m.facts.removals = [{ id: 'payments', generation: 41, removed_at: iso(NOW - 4 * 60_000) }]) });
    expect(rows(t, 'removals')).toEqual([['payments', '41', '4 min ago']]);
    expect(text(t.querySelector('section.removals p'))).toContain('2 min');
  });

  it("says which part couldn't be read and how old it is, a member's renewals among them", async () => {
    const t = await render({
      prepare: (m, r) => {
        m.read.cluster = { at: NOW - 60_000, error: 'HTTP 503' };
        r.read = { n2: { at: null, error: 'the request timed out' } };
      },
    });
    expect(text(t.querySelector('.notice.failing'))).toBe(
      "Couldn't be read; showing what was last read: the cluster (HTTP 503), last read 1 min ago; n2's lease renewals (the request timed out), never read",
    );
  });

  it('dims every panel and says how old they are while the stream is stale', async () => {
    const t = await render({ stale: true });
    expect(text(t.querySelector('.notice'))).toMatch(/^Not current: as of \d\d:\d\d:\d\d · 34 s old$/);
    expect([...t.querySelectorAll('.tiles, section')].every((s) => s.classList.contains('stale'))).toBe(true);
  });
});
