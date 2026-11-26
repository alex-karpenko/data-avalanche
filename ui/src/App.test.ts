import { flushSync, mount, tick, unmount } from 'svelte';
import { afterEach, describe, expect, it, vi } from 'vitest';
import App from './App.svelte';
import { RETRY_MS, type Session } from './lib/session';
import { facts } from './model/fixtures';
import { nodeDetail } from './nodes/fixtures';
import { detail, liveFigures } from './pipelines/fixtures';

// The chart's uPlot, as in the pipeline page's tests.
vi.mock('uplot', () => ({
  default: class {
    bbox = { left: 0, top: 0, width: 600, height: 180 };
    ctx = { save() {}, restore() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() {}, setLineDash() {}, lineWidth: 0, strokeStyle: '' };
    cursor = { idx: null, left: -10 };
    valToPos(v: number) {
      return v % 600;
    }
    setData() {}
    setScale() {}
    setSize() {}
    redraw() {}
    destroy() {}
  },
}));

let mounted: ReturnType<typeof mount> | undefined;

describe('the app', () => {
  afterEach(() => {
    if (mounted) unmount(mounted);
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("says when it can't read the session, and reads it again", async () => {
    vi.useFakeTimers();
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response('{}', { status: 502 }))
      .mockRejectedValueOnce(new TypeError('Failed to fetch'));
    vi.stubGlobal('fetch', fetch);
    const target = document.createElement('div');
    document.body.replaceChildren(target);
    mounted = mount(App, { target });
    await vi.advanceTimersByTimeAsync(0);
    flushSync();
    expect(target.querySelector('.unread')?.textContent?.replace(/\s+/g, ' ').trim()).toBe(
      "The console couldn't read your session: the server answered 502. Trying again in 5 s. Reload",
    );
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    flushSync();
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(target.querySelector('.unread')?.textContent).toContain('Failed to fetch');
  });

  /** The console as the server answers it, for pipelines `a` and `b`; `asked` lists the reads. */
  function serve(session: () => Session, cluster: unknown = { members: [], pipelines: [] }) {
    const asked: string[] = [];
    const json = (body: unknown) => new Response(JSON.stringify(body), { status: 200 });
    const fetch = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      asked.push(path);
      if (path === '/ui/session') return json(session());
      if (path === '/ui/session/refresh') return json(session());
      if (path === '/v1/pipelines') return json({ pipelines: facts().pipelines });
      if (path === '/v1/cluster/status') return json(cluster);
      if (path === '/v1/cluster/removals') return json({ removals: [] });
      if (path === '/v1/snapshots') return json({ jobs: [] });
      if (path === '/v1/config/nodes') return json(facts().config);
      if (path === '/v1/nodes/n2') return json(nodeDetail());
      if (path === '/v1/tables') return json({ tables: [], unreachable: [] });
      const one = /^\/v1\/pipelines\/([^/]+)(\/[a-z]+)?$/.exec(path);
      if (one !== null) {
        const id = one[1];
        if (one[2] === '/live') return json({ ...liveFigures(), id });
        if (one[2] === '/snapshots') return json({ jobs: [] });
        return json({ ...detail(), id, name: id });
      }
      if (path.startsWith('/v1/events')) return json({ events: [] });
      if (path.startsWith('/v1/history')) return json({ series: [] });
      return json({});
    });
    vi.stubGlobal('fetch', fetch);
    vi.stubGlobal(
      'EventSource',
      class {
        readyState = 0;
        onerror = null;
        addEventListener() {}
        close() {}
      },
    );
    vi.stubGlobal('scrollTo', () => {});
    return asked;
  }

  const settle = async () => {
    for (let i = 0; i < 6; i++) {
      await new Promise((r) => setTimeout(r, 0));
      await tick();
    }
    flushSync();
  };

  const operator = (): Session => ({
    name: 'Oleksii',
    role: 'operator',
    auth: 'basic',
    expires: null,
    signed_in: null,
    csrf: 't',
  });

  async function open(path: string): Promise<HTMLElement> {
    history.pushState(null, '', path);
    const target = document.createElement('div');
    document.body.replaceChildren(target);
    mounted = mount(App, { target });
    await settle();
    return target;
  }

  const button = (t: HTMLElement, text: string) =>
    [...t.querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent?.trim() === text)!;

  it("shows the next pipeline's page without the last one's dialog, or its choices", async () => {
    serve(operator);
    const t = await open('/ui/pipelines/a');
    expect(t.querySelector('h2')?.textContent).toContain('a');
    button(t, 'Snapshot…').click();
    flushSync();
    t.querySelector<HTMLInputElement>('[role="dialog"] input[type="checkbox"]')!.click();
    flushSync();
    expect(t.querySelector('[role="dialog"]')?.textContent).toContain('Copy 1 table');

    history.pushState(null, '', '/ui/pipelines/b');
    window.dispatchEvent(new PopStateEvent('popstate'));
    flushSync();
    // Not a's page for a tick under b's address.
    expect(t.querySelector('h2')?.textContent ?? '').not.toMatch(/\ba\b/);
    await settle();
    expect(t.querySelector('h2')?.textContent).toContain('b');
    expect(t.querySelector('[role="dialog"]')).toBeNull();
    button(t, 'Snapshot…').click();
    flushSync();
    expect(t.querySelector('[role="dialog"]')?.textContent).toContain('Choose the tables to copy.');
  });

  it("doesn't build the pipeline's page again when the session's token is refreshed", async () => {
    let current: Session = { ...operator(), auth: 'oauth2', expires: new Date(Date.now() + 60_400).toISOString() };
    const asked = serve(() => current);
    const t = await open('/ui/pipelines/a');
    const reads = () => asked.filter((p) => p === '/v1/pipelines/a').length;
    expect(reads()).toBe(1);
    const h2 = t.querySelector('h2');
    current = { ...current, csrf: 'next', expires: new Date(Date.now() + 3_600_000).toISOString() };
    await new Promise((r) => setTimeout(r, 700));
    await settle();
    expect(asked).toContain('/ui/session/refresh');
    expect(reads()).toBe(1);
    expect(t.querySelector('h2')).toBe(h2);
  });

  it("reads each member's lease renewals once as the Cluster page opens, not again at each answer", async () => {
    const asked = serve(operator, { members: facts().cluster!.members, pipelines: [] });
    await open('/ui/cluster');
    await settle();
    const renewals = asked.filter((p) => p.includes('series=lease_renew_seconds')).sort();
    expect(renewals).toEqual(['n1', 'n2', 'n3'].map((n) => `/v1/history?node=${n}&series=lease_renew_seconds`));
  });

  it("reads a node's detail, charts and activity once as its page opens", async () => {
    const asked = serve(operator, { members: facts().cluster!.members, pipelines: [] });
    const t = await open('/ui/cluster/nodes/n2');
    expect(t.querySelector('h2')?.textContent).toContain('n2');
    expect(asked.filter((p) => p === '/v1/nodes/n2')).toHaveLength(1);
    expect(asked.filter((p) => p.startsWith('/v1/history?node=n2&series=lease_renew_seconds,'))).toHaveLength(1);
    expect(asked.filter((p) => p.startsWith('/v1/events?node=n2'))).toHaveLength(1);
  });
});
