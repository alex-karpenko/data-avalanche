import { flushSync, mount, unmount } from 'svelte';
import { afterEach, describe, expect, it, vi } from 'vitest';
import Frame from './Frame.svelte';
import type { Session } from './lib/session';

const NOW = new Date(2026, 10, 25, 14, 21, 19).getTime();

function session(over: Partial<Session> = {}): Session {
  return {
    name: 'Oleksii',
    role: 'operator',
    auth: 'basic',
    expires: null,
    signed_in: null,
    csrf: 'anti-forgery',
    ...over,
  };
}

let mounted: ReturnType<typeof mount> | undefined;

function render(props: Partial<Parameters<typeof Frame>[1]> = {}): HTMLElement {
  const target = document.createElement('div');
  document.body.replaceChildren(target);
  mounted = mount(Frame, {
    target,
    props: {
      session: session(),
      section: 'overview',
      generation: 42,
      state: 'live',
      last: NOW,
      now: NOW,
      onRetry: () => {},
      status: [],
      onStatus: () => {},
      ...props,
    },
  });
  flushSync();
  return target;
}

describe('the frame', () => {
  afterEach(() => {
    if (mounted) unmount(mounted);
  });

  it('names the sections, the one shown marked, and who is signed in', () => {
    const page = render({ section: 'pipelines' });
    const nav = [...page.querySelectorAll('nav a')].map((a) => a.textContent);
    expect(nav).toEqual(['Overview', 'Cluster', 'Pipelines', 'Tables', 'Snapshots', 'Events', 'Config']);
    expect(page.querySelector('[aria-current="page"]')?.getAttribute('href')).toBe('/ui/pipelines');
    expect(page.querySelector('.who')?.textContent).toMatch(/live\s+config gen 42\s+Oleksii\s+operator/);
    expect(page.querySelector('form[action="/ui/sign-out"]')).not.toBeNull();
  });

  it("offers no sign-out without a session, and warns when there's no authentication", () => {
    expect(render({ session: session({ auth: 'jwt', csrf: null }) }).querySelector('form')).toBeNull();
    const open = render({ session: session({ auth: 'none', csrf: null, role: 'admin' }) });
    expect(open.querySelector('[role="alert"]')?.textContent).toContain(
      'This cluster has no authentication: everyone who reaches the console is admin.',
    );
  });

  it("shows the console's status from the user menu, read again as it opens", () => {
    const onStatus = vi.fn();
    const page = render({
      onStatus,
      status: [
        { label: 'Live updates', chip: 'connected', text: 'via n1', tone: 'ok' },
        { label: 'Prometheus', chip: 'failing', text: 'connection refused · last asked 13:40', tone: 'red' },
        { label: 'Event store', chip: null, text: 'not read yet', tone: 'muted' },
      ],
    });
    const menu = page.querySelector<HTMLDetailsElement>('.who details.menu');
    expect(menu?.querySelector('summary')?.textContent).toMatch(/^Oleksii\s+operator$/);
    // Closed, it draws nothing: its rows aren't the page's.
    expect(menu?.querySelector('.status')).toBeNull();
    menu!.open = true;
    menu!.dispatchEvent(new Event('toggle'));
    flushSync();
    expect(menu?.querySelector('h3')?.textContent).toBe('Console status');
    const rows = [...(menu?.querySelectorAll('tr') ?? [])].map((r) => [...r.querySelectorAll('td')].map((d) => d.textContent?.trim()));
    expect(rows).toEqual([
      ['Live updates', 'connected via n1'],
      ['Prometheus', 'failing connection refused · last asked 13:40'],
      ['Event store', 'not read yet'],
    ]);
    expect(menu?.querySelector('.chip.red')?.textContent).toBe('failing');
    expect(menu?.querySelector('td.muted')?.textContent?.trim()).toBe('not read yet');
    menu!.open = false;
    menu!.dispatchEvent(new Event('toggle'));
    flushSync();
    expect(menu?.querySelector('.status')).toBeNull();
    expect(onStatus).toHaveBeenCalledOnce();
  });

  it('says how old the page is once the updates stop', () => {
    const late = render({ state: 'reconnecting', now: NOW + 14_000 });
    expect(late.querySelector('[role="status"]')?.textContent).toBe('reconnecting · last update 14 s ago');
    const onRetry = vi.fn();
    const offline = render({ state: 'offline', now: NOW + 60_000, onRetry });
    const alert = offline.querySelector('.down');
    expect(alert?.textContent).toContain('Offline since 14:21:19 — everything below is as of then.');
    alert?.querySelector('button')?.click();
    expect(onRetry).toHaveBeenCalledOnce();
  });
});
