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
