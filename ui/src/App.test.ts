import { flushSync, mount, unmount } from 'svelte';
import { afterEach, describe, expect, it, vi } from 'vitest';
import App from './App.svelte';
import { RETRY_MS } from './lib/session';

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
});
