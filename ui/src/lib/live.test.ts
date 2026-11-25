import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Live, OFFLINE_AFTER_MS, STALE_AFTER_MS, type SourceLike } from './live.svelte';

/** An `EventSource` the test drives. */
class FakeSource implements SourceLike {
  readyState = 0;
  onerror: ((e: Event) => void) | null = null;
  closed = false;
  #fns = new Map<string, (e: MessageEvent) => void>();
  constructor(readonly url: string) {}
  addEventListener(kind: string, fn: (e: MessageEvent) => void) {
    this.#fns.set(kind, fn);
  }
  close() {
    this.closed = true;
    this.readyState = 2;
  }
  send(kind: string, data: unknown) {
    this.readyState = 1;
    this.#fns.get(kind)?.(new MessageEvent(kind, { data: JSON.stringify(data) }));
  }
  fail() {
    this.readyState = 2;
    this.onerror?.(new Event('error'));
  }
}

const NOW = Date.UTC(2026, 10, 25, 14, 0, 0);

describe('the live stream', () => {
  let sources: FakeSource[];
  const open = (url: string) => {
    const s = new FakeSource(url);
    sources.push(s);
    return s;
  };

  beforeEach(() => {
    sources = [];
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  it("hands each message to its kind's listeners, and is live while they come", () => {
    const live = new Live({ open, check: vi.fn() });
    const states = vi.fn();
    live.on('pipeline', states);
    live.start();
    expect(sources[0].url).toBe('/v1/stream');
    expect(live.state).toBe('connecting');
    sources[0].send('hello', { node: 'n1', follows: ['n2'] });
    sources[0].send('pipeline', { pipeline: 'orders', actual: 'paused', node: 'n2' });
    expect(states).toHaveBeenCalledWith({ pipeline: 'orders', actual: 'paused', node: 'n2' });
    expect(live.state).toBe('live');
    expect(live.last).toBe(NOW);
  });

  it('turns stale, then offline, as the messages stop, and reopens a silent stream', async () => {
    const live = new Live({ open, check: vi.fn().mockResolvedValue('ok') });
    live.start();
    sources[0].send('hello', { node: 'n1' });
    vi.setSystemTime(NOW + STALE_AFTER_MS - 1);
    live.tick(Date.now());
    expect([live.state, live.stale(Date.now())]).toEqual(['live', false]);
    vi.setSystemTime(NOW + STALE_AFTER_MS);
    live.tick(Date.now());
    expect([live.state, live.stale(Date.now())]).toEqual(['reconnecting', true]);
    vi.setSystemTime(NOW + OFFLINE_AFTER_MS);
    live.tick(Date.now());
    expect(live.state).toBe('offline');
    expect(sources[0].closed).toBe(true);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sources).toHaveLength(2);
  });

  it('reads the state again after it restarts or misses messages: no replay', () => {
    const live = new Live({ open, check: vi.fn() });
    const resync = vi.fn();
    live.onResync(resync);
    live.start();
    sources[0].send('hello', { node: 'n1' });
    expect(resync).not.toHaveBeenCalled();
    sources[0].send('reset', { node: 'n1', reason: 'lagged', missed: 12 });
    expect(resync).toHaveBeenCalledTimes(1);
    // The browser reconnected by itself: a new hello.
    sources[0].send('hello', { node: 'n2' });
    expect(resync).toHaveBeenCalledTimes(2);
  });

  it("checks the credentials once it closes, and opens again only if they hold", async () => {
    const check = vi.fn().mockResolvedValueOnce('ok').mockResolvedValueOnce('ended');
    const live = new Live({ open, check });
    live.start();
    sources[0].send('hello', { node: 'n1' });
    sources[0].send('reset', { node: 'n1', reason: 'credentials' });
    expect(sources[0].closed).toBe(true);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(check).toHaveBeenCalledOnce();
    expect(sources).toHaveLength(2);
    // Refused now (a 401): the page was left to the server. No message came since the last
    // try, so the next waits longer.
    sources[1].fail();
    await vi.advanceTimersByTimeAsync(2_000);
    expect(check).toHaveBeenCalledTimes(2);
    expect(sources).toHaveLength(2);
  });

  it('waits 1, 2, 5, then 10 s between tries, and retries at once when asked', async () => {
    const live = new Live({ open, check: vi.fn().mockResolvedValue('later') });
    live.start();
    for (const wait of [1_000, 2_000, 5_000, 10_000, 10_000]) {
      const n = sources.length;
      sources[n - 1].fail();
      await vi.advanceTimersByTimeAsync(wait - 1);
      expect(sources).toHaveLength(n);
      await vi.advanceTimersByTimeAsync(1);
      expect(sources).toHaveLength(n + 1);
    }
    live.retry();
    expect(sources).toHaveLength(7);
  });
});
