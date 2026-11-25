import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  awaitSession,
  checkSession,
  keepFresh,
  readSession,
  REFRESH_AHEAD_MS,
  RETRY_MS,
  type Session,
} from './session';

const NOW = Date.UTC(2026, 10, 25, 14, 0, 0);

function session(over: Partial<Session> = {}): Session {
  return {
    name: 'ops',
    role: 'operator',
    auth: 'oauth2',
    expires: new Date(NOW + 10 * 60_000).toISOString(),
    signed_in: new Date(NOW).toISOString(),
    csrf: 'anti-forgery',
    ...over,
  };
}

function answer(status: number, body: unknown = {}, headers: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), { status, headers });
}

describe('the session', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  it('is read from the server, and a 401 leaves the page to it', async () => {
    const ended = vi.fn();
    const ok = vi.fn().mockResolvedValue(answer(200, session()));
    expect((await readSession({ fetch: ok, ended }))?.name).toBe('ops');
    const gone = vi.fn().mockResolvedValue(answer(401, { error: 'expired' }));
    expect(await readSession({ fetch: gone, ended })).toBeNull();
    expect(ended).toHaveBeenCalledOnce();
  });

  it('leaves the page to the server on a 403 too: a role taken away', async () => {
    const ended = vi.fn();
    const forbidden = vi.fn().mockResolvedValue(answer(403, { error: 'forbidden' }));
    expect(await readSession({ fetch: forbidden, ended })).toBeNull();
    expect(ended).toHaveBeenCalledOnce();
  });

  it('is read again after RETRY_MS while it fails, saying why each time', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(answer(502, { error: 'bad gateway' }))
      .mockRejectedValueOnce(new TypeError('Failed to fetch'))
      .mockResolvedValueOnce(answer(200, session()));
    const read = vi.fn();
    const failed = vi.fn();
    awaitSession({ fetch, ended: vi.fn() }, read, failed);
    await vi.advanceTimersByTimeAsync(0);
    expect(failed).toHaveBeenLastCalledWith('the server answered 502');
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(failed).toHaveBeenLastCalledWith('Failed to fetch');
    expect(read).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(read).toHaveBeenCalledWith(session());
    expect(fetch).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(10 * RETRY_MS);
    expect(fetch).toHaveBeenCalledTimes(3);
  });

  it('is read no more once stopped', async () => {
    const fetch = vi.fn().mockResolvedValue(answer(500));
    const stop = awaitSession({ fetch, ended: vi.fn() }, vi.fn(), vi.fn());
    await vi.advanceTimersByTimeAsync(0);
    stop();
    await vi.advanceTimersByTimeAsync(10 * RETRY_MS);
    expect(fetch).toHaveBeenCalledOnce();
  });

  it("is checked for the live stream: a 401 or a 403 ends the page, else it's ok or later", async () => {
    for (const [status, said, ends] of [
      [200, 'ok', 0],
      [401, 'ended', 1],
      [403, 'ended', 1],
      [502, 'later', 0],
    ] as const) {
      const ended = vi.fn();
      const fetch = vi.fn().mockResolvedValue(answer(status, session()));
      expect(await checkSession({ fetch, ended })).toBe(said);
      expect(ended).toHaveBeenCalledTimes(ends);
    }
  });

  it("refreshes an oauth2 token a minute before it expires, with the anti-forgery token", async () => {
    const renewed = session({ expires: new Date(NOW + 20 * 60_000).toISOString() });
    const fetch = vi.fn().mockImplementation(async () => answer(200, renewed));
    const changed = vi.fn();
    keepFresh(session(), { fetch, ended: vi.fn() }, changed);
    await vi.advanceTimersByTimeAsync(10 * 60_000 - REFRESH_AHEAD_MS - 1);
    expect(fetch).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(fetch).toHaveBeenCalledWith('/ui/session/refresh', {
      method: 'POST',
      headers: { Accept: 'application/json', 'X-CSRF-Token': 'anti-forgery' },
    });
    expect(changed).toHaveBeenCalledWith(renewed);
    // The next one, from the new expiry.
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it('asks again after Retry-After when the provider is down, and never signs out for it', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(answer(503, { error: 'down' }, { 'Retry-After': '5' }))
      .mockResolvedValueOnce(answer(200, session({ expires: null })));
    const ended = vi.fn();
    keepFresh(session({ expires: new Date(NOW + 30_000).toISOString() }), { fetch, ended }, vi.fn());
    await vi.advanceTimersByTimeAsync(0);
    expect(fetch).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(4_999);
    expect(fetch).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(ended).not.toHaveBeenCalled();
  });

  it('tries again on a proxy\'s 5xx, a network failure or an answer it can\'t read', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(answer(502, { error: 'bad gateway' }))
      .mockRejectedValueOnce(new TypeError('Failed to fetch'))
      .mockResolvedValueOnce(new Response('<html>', { status: 200 }))
      .mockResolvedValueOnce(answer(504))
      .mockResolvedValueOnce(answer(200, session({ expires: null })));
    const ended = vi.fn();
    const changed = vi.fn();
    keepFresh(session({ expires: new Date(NOW).toISOString() }), { fetch, ended }, changed);
    await vi.advanceTimersByTimeAsync(0);
    for (let n = 2; n <= 5; n++) {
      await vi.advanceTimersByTimeAsync(RETRY_MS);
      expect(fetch).toHaveBeenCalledTimes(n);
    }
    expect(changed).toHaveBeenCalledOnce();
    expect(ended).not.toHaveBeenCalled();
  });

  it('does nothing more once stopped, whatever the refresh answered', async () => {
    let fail!: (e: Error) => void;
    const fetch = vi.fn().mockImplementationOnce(() => new Promise((_, reject) => (fail = reject)));
    const stop = keepFresh(session({ expires: new Date(NOW).toISOString() }), { fetch, ended: vi.fn() }, vi.fn());
    await vi.advanceTimersByTimeAsync(0);
    stop();
    fail(new TypeError('Failed to fetch'));
    await vi.advanceTimersByTimeAsync(10 * RETRY_MS);
    expect(fetch).toHaveBeenCalledOnce();
  });

  it('ends when the provider refuses the refresh', async () => {
    for (const status of [401, 403]) {
      const fetch = vi.fn().mockResolvedValue(answer(status, { error: 'invalid_grant' }));
      const ended = vi.fn();
      keepFresh(session({ expires: new Date(NOW).toISOString() }), { fetch, ended }, vi.fn());
      await vi.advanceTimersByTimeAsync(0);
      expect(ended).toHaveBeenCalledOnce();
    }
  });

  it('leaves alone what has no token to refresh', async () => {
    const fetch = vi.fn();
    for (const s of [session({ auth: 'basic' }), session({ expires: null }), session({ auth: 'jwt' })]) {
      keepFresh(s, { fetch, ended: vi.fn() }, vi.fn());
    }
    await vi.advanceTimersByTimeAsync(24 * 3600_000);
    expect(fetch).not.toHaveBeenCalled();
  });
});
