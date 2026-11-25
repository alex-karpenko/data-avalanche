import { describe, expect, it, vi } from 'vitest';
import { client, HttpError } from './api';

const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status });

describe('the calls to /v1', () => {
  it('read JSON within their time limit', async () => {
    const fetch = vi.fn().mockResolvedValue(json(200, { pipelines: [] }));
    const api = client({ fetch, csrf: () => 'token', ended: vi.fn() });
    expect(await api.get('/v1/pipelines')).toEqual({ pipelines: [] });
    const [path, init] = fetch.mock.calls[0];
    expect(path).toBe('/v1/pipelines');
    expect(init.signal).toBeInstanceOf(AbortSignal);
    expect(init.headers).toEqual({ Accept: 'application/json' });
  });

  it('send the anti-forgery token with what changes something', async () => {
    const fetch = vi.fn().mockImplementation(async () => json(202, { accepted: true }));
    const api = client({ fetch, csrf: () => 'token', ended: vi.fn() });
    await api.send('POST', '/v1/pipelines/orders/pause', { note: 'upgrade' });
    expect(fetch).toHaveBeenCalledWith('/v1/pipelines/orders/pause', {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'X-CSRF-Token': 'token',
      },
      body: '{"note":"upgrade"}',
    });
    // Without a session (none, jwt): no token.
    const bare = client({ fetch, csrf: () => null, ended: vi.fn() });
    await bare.send('POST', '/v1/pipelines/orders/flush');
    expect(fetch.mock.calls[1][1].headers).not.toHaveProperty('X-CSRF-Token');
  });

  it("leave the page to the server on a 401, and say a refusal's reason", async () => {
    const ended = vi.fn();
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(json(401, { error: 'the session ended' }))
      .mockResolvedValueOnce(json(403, { error: 'operator or above' }));
    const api = client({ fetch, csrf: () => null, ended });
    await expect(api.get('/v1/pipelines')).rejects.toEqual(new HttpError(401, 'the session ended'));
    expect(ended).toHaveBeenCalledOnce();
    const refused = api.send('POST', '/v1/pipelines/orders/pause');
    await expect(refused).rejects.toMatchObject({ status: 403, message: 'operator or above' });
    expect(ended).toHaveBeenCalledOnce();
  });
});
