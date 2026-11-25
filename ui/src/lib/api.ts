// The console's calls to `/v1` (Admin_Console §9.1): the session's cookie goes by itself; a call
// that changes something adds the session's anti-forgery token. A 401 leaves the page to the
// server, which says why the session ended.

/** How long one section of a page waits for its read (Admin_Console §8: each has its own). */
export const READ_WITHIN_MS = 5_000;

/** A `/v1` answer that isn't a success: its status and the server's `error`. */
export class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

export interface Deps {
  fetch: typeof fetch;
  /** The session's anti-forgery token, `null` without a session. */
  csrf: () => string | null;
  ended: () => void;
}

export interface Client {
  /** `GET path`, within `READ_WITHIN_MS`. */
  get<T>(path: string): Promise<T>;
  /** A call that changes something, with its JSON body. */
  send<T>(method: 'POST' | 'PUT' | 'DELETE', path: string, body?: unknown): Promise<T>;
}

export function client(deps: Deps): Client {
  const call = async <T>(path: string, init: RequestInit): Promise<T> => {
    const r = await deps.fetch(path, init);
    if (r.status === 401) deps.ended();
    if (!r.ok) {
      const body = (await r.json().catch(() => ({}))) as { error?: string };
      throw new HttpError(r.status, body.error ?? r.statusText);
    }
    return (await r.json()) as T;
  };
  return {
    get: (path) =>
      call(path, {
        headers: { Accept: 'application/json' },
        signal: AbortSignal.timeout(READ_WITHIN_MS),
      }),
    send: (method, path, body) => {
      const headers: Record<string, string> = {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      };
      const csrf = deps.csrf();
      if (csrf !== null) headers['X-CSRF-Token'] = csrf;
      return call(path, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    },
  };
}
