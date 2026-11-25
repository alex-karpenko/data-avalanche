// Who the console's page acts for (Admin_Console §9.1): `GET /ui/session`, and an oauth2
// session's token refreshed before it expires.

export type Role = 'viewer' | 'operator' | 'admin';
export type AuthMode = 'none' | 'basic' | 'oauth2' | 'jwt';

/** `GET /ui/session`: the caller, how they signed in, and the anti-forgery token. */
export interface Session {
  name: string;
  role: Role;
  auth: AuthMode;
  /** When the credentials expire (RFC 3339); `null` when they don't. */
  expires: string | null;
  signed_in: string | null;
  /** The session's anti-forgery token, `null` without a session (`none`, `jwt`). */
  csrf: string | null;
}

/** How long before its token expires an oauth2 session is refreshed: the server's own lead. */
export const REFRESH_AHEAD_MS = 60_000;
/** The wait before trying again when the provider can't be reached and says no `Retry-After`. */
export const RETRY_MS = 5_000;

/** What the session's calls need: `ended` leaves the page to the server, which says why. */
export interface Deps {
  fetch: typeof fetch;
  ended: () => void;
}

/** The caller's session; `null` once the page is left to the server (`ended`). */
export async function readSession(deps: Deps): Promise<Session | null> {
  const r = await deps.fetch('/ui/session', { headers: { Accept: 'application/json' } });
  if (r.status === 401) {
    deps.ended();
    return null;
  }
  if (!r.ok) throw new Error(`the session couldn't be read: ${r.status}`);
  return (await r.json()) as Session;
}

/**
 * Keeps an oauth2 session's token fresh: refreshes it `REFRESH_AHEAD_MS` before it expires. A
 * provider that can't be reached (503) is asked again after its `Retry-After`; a refusal (401)
 * ends the session. Returns the way to stop.
 */
export function keepFresh(
  first: Session,
  deps: Deps,
  changed: (s: Session) => void,
): () => void {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let stopped = false;
  const plan = (s: Session) => {
    if (stopped || s.auth !== 'oauth2' || s.expires === null) return;
    const wait = Math.max(0, Date.parse(s.expires) - REFRESH_AHEAD_MS - Date.now());
    timer = setTimeout(() => void refresh(s), wait);
  };
  const refresh = async (s: Session) => {
    let r: Response;
    try {
      r = await deps.fetch('/ui/session/refresh', {
        method: 'POST',
        headers: { Accept: 'application/json', 'X-CSRF-Token': s.csrf ?? '' },
      });
    } catch {
      timer = setTimeout(() => void refresh(s), RETRY_MS);
      return;
    }
    if (stopped) return;
    if (r.ok) {
      const next = (await r.json()) as Session;
      changed(next);
      plan(next);
    } else if (r.status === 503) {
      const after = Number(r.headers.get('Retry-After'));
      timer = setTimeout(() => void refresh(s), after > 0 ? after * 1000 : RETRY_MS);
    } else {
      deps.ended();
    }
  };
  plan(first);
  return () => {
    stopped = true;
    clearTimeout(timer);
  };
}
