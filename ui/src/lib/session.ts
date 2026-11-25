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
/** The wait before reading the session or refreshing it again, when no `Retry-After` says. */
export const RETRY_MS = 5_000;

/** What the session's calls need: `ended` leaves the page to the server, which says why. */
export interface Deps {
  fetch: typeof fetch;
  ended: () => void;
}

/** Whether a status ends the session: signed out (401), or a role taken away (403). */
const ends = (status: number) => status === 401 || status === 403;

/** The caller's session; `null` once the page is left to the server (`ended`). */
export async function readSession(deps: Deps): Promise<Session | null> {
  const r = await deps.fetch('/ui/session', { headers: { Accept: 'application/json' } });
  if (ends(r.status)) {
    deps.ended();
    return null;
  }
  if (!r.ok) throw new Error(`the server answered ${r.status}`);
  return (await r.json()) as Session;
}

/**
 * Reads the session until it is read, then hands it to `read`; a read that fails is told to
 * `failed`, with why, and tried again after `RETRY_MS`. Returns the way to stop.
 */
export function awaitSession(
  deps: Deps,
  read: (s: Session) => void,
  failed: (why: string) => void,
): () => void {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let stopped = false;
  const attempt = async () => {
    try {
      const s = await readSession(deps);
      if (!stopped && s !== null) read(s);
    } catch (e) {
      if (stopped) return;
      failed(e instanceof Error ? e.message : String(e));
      timer = setTimeout(() => void attempt(), RETRY_MS);
    }
  };
  void attempt();
  return () => {
    stopped = true;
    clearTimeout(timer);
  };
}

/**
 * For the live stream once it closed: whether the session still holds (`ok`), the page was left
 * to the server (`ended`), or the server couldn't tell (`later`).
 */
export async function checkSession(deps: Deps): Promise<'ok' | 'ended' | 'later'> {
  const r = await deps.fetch('/ui/session', { headers: { Accept: 'application/json' } });
  if (ends(r.status)) {
    deps.ended();
    return 'ended';
  }
  return r.ok ? 'ok' : 'later';
}

/**
 * Keeps an oauth2 session's token fresh: refreshes it `REFRESH_AHEAD_MS` before it expires. A
 * refresh that fails otherwise (a provider down, a proxy's 5xx in a rolling restart, the network,
 * an answer that can't be read) is tried again after its `Retry-After`, else `RETRY_MS`; only a
 * refusal (401, 403) ends the session. Returns the way to stop.
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
  const again = (s: Session, after = RETRY_MS) => {
    if (!stopped) timer = setTimeout(() => void refresh(s), after);
  };
  const refresh = async (s: Session) => {
    let next: Session;
    try {
      const r = await deps.fetch('/ui/session/refresh', {
        method: 'POST',
        headers: { Accept: 'application/json', 'X-CSRF-Token': s.csrf ?? '' },
      });
      if (stopped) return;
      if (ends(r.status)) {
        deps.ended();
        return;
      }
      if (!r.ok) {
        const after = Number(r.headers.get('Retry-After'));
        again(s, after > 0 ? after * 1000 : RETRY_MS);
        return;
      }
      next = (await r.json()) as Session;
    } catch {
      again(s);
      return;
    }
    if (stopped) return;
    changed(next);
    plan(next);
  };
  plan(first);
  return () => {
    stopped = true;
    clearTimeout(timer);
  };
}
