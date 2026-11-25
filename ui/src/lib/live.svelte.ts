// The live stream (Admin_Console §8, §8.1): one `EventSource` on `/v1/stream` per page. Each
// message is handed to whoever listens for its kind; the page reads the current state when the
// stream (re)starts or missed some, since the stream has no replay. How old the last message
// is says whether the page is live, stale or offline: a number is never shown as current when it
// isn't.

export const KINDS = [
  'hello',
  'pipeline',
  'figures',
  'node',
  'samples',
  'event',
  'members',
  'peer',
  'reset',
] as const;
export type Kind = (typeof KINDS)[number];

/** After this long without a message every panel is stale: each node sends every 5 s. */
export const STALE_AFTER_MS = 10_000;
/** After this long without a message the page is offline: everything on it is as of then. */
export const OFFLINE_AFTER_MS = 30_000;
/** The waits before opening the stream again: 1, 2, 5, then every 10 s. */
export const REOPEN_MS = [1_000, 2_000, 5_000, 10_000];

export type State = 'connecting' | 'live' | 'reconnecting' | 'offline';

/** What the stream needs of an `EventSource`. */
export interface SourceLike {
  readonly readyState: number;
  onerror: ((e: Event) => void) | null;
  addEventListener(kind: string, fn: (e: MessageEvent) => void): void;
  close(): void;
}

export interface Deps {
  open: (url: string) => SourceLike;
  /**
   * Once the stream closed, whether its credentials still hold: go on (`ok`), the page was left
   * to the server (`ended`), or ask again later.
   */
  check: () => Promise<'ok' | 'ended' | 'later'>;
}

const CLOSED = 2;

export class Live {
  state = $state<State>('connecting');
  /** When the last message came (ms since the epoch); `null` before the first. */
  last = $state<number | null>(null);

  #deps: Deps;
  #url: string;
  #source: SourceLike | null = null;
  #listeners = new Map<Kind, Set<(data: unknown) => void>>();
  #resync = new Set<() => void>();
  #hellos = 0;
  #opened = 0;
  #reopens = 0;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #stopped = false;

  constructor(deps: Deps, url = '/v1/stream') {
    this.#deps = deps;
    this.#url = url;
  }

  /** Hands each message of `kind` to `fn`; returns the way to stop. */
  on(kind: Kind, fn: (data: unknown) => void): () => void {
    const set = this.#listeners.get(kind) ?? new Set();
    set.add(fn);
    this.#listeners.set(kind, set);
    return () => set.delete(fn);
  }

  /** `fn` reads the current state again: the stream restarted, or missed messages. */
  onResync(fn: () => void): () => void {
    this.#resync.add(fn);
    return () => this.#resync.delete(fn);
  }

  start(): void {
    this.#stopped = false;
    this.#open();
  }

  stop(): void {
    this.#stopped = true;
    clearTimeout(this.#timer);
    this.#source?.close();
    this.#source = null;
  }

  /** Opens the stream again now ("Retry now"). */
  retry(): void {
    clearTimeout(this.#timer);
    this.#source?.close();
    this.#reopens = 0;
    this.#open();
  }

  /** Whether the page's numbers are older than `STALE_AFTER_MS` at `now`. */
  stale(now: number): boolean {
    return now - (this.last ?? this.#opened) >= STALE_AFTER_MS;
  }

  /** Brings `state` up to `now`; the page calls it every second. A stream silent past
   *  `OFFLINE_AFTER_MS` is opened again: a proxy may hold a dead connection. */
  tick(now: number): void {
    const quiet = now - (this.last ?? this.#opened);
    if (quiet < STALE_AFTER_MS) {
      this.state = this.last === null ? 'connecting' : 'live';
    } else if (quiet < OFFLINE_AFTER_MS) {
      this.state = 'reconnecting';
    } else {
      this.state = 'offline';
      if (this.#source !== null && this.#timer === undefined) this.#closed();
    }
  }

  #open(): void {
    this.#timer = undefined;
    if (this.#stopped) return;
    this.#opened ||= Date.now();
    const source = this.#deps.open(this.#url);
    this.#source = source;
    for (const kind of KINDS) {
      source.addEventListener(kind, (e) => this.#message(kind, e.data as string));
    }
    source.onerror = () => {
      // Still connecting: the browser tries again by itself. Closed: a refusal (a 401), or
      // a reset; see whether the credentials still hold before opening it again.
      if (source.readyState === CLOSED && this.#source === source) this.#closed();
    };
  }

  #message(kind: Kind, text: string): void {
    let data: unknown;
    try {
      data = JSON.parse(text);
    } catch {
      return;
    }
    this.last = Date.now();
    this.#reopens = 0;
    this.state = 'live';
    if (kind === 'hello' && this.#hellos++ > 0) this.#resyncAll();
    if (kind === 'reset') {
      const reason = (data as { reason?: string }).reason;
      if (reason === 'lagged') this.#resyncAll();
      if (reason === 'credentials') this.#closed();
    }
    for (const fn of this.#listeners.get(kind) ?? []) fn(data);
  }

  #resyncAll(): void {
    for (const fn of this.#resync) fn();
  }

  #closed(): void {
    this.#source?.close();
    this.#source = null;
    const wait = REOPEN_MS[Math.min(this.#reopens++, REOPEN_MS.length - 1)];
    this.#timer = setTimeout(() => {
      void this.#deps.check().then((verdict) => {
        if (verdict === 'ended' || this.#stopped) return;
        this.#open();
      });
    }, wait);
  }
}
