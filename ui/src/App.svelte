<script lang="ts">
  // The console's app: who is signed in, the live stream, and the page the address names.
  import { onMount } from 'svelte';
  import Frame from './Frame.svelte';
  import Later from './Later.svelte';
  import { client } from './lib/api';
  import { Live } from './lib/live.svelte';
  import { Router } from './lib/router.svelte';
  import { awaitSession, checkSession, keepFresh, RETRY_MS, type Session } from './lib/session';
  import { Overview as OverviewModel } from './overview/model.svelte';
  import Overview from './overview/Overview.svelte';

  // The server says why the session ended: its 401 or 403 page, or the sign-in.
  const ended = () => location.reload();
  const fetch = globalThis.fetch.bind(globalThis);

  let session = $state<Session | null>(null);
  /** Why the session couldn't be read, while it is read again. */
  let unread = $state<string | null>(null);
  let generation = $state<number | null>(null);
  let now = $state(Date.now());
  const router = new Router();
  const api = client({ fetch, csrf: () => session?.csrf ?? null, ended });
  const live = new Live({
    open: (url) => new EventSource(url),
    check: () => checkSession({ fetch, ended }),
  });
  const overview = new OverviewModel(api);

  async function readGeneration() {
    try {
      generation = (await api.get<{ generation: number }>('/v1/config/status')).generation;
    } catch {
      // The generation stays as last read; the page shows how old the rest is.
    }
  }

  onMount(() => {
    const stops = [
      router.listen(),
      live.onResync(() => void readGeneration()),
      live.on('event', (e) => {
        if ((e as { kind?: string }).kind === 'config.published') void readGeneration();
      }),
      overview.follow(live),
    ];
    const read = (s: Session) => {
      session = s;
      unread = null;
      stops.push(keepFresh(s, { fetch, ended }, (next) => (session = next)));
      live.start();
      void readGeneration();
      void overview.load();
    };
    stops.push(awaitSession({ fetch, ended }, read, (why) => (unread = why)));
    const tick = setInterval(() => {
      now = Date.now();
      live.tick(now);
    }, 1000);
    return () => {
      for (const stop of stops) stop();
      live.stop();
      clearInterval(tick);
    };
  });
</script>

{#if session !== null}
  <Frame
    {session}
    section={router.section}
    {generation}
    state={live.state}
    last={live.last}
    {now}
    onRetry={() => live.retry()}
  >
    {#if router.section === 'overview'}
      <Overview model={overview} {now} stale={live.stale(now)} last={live.last} />
    {:else}
      <Later section={router.section} />
    {/if}
  </Frame>
{:else if unread !== null}
  <p class="unread" role="alert">
    The console couldn't read your session: {unread}. Trying again in {RETRY_MS / 1000} s.
    <button type="button" onclick={() => location.reload()}>Reload</button>
  </p>
{/if}
