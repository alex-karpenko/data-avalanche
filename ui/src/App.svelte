<script lang="ts">
  // The console's app: who is signed in, the live stream, and the page the address names.
  import { onMount } from 'svelte';
  import Frame from './Frame.svelte';
  import Later from './Later.svelte';
  import { client } from './lib/api';
  import { Live } from './lib/live.svelte';
  import { Router } from './lib/router.svelte';
  import { keepFresh, readSession, type Session } from './lib/session';
  import { Overview as OverviewModel } from './overview/model.svelte';
  import Overview from './overview/Overview.svelte';

  // The server says why the session ended: its 401 page, or the sign-in.
  const ended = () => location.reload();
  const fetch = globalThis.fetch.bind(globalThis);

  let session = $state<Session | null>(null);
  let generation = $state<number | null>(null);
  let now = $state(Date.now());
  const router = new Router();
  const api = client({ fetch, csrf: () => session?.csrf ?? null, ended });
  const live = new Live({
    open: (url) => new EventSource(url),
    check: async () => {
      const r = await fetch('/ui/session', { headers: { Accept: 'application/json' } });
      if (r.status === 401) {
        ended();
        return 'ended';
      }
      return r.ok ? 'ok' : 'later';
    },
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
    void readSession({ fetch, ended }).then((s) => {
      if (s === null) return;
      session = s;
      stops.push(keepFresh(s, { fetch, ended }, (next) => (session = next)));
      live.start();
      void readGeneration();
      void overview.load();
    });
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
{/if}
