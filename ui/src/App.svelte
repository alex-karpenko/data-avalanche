<script lang="ts">
  // The console's app: who is signed in, the live stream, and the page the address names.
  import { onMount, untrack } from 'svelte';
  import Cluster from './cluster/Cluster.svelte';
  import { Renewals } from './cluster/renewals.svelte';
  import Frame from './Frame.svelte';
  import Later from './Later.svelte';
  import { client } from './lib/api';
  import { Live } from './lib/live.svelte';
  import { Router } from './lib/router.svelte';
  import { consoleStatus } from './lib/status';
  import { awaitSession, checkSession, keepFresh, RETRY_MS, type Session } from './lib/session';
  import { Model } from './model/model.svelte';
  import Node from './nodes/Node.svelte';
  import { NodePage } from './nodes/page.svelte';
  import Overview from './overview/Overview.svelte';
  import { LagHistory } from './pipelines/lag.svelte';
  import List from './pipelines/List.svelte';
  import { PipelinePage } from './pipelines/page.svelte';
  import Pipeline from './pipelines/Pipeline.svelte';

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
  const model = new Model(api);
  /** Every pipeline's lag over 6 hours, read when the list is first shown. */
  const lag = new LagHistory(api);
  /** Each member's lease renewals over the last hour, read each time the Cluster page is shown. */
  const renewals = new Renewals(api);
  const onCluster = $derived(router.section === 'cluster' && router.rest.length === 0);
  /** The members' names: read again when they change, not at each read of the cluster. */
  const memberNames = $derived((model.facts.cluster?.members ?? []).map((m) => m.node).join('\n'));
  $effect(() => {
    if (!onCluster || !signedIn || memberNames === '') return;
    const names = memberNames.split('\n');
    // The read follows the members, not the renewals it writes.
    untrack(() => void renewals.load(names));
  });
  /** The pipeline whose page is shown; its page reads and follows it while shown. */
  const pageId = $derived(router.section === 'pipelines' && router.rest.length === 1 ? router.rest[0] : null);
  /** Signed in or not: a token refresh swaps the session's object, not who is signed in. */
  const signedIn = $derived(session !== null);
  let page = $state<PipelinePage | null>(null);
  /** The node whose page is shown: `/ui/cluster/nodes/{name}`. */
  const nodeName = $derived(
    router.section === 'cluster' && router.rest.length === 2 && router.rest[0] === 'nodes' ? router.rest[1] : null,
  );
  let nodePage = $state<NodePage | null>(null);
  $effect(() => {
    if (nodeName === null || !signedIn) {
      nodePage = null;
      return;
    }
    const p = new NodePage(api, nodeName);
    nodePage = p;
    void p.load();
    return p.follow(live);
  });
  $effect(() => {
    if (pageId === null || !signedIn) {
      page = null;
      return;
    }
    // What the stream sent already is drawn at once (Admin_Console §8: now first).
    const p = new PipelinePage(api, pageId, untrack(() => model.facts.figures[pageId] ?? null));
    page = p;
    void p.load();
    return p.follow(live);
  });

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
      model.follow(live),
      lag.follow(live),
    ];
    const read = (s: Session) => {
      session = s;
      unread = null;
      stops.push(keepFresh(s, { fetch, ended }, (next) => (session = next)));
      live.start();
      void readGeneration();
      void model.load();
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
    status={consoleStatus({ state: live.state, via: live.via, last: live.last, facts: model.facts, read: model.read }, now)}
    onStatus={() => void model.load(['lag', 'events'])}
  >
    {#if router.section === 'overview'}
      <Overview {model} {now} stale={live.stale(now)} last={live.last} />
    {:else if onCluster}
      <Cluster {model} {renewals} {now} stale={live.stale(now)} last={live.last} />
    {:else if nodePage !== null && nodePage.name === nodeName}
      {#key nodePage}
        <Node
          page={nodePage}
          {model}
          {lag}
          {session}
          {api}
          via={live.via}
          {now}
          stale={live.stale(now)}
          last={live.last}
          offline={live.state === 'offline'}
        />
      {/key}
    {:else if page !== null && page.id === pageId}
      <!-- Each pipeline's page starts clean: nothing chosen or open carries over from the last. -->
      {#key page}
        <Pipeline
          {page}
          {model}
          {session}
          {api}
          {now}
          stale={live.stale(now)}
          last={live.last}
          offline={live.state === 'offline'}
        />
      {/key}
    {:else if router.section === 'pipelines' && router.rest.length === 0}
      <List
        {model}
        {lag}
        {session}
        {api}
        {router}
        {now}
        stale={live.stale(now)}
        last={live.last}
        offline={live.state === 'offline'}
      />
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
