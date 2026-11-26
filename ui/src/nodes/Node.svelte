<script lang="ts">
  // A node's page (Admin_Console §4, mockup 04): its header, the pipelines it holds, its lease
  // renewals, memory and CPU over 6 hours, what it recorded, and what its gossip sees; an admin's
  // leave and shutdown (mockup 11).
  import { onMount } from 'svelte';
  import type { Client } from '../lib/api';
  import Chart from '../lib/Chart.svelte';
  import Dialog from '../lib/Dialog.svelte';
  import { failingNotice, failures, staleNotice, type PartRead } from '../lib/notices';
  import type { Session } from '../lib/session';
  import type { Model } from '../model/model.svelte';
  import { LAG_TARGET_S } from '../overview/attention';
  import Sparkline from '../overview/Sparkline.svelte';
  import { activity } from '../pipelines/detail';
  import type { LagHistory } from '../pipelines/lag.svelte';
  import { askNode, mayAdmin, NODE_ACTIONS, NODE_VERBS, nodeTarget, recordedOn, typedOk, type NodeVerb } from './actions';
  import { NODE_PART_NAMES, NODE_PARTS, SPAN_S, type NodePage } from './page.svelte';
  import { cpuNote, gossipLines, memoryNote, nodeHeader, nodePipelines, renewalNote } from './view';

  interface Props {
    page: NodePage;
    model: Model;
    /** Every pipeline's lag over 6 hours. */
    lag: LagHistory;
    session: Session;
    api: Client;
    /** The node the console's live updates come through. */
    via: string | null;
    now: number;
    /** The live updates stopped: every number is as of `last`. */
    stale: boolean;
    last: number | null;
    /** Offline: actions wait. */
    offline: boolean;
  }
  let { page, model, lag, session, api, via, now, stale, last, offline }: Props = $props();

  const name = $derived(page.name);
  const head = $derived(nodeHeader(name, model.facts, page.detail, now));
  const held = $derived(nodePipelines(name, model.facts, page.detail, now));
  /** The stream's figures are fresher than the node's detail. */
  const live = $derived(model.facts.nodes[name] ?? page.detail);
  const since = $derived(now / 1000 - SPAN_S);
  const lines = $derived(activity(page.events, now));
  const failing = $derived(
    failingNotice(failures(NODE_PARTS.map((p): [string, PartRead] => [NODE_PART_NAMES[p], page.read[p]]), now)),
  );
  const series = (s: string) => page.series[s] ?? [];

  /** A member still in the ring: a leaving one is leaving already. */
  const active = $derived(model.facts.cluster?.members.find((m) => m.node === name)?.phase === 'active');
  let acting = $state<NodeVerb | null>(null);
  let typed = $state('');
  const blocked = $derived(
    offline
      ? 'Offline: actions wait until the console is live again.'
      : acting !== null && NODE_ACTIONS[acting].typed && !typedOk(typed, name)
        ? `Type ${name} to confirm.`
        : null,
  );

  function open(v: NodeVerb) {
    typed = '';
    acting = v;
  }

  async function act(v: NodeVerb, note: string): Promise<string | null> {
    try {
      // What follows comes on the stream: the recorded action, and the member's new phase.
      await askNode(api, v, name, note);
      return null;
    } catch (e) {
      return e instanceof Error ? e.message : String(e);
    }
  }

  onMount(() => void lag.ensure());
</script>

{#if page.missing}
  <h2>{name}</h2>
  <p role="alert">There is no member {name} in this node's view of the cluster.</p>
  <a href="/ui/cluster">The cluster →</a>
{:else}
  <h2>
    {name}
    {#if head !== null}<span class="chip {head.state.tone}">{head.state.label}</span>{/if}
  </h2>
  {#if head !== null}<p class="where muted">{head.line}</p>{/if}
  {#if mayAdmin(session.role) && head !== null}
    <div class="actions">
      {#each NODE_VERBS as v (v)}
        <button type="button" class="destructive" disabled={offline || !active} onclick={() => open(v)}>
          {NODE_ACTIONS[v].label}…
        </button>
      {/each}
    </div>
    <p class="muted">
      Leave hands its pipelines to the others and stays up, out of the ring. Shut down leaves, then
      exits; a supervisor such as Kubernetes starts it again. There is no in-place restart. Admin
      only.
    </p>
  {/if}
  {#if stale}
    <p class="notice" role="status">{staleNotice(last, now)}</p>
  {/if}
  {#if failing !== null}
    <p class="notice failing" role="alert">{failing}</p>
  {/if}

  <section class="panel pipelines" class:stale>
    <h3>Pipelines on this node · {held.length}</h3>
    <table>
      <thead>
        <tr><th>Pipeline</th><th>State</th><th>Lag, 6 h</th><th>Lease</th></tr>
      </thead>
      <tbody>
        {#each held as p (p.id)}
          <tr>
            <td><a href={p.href}>{p.id}</a></td>
            <td><span class="chip {p.state.tone}">{p.state.label}</span></td>
            <td class="lag">
              <Sparkline points={lag.lines[p.id] ?? []} target={LAG_TARGET_S} />
              {p.lag}
            </td>
            <td>{p.lease}</td>
          </tr>
        {:else}
          <tr><td colspan="4" class="muted">It holds no pipeline.</td></tr>
        {/each}
      </tbody>
    </table>
  </section>

  <section class="panel charts" class:stale>
    <h3>This node, 6 h</h3>
    <div class="three">
      <div>
        <p class="note">{renewalNote(series('lease_renew_seconds'))}</p>
        <Chart label="Lease renewal, 6 h" unit="seconds" points={series('lease_renew_seconds')} {since} until={now / 1000} height={140} />
      </div>
      <div>
        <p class="note">{live === null ? '—' : memoryNote(live.process)}</p>
        <Chart
          label="Memory, 6 h"
          unit="bytes"
          points={series('memory_bytes')}
          {since}
          until={now / 1000}
          target={live?.process.memory_limit_bytes ?? null}
          height={140}
        />
      </div>
      <div>
        <p class="note">{live === null ? '—' : cpuNote(live.process)}</p>
        <Chart
          label="CPU, 6 h"
          unit="cores"
          points={series('cpu_cores')}
          {since}
          until={now / 1000}
          target={live?.process.cpu_limit_cores ?? null}
          height={140}
        />
      </div>
    </div>
  </section>

  <div class="cols">
    <section class="panel" class:stale>
      <h3>Activity on this node</h3>
      <ul class="activity">
        {#each lines as e (e.key)}
          <li class={e.severity}>
            <span class="when">{e.when}</span>
            <span>{e.text}{#if e.who !== null}<span class="muted"> · {e.who}</span>{/if}{#if e.note !== null}<span class="muted"> "{e.note}"</span>{/if}</span>
          </li>
        {:else}
          <li class="muted">Nothing recorded by it yet.</li>
        {/each}
      </ul>
      <a href="/ui/events?node={encodeURIComponent(name)}">All its events →</a>
    </section>
    <section class="panel gossip" class:stale>
      <h3>Gossip</h3>
      {#if live === null}
        <p class="muted">Not read yet.</p>
      {:else}
        <dl>
          {#each gossipLines(live.gossip, now) as g (g.label)}
            <dt>{g.label}</dt>
            <dd>{g.value}</dd>
          {/each}
        </dl>
      {/if}
    </section>
  </div>

  {#if acting !== null}
    {@const v = acting}
    {@const target = nodeTarget(name, model.facts, via)}
    <Dialog
      title={NODE_ACTIONS[v].title(name)}
      lines={NODE_ACTIONS[v].lines(target)}
      recorded={recordedOn(v, name, session)}
      confirm={NODE_ACTIONS[v].confirm(name)}
      destructive={NODE_ACTIONS[v].destructive}
      {blocked}
      onConfirm={(note) => act(v, note)}
      onClose={() => (acting = null)}
    >
      {#if NODE_ACTIONS[v].typed}
        <label>Type {name} to confirm <input class="typed" bind:value={typed} autocomplete="off" spellcheck="false" /></label>
      {/if}
    </Dialog>
  {/if}
{/if}

<style>
  .where {
    margin-top: -6px;
  }
  .actions {
    display: flex;
    gap: 8px;
  }
  .destructive {
    color: var(--red);
  }
  .notice {
    padding: 6px 10px;
    border-radius: 6px;
    background: var(--grey-bg);
  }
  .notice.failing {
    background: var(--orange-bg);
  }
  .stale {
    opacity: 0.55;
  }
  .panel {
    background: var(--bg-panel);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 10px 12px;
    margin-bottom: 12px;
  }
  .panel h3 {
    margin: 0 0 8px;
    font-size: 14px;
  }
  .three {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 12px;
  }
  .note {
    margin: 0 0 4px;
    font-size: 12px;
    color: var(--text-2);
  }
  .cols {
    display: grid;
    grid-template-columns: 2fr 1fr;
    gap: 12px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
  }
  th,
  td {
    text-align: left;
    padding: 4px 6px;
    border-bottom: 1px solid var(--border);
  }
  .lag {
    display: flex;
    gap: 8px;
    align-items: center;
  }
  dl {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 4px 10px;
    margin: 0;
  }
  dt {
    color: var(--text-2);
  }
  dd {
    margin: 0;
  }
  .activity {
    list-style: none;
    padding: 0;
    margin: 0 0 8px;
  }
  .activity li {
    display: flex;
    gap: 10px;
    padding: 3px 4px;
  }
  .activity li.error .when {
    color: var(--red);
  }
  .when {
    color: var(--text-2);
    min-width: 80px;
    font-variant-numeric: tabular-nums;
  }
  .chip {
    font-size: 12px;
    font-weight: normal;
    padding: 1px 8px;
    border-radius: 10px;
    background: var(--grey-bg);
  }
  .chip.green {
    background: var(--green-bg);
  }
  .chip.orange {
    background: var(--orange-bg);
  }
  .chip.red {
    background: var(--red-bg);
  }
</style>
