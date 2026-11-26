<script lang="ts">
  // The Cluster page (Admin_Console §4, mockup 03): its members, the pipelines held away from
  // their ring owner, every lease, and the removals waiting for their teardown (DA-API-21).
  import { clock } from '../lib/format';
  import { failingNotice, failures, staleNotice, type PartRead } from '../lib/notices';
  import Tiles from '../lib/Tiles.svelte';
  import { PART_NAMES, type Model, type Part } from '../model/model.svelte';
  import { REMOVAL_PENDING_S } from '../overview/attention';
  import { awayRows, clusterTiles, leaseRows, memberRows, removalRows } from './derive';
  import type { Renewals } from './renewals.svelte';

  interface Props {
    model: Model;
    renewals: Renewals;
    now: number;
    /** The live updates stopped: every number is as of `last`. */
    stale: boolean;
    last: number | null;
  }
  let { model, renewals, now, stale, last }: Props = $props();

  /** The parts the page shows. */
  const PAGE_PARTS: Part[] = ['cluster', 'config', 'jobs', 'leases', 'removals'];

  const tiles = $derived(clusterTiles(model.facts, now));
  const members = $derived(memberRows(model.facts, renewals.max, now));
  const away = $derived(awayRows(model.facts, now));
  const leases = $derived(leaseRows(model.facts, now));
  const removals = $derived(removalRows(model.facts, now));
  const failing = $derived(
    failingNotice(
      failures(
        [
          ...PAGE_PARTS.map((p): [string, PartRead] => [PART_NAMES[p], model.read[p]]),
          ...Object.entries(renewals.read).map(([n, r]): [string, PartRead] => [`${n}'s lease renewals`, r]),
        ],
        now,
      ),
    ),
  );
</script>

<h2>Cluster</h2>
{#if stale}
  <p class="notice" role="status">{staleNotice(last, now)}</p>
{/if}
{#if failing !== null}
  <p class="notice failing" role="alert">{failing}</p>
{/if}

<Tiles {tiles} {stale} />

<section class="panel members" class:stale>
  <h3>Members</h3>
  <table>
    <thead>
      <tr>
        <th>Node</th><th>State</th><th>Address</th><th>Version</th><th>Config</th><th>Pipelines</th>
        <th>Lease renewal, slowest 1 h</th><th>Up</th>
      </tr>
    </thead>
    <tbody>
      {#each members as m (m.node)}
        <tr class={m.state.tone}>
          <td><a href={m.href}>{m.node}</a></td>
          <td class={m.state.tone}>{m.state.label}</td>
          <td>{m.address}</td>
          <td>{m.build}</td>
          <td class={m.generation.tone}>{m.generation.value}</td>
          <td>{m.pipelines}</td>
          <td>{m.renewal}</td>
          <td>{m.up}</td>
        </tr>
      {/each}
    </tbody>
  </table>
  <p class="muted">
    States: alive · suspect (missed probes; stays in the ring) · leaving (handing off) · joining. A
    node the console's stream can't reach says for how long.
  </p>
</section>

{#if away.length > 0}
  <section class="panel away" class:stale>
    <h3>Away from their owner · {away.length}</h3>
    <table>
      <thead>
        <tr><th>Pipeline</th><th>Held by</th><th>Owner</th><th>Since</th><th>Why</th><th>Goes back</th></tr>
      </thead>
      <tbody>
        {#each away as a (a.id)}
          <tr class={a.tone}>
            <td><a href={a.href}>{a.id}</a></td>
            <td>{a.holder}</td>
            <td>{a.owner}</td>
            <td>{a.since === null ? '—' : clock(a.since)}</td>
            <td>{a.why}</td>
            <td>{a.back}</td>
          </tr>
        {/each}
      </tbody>
    </table>
    <p class="muted">
      Shown only while something is away; red once one has been away over an hour with no hand-off
      to say why.
    </p>
  </section>
{/if}

<section class="panel leases" class:stale>
  <h3>Leases · {leases.length}</h3>
  <table>
    <thead>
      <tr><th>Pipeline</th><th>Holder</th><th>Ring owner</th><th>Epoch</th><th>Expires</th><th>State</th></tr>
    </thead>
    <tbody>
      {#each leases as l (l.id)}
        <tr>
          <td><a href={l.href}>{l.id}</a></td>
          <td>{l.holder ?? '—'}</td>
          <td>{l.owner ?? '—'}</td>
          <td>{l.epoch ?? '—'}</td>
          <td>{l.expires}</td>
          <td class={l.state.tone}>{l.state.label}</td>
        </tr>
      {/each}
    </tbody>
  </table>
</section>

{#if removals.length > 0}
  <section class="panel removals" class:stale>
    <h3>Pending removals · {removals.length}</h3>
    <table>
      <thead>
        <tr><th>Pipeline</th><th>Removed by generation</th><th>Since</th></tr>
      </thead>
      <tbody>
        {#each removals as r (r.id)}
          <tr>
            <td>{r.id}</td>
            <td>{r.generation}</td>
            <td class={r.tone}>{r.since}</td>
          </tr>
        {/each}
      </tbody>
    </table>
    <p class="muted">
      Its owner tears a removed pipeline down; one no node owns, the node the ring places it on
      within a sweep (every 30 s). Needs attention lists one still pending after
      {REMOVAL_PENDING_S / 60} min.
    </p>
  </section>
{/if}

<style>
  .notice {
    padding: 6px 10px;
    border-radius: 6px;
    background: var(--grey-bg);
    margin: 0 0 10px;
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
    margin: 12px 0;
  }
  .panel h3 {
    margin: 0 0 8px;
    font-size: 14px;
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
  td.orange,
  tr.orange td:first-child {
    color: var(--orange);
  }
  td.red,
  tr.red td:first-child {
    color: var(--red);
  }
  td.green {
    color: var(--green);
  }
  .muted {
    font-size: 12px;
  }
</style>
