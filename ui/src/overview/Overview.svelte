<script lang="ts">
  // The Overview (Admin_Console §4, mockup 01): is anything wrong? The headline tiles, what needs
  // attention, the pipelines worst first with their lag over the last hour, the newest events.
  import { ago, bytes, clock, duration } from '../lib/format';
  import {
    FAILED_WITHIN_S,
    LAG_TARGET_S,
    LEASE_LOSS_WITHIN_S,
    PAUSED_FOR_S,
    RISING_OVER_S,
    UNHELD_FOR_S,
    WAL_SHARE,
    WAL_UNLIMITED_BYTES,
    attention,
    nodeHref,
  } from './attention';
  import { staleNotice } from '../lib/notices';
  import { rows, tiles } from './derive';
  import { EVENTS_SHOWN, PARTS, type Model } from '../model/model.svelte';
  import Sparkline from './Sparkline.svelte';

  interface Props {
    model: Model;
    now: number;
    /** The live updates stopped: every number is as of `last`. */
    stale: boolean;
    last: number | null;
  }
  let { model, now, stale, last }: Props = $props();

  const list = $derived(attention(model.facts, now));
  const shown = $derived(tiles(model.facts, now));
  const table = $derived(rows(model.facts, now));
  const events = $derived(model.facts.events.slice(0, EVENTS_SHOWN));
  const failing = $derived(
    PARTS.filter((p) => model.read[p].error !== null).map((p) => `${p} (${model.read[p].error})`),
  );
  const href = (kind: string, id: string) => `/ui/${kind}/${encodeURIComponent(id)}`;
</script>

<h2>Overview</h2>
{#if stale}
  <p class="notice stale-note" role="status">{staleNotice(last, now)}</p>
{/if}
{#if failing.length > 0}
  <p class="notice failing" role="alert">Couldn't be read, shown as last read: {failing.join('; ')}</p>
{/if}

<div class="tiles" class:stale>
  {#each shown as t (t.key)}
    <div class="tile {t.tone}">
      <div class="k">{t.key}</div>
      <div class="v">{t.value}</div>
      <div class="s">{t.detail}</div>
    </div>
  {/each}
</div>

<h3>Needs attention · {list.length}</h3>
{#if list.length === 0}
  <p class="muted">Nothing needs attention.</p>
{:else}
  <ul class="att" class:stale>
    {#each list as a (a.severity + a.what + a.href)}
      <li>
        <span class="dot {a.severity}" title={a.severity}></span>
        <span class="what"><b>{a.what}</b>: {a.why}</span>
        <span class="when">{a.since === null ? '' : ago(a.since, now)}</span>
        <a href={a.href}>{a.link} →</a>
      </li>
    {/each}
  </ul>
{/if}
<p class="rules muted">
  Lag target {LAG_TARGET_S} s, rising over {RISING_OVER_S / 60} min · a slot's WAL past
  {WAL_SHARE * 100}% of max_slot_wal_keep_size, or {bytes(WAL_UNLIMITED_BYTES)} without one ·
  paused or stopped by a person for over {PAUSED_FOR_S / 3600} h · snapshot jobs failed in the last
  {FAILED_WITHIN_S / 3600} h · leases lost in the last {LEASE_LOSS_WITHIN_S / 3600} h, or with no
  holder for over {UNHELD_FOR_S} s
</p>

<div class="two">
  <section>
    <h3>Pipelines</h3>
    <table class:stale>
      <thead>
        <tr><th>Pipeline</th><th>State</th><th>Node</th><th>Lag, last hour</th><th>Last commit</th></tr>
      </thead>
      <tbody>
        {#each table as r (r.id)}
          <tr>
            <td><a href={href('pipelines', r.id)}><b>{r.id}</b></a><div class="muted">{r.what}</div></td>
            <td>
              <span class="chip {r.state.tone}">{r.state.label}</span>
              {#if r.state.note !== null}<div class="muted note">{r.state.note}</div>{/if}
            </td>
            <td>{#if r.node !== null}<a href={nodeHref(r.node)}>{r.node}</a>{/if}</td>
            <td>
              <Sparkline points={r.points} target={LAG_TARGET_S} />
              {r.lag === null ? '' : duration(r.lag)}
            </td>
            <td>{r.lastCommit === null ? '—' : ago(r.lastCommit, now)}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </section>
  <section>
    <h3>Recent events</h3>
    <ul class="ev" class:stale>
      {#each events as e (e.key)}
        <li>
          <span class="t">{clock(Date.parse(e.at))}</span>
          <span class="sev {e.severity}">{e.text}</span>
          {#if e.actor}by <b>{e.actor.name}</b> ({e.actor.role}){/if}
        </li>
      {/each}
    </ul>
    <a href="/ui/events">All events →</a>
  </section>
</div>

<style>
  h3 {
    margin: 16px 0 6px;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-2);
  }
  .notice {
    padding: 6px 10px;
    border-radius: 6px;
  }
  .stale-note {
    background: var(--grey-bg);
  }
  .failing {
    background: var(--orange-bg);
  }
  .stale {
    opacity: 0.55;
  }
  .tiles {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 10px;
  }
  .tile {
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 10px;
    background: var(--bg-panel);
  }
  .k {
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-2);
  }
  .v {
    font-size: 22px;
    font-weight: 600;
  }
  .s {
    font-size: 11px;
    color: var(--text-2);
  }
  .tile.orange .v {
    color: var(--orange);
  }
  .tile.red .v {
    color: var(--red);
  }
  .att,
  .ev {
    list-style: none;
    margin: 0;
    padding: 0;
  }
  .att {
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--bg-panel);
  }
  .att li {
    display: flex;
    gap: 10px;
    align-items: center;
    padding: 7px 10px;
    border-top: 1px solid var(--border);
  }
  .att li:first-child {
    border-top: 0;
  }
  .what {
    flex: 1;
  }
  .when {
    color: var(--text-2);
    font-size: 12px;
  }
  .dot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    flex: none;
  }
  .dot.red {
    background: var(--red);
  }
  .dot.orange {
    background: var(--orange);
  }
  .dot.yellow {
    background: var(--yellow);
  }
  .rules {
    font-size: 11px;
  }
  .two {
    display: grid;
    grid-template-columns: 1.6fr 1fr;
    gap: 16px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
  }
  th,
  td {
    padding: 6px 8px;
    border-top: 1px solid var(--border);
    text-align: left;
    vertical-align: top;
  }
  th {
    font-weight: 500;
    color: var(--text-2);
    border-top: 0;
    font-size: 11px;
    text-transform: uppercase;
  }
  .chip {
    display: inline-block;
    padding: 1px 8px;
    border-radius: 10px;
    font-size: 11px;
    font-weight: 600;
  }
  .chip.green {
    background: var(--green-bg);
    color: var(--green);
  }
  .chip.orange {
    background: var(--orange-bg);
    color: var(--orange);
  }
  .chip.red {
    background: var(--red-bg);
    color: var(--red);
  }
  .chip.grey {
    background: var(--grey-bg);
    color: var(--text-2);
  }
  .note {
    font-size: 11px;
  }
  .ev li {
    padding: 4px 0;
    border-top: 1px solid var(--border);
  }
  .ev li:first-child {
    border-top: 0;
  }
  .t {
    color: var(--text-2);
    display: inline-block;
    width: 44px;
  }
  .sev.error {
    color: var(--red);
  }
  .sev.warning {
    color: var(--orange);
  }
</style>
