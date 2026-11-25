<script lang="ts">
  // A pipeline's page (Admin_Console §4, mockup 06): the header with its source, sink, node and
  // lease; its actions; the lag over 6 hours with its events; its tables; its source; its
  // snapshot; its activity. Hovering an event on the chart highlights it in the activity, and the
  // reverse.
  import { onMount } from 'svelte';
  import type { Client } from '../lib/api';
  import { ACTIONS, ask, mayAct, noteBody, recordedAs, VERBS, type Target, type Verb } from '../lib/actions';
  import Chart from '../lib/Chart.svelte';
  import Dialog from '../lib/Dialog.svelte';
  import { ago, clock, duration } from '../lib/format';
  import type { Session } from '../lib/session';
  import { at } from '../model/facts';
  import { CLUSTER_REREAD_MS, type Model } from '../model/model.svelte';
  import { LAG_TARGET_S } from '../overview/attention';
  import { activity, header, marks, snapshotPanel, sourceLines, tableLines } from './detail';
  import { SPAN_S } from './lag.svelte';
  import { PAGE_PARTS, type PipelinePage } from './page.svelte';

  interface Props {
    page: PipelinePage;
    model: Model;
    session: Session;
    api: Client;
    now: number;
    stale: boolean;
    last: number | null;
    offline: boolean;
  }
  let { page, model, session, api, now, stale, last, offline }: Props = $props();

  const id = $derived(page.id);
  const item = $derived(model.facts.pipelines.find((p) => p.id === id));
  const lease = $derived(model.facts.cluster?.pipelines.find((l) => l.id === id));
  const head = $derived(header(page.detail, item, page.figures, lease, now));
  const tables = $derived(tableLines(id, page.figures, page.detail, page.events, now));
  const source = $derived(sourceLines(page.figures));
  const snap = $derived(snapshotPanel(page.jobs, now));
  const lines = $derived(activity(page.events, now));
  const since = $derived(now / 1000 - SPAN_S);
  const drawn = $derived(marks(page.events, since));
  const desired = $derived(page.detail?.desired_state ?? item?.desired_state ?? null);
  const can = $derived(mayAct(session.role) && desired !== null);
  const offered = $derived(desired === null ? [] : VERBS.filter((v) => ACTIONS[v].takes(desired)));
  const failing = $derived(
    PAGE_PARTS.filter((p) => page.read[p].error !== null).map((p) => {
      const r = page.read[p];
      return `${p} (${r.error}), ${r.at === null ? 'never read' : `last read ${ago(r.at, now)}`}`;
    }),
  );
  const asOf = $derived(last ?? 0);
  const readAt = $derived(at(page.figures?.source?.read_at));

  onMount(() => {
    const timer = setInterval(() => void model.load(['cluster']), CLUSTER_REREAD_MS);
    return () => clearInterval(timer);
  });

  let hovered = $state<string | null>(null);
  let acting = $state<Verb | null>(null);
  let snapping = $state(false);
  let chosen = $state<string[]>([]);
  let truncate = $state(false);

  const target = $derived<Target | null>(
    desired === null
      ? null
      : {
          id,
          node: page.figures?.node ?? page.detail?.holder?.node ?? item?.holder ?? null,
          desired,
          actual: page.figures?.actual ?? page.detail?.actual_state ?? null,
          job: snap.current?.job ?? null,
        },
  );
  const blocked = $derived(offline ? 'Offline: actions wait until the console is live again.' : null);

  async function act(v: Verb, note: string): Promise<string | null> {
    try {
      await ask(api, v, id, note);
      return null;
    } catch (e) {
      return e instanceof Error ? e.message : String(e);
    }
  }

  async function snapshot(note: string): Promise<string | null> {
    try {
      await api.send('POST', `/v1/pipelines/${encodeURIComponent(id)}/snapshots`, {
        tables: chosen,
        truncate,
        ...noteBody(note),
      });
      chosen = [];
      truncate = false;
      return null;
    } catch (e) {
      return e instanceof Error ? e.message : String(e);
    }
  }

  const pick = (t: string) => (chosen = chosen.includes(t) ? chosen.filter((c) => c !== t) : [...chosen, t]);
</script>

{#if page.missing}
  <h2>{id}</h2>
  <p role="alert">There is no pipeline {id} in the generation this node runs.</p>
  <a href="/ui/pipelines">All pipelines →</a>
{:else}
  <h2>
    {id}
    {#if head !== null}<span class="chip {head.state.tone}">{head.state.label}</span>{/if}
  </h2>
  {#if head !== null}<p class="where muted">{head.route} · {head.where}</p>{/if}
  {#if page.figures?.error ?? page.detail?.error}
    <p class="error" role="alert">{page.figures?.error ?? page.detail?.error}</p>
  {/if}
  {#if can && target !== null}
    <div class="actions">
      {#each offered as v (v)}
        <button type="button" class:destructive={ACTIONS[v].destructive} disabled={offline} onclick={() => (acting = v)}>
          {ACTIONS[v].label}
        </button>
      {/each}
      {#if desired === 'running'}
        <button type="button" disabled={offline} onclick={() => (snapping = true)}>Snapshot…</button>
      {/if}
    </div>
  {/if}
  {#if stale}
    <p class="notice" role="status">Not current: as of {clock(asOf, true)} · {duration((now - asOf) / 1000)} old</p>
  {/if}
  {#if failing.length > 0}
    <p class="notice failing" role="alert">Couldn't be read, shown as last read: {failing.join('; ')}</p>
  {/if}

  <section class="panel" class:stale>
    <Chart
      label="Lag, last 6 hours"
      unit="seconds"
      points={page.lag.lines[id] ?? []}
      {since}
      until={now / 1000}
      target={LAG_TARGET_S}
      marks={drawn}
      {hovered}
      onHover={(k) => (hovered = k)}
    />
  </section>

  <div class="cols">
    <section class="panel" class:stale>
      <h3>Tables · {tables.length}</h3>
      <table>
        <thead>
          <tr><th>Table</th><th>State</th><th>Changes/min</th><th>Last commit</th><th>Delta v</th><th>Files / MERGE</th><th>Schema</th></tr>
        </thead>
        <tbody>
          {#each tables as t (t.table)}
            <tr>
              <td><a href={t.href}>{t.table}</a></td>
              <td><span class="chip {t.state.tone}">{t.state.label}</span></td>
              <td>{t.changes ?? '—'}</td>
              <td>{t.lastCommit === null ? '—' : ago(t.lastCommit, now)}</td>
              <td>{t.version ?? '—'}</td>
              <td>{t.files ?? '—'}</td>
              <td class="schema">{t.schema}</td>
            </tr>
          {:else}
            <tr><td colspan="7" class="muted">No table known yet: no worker runs it, and none is halted.</td></tr>
          {/each}
        </tbody>
      </table>
    </section>

    <aside>
      <section class="panel" class:stale>
        <h3>Source</h3>
        {#if page.figures?.source == null}
          <p class="muted">No worker runs it now: the source's detail comes from the node that runs it.</p>
        {:else if source.length === 0}
          <p class="muted">
            Not read yet: the node that runs it reads the source's detail at each heartbeat
            (its heartbeat, 60 s by default).
          </p>
        {:else}
          <dl>
            {#each source as s (s.label)}<dt>{s.label}</dt><dd class={s.tone}>{s.value}</dd>{/each}
          </dl>
        {/if}
        {#if readAt !== null}<p class="muted">read {ago(readAt, now)}</p>{/if}
      </section>
      <section class="panel" class:stale>
        <h3>Snapshot</h3>
        {#if snap.current !== null}
          <p><a href={snap.current.href}>{snap.current.job}</a> {snap.current.what}</p>
          {#if snap.current.note !== null}<p class="muted">"{snap.current.note}"</p>{/if}
          <p>{snap.current.progress}</p>
        {:else}
          <p class="muted">No job runs now.</p>
        {/if}
        {#if snap.earlier.length > 0}
          <p class="muted">
            Earlier:
            {#each snap.earlier as j, i (j.job)}{i > 0 ? ' · ' : ''}<a href={j.href}>{j.job}</a> {j.text}{/each}
            · <a href="/ui/snapshots?pipeline={encodeURIComponent(id)}">all →</a>
          </p>
        {/if}
      </section>
    </aside>
  </div>

  <section class="panel" class:stale>
    <h3>Activity on this pipeline</h3>
    <ul class="activity">
      {#each lines as e (e.key)}
        <li
          class:on={e.key === hovered}
          class={e.severity}
          onmouseenter={() => (hovered = e.key)}
          onmouseleave={() => (hovered = null)}
        >
          <span class="when">{e.when}</span>
          <span>{e.text}{#if e.who !== null}<span class="muted"> · {e.who}</span>{/if}{#if e.note !== null}<span class="muted"> "{e.note}"</span>{/if}</span>
        </li>
      {:else}
        <li class="muted">Nothing recorded about it yet.</li>
      {/each}
    </ul>
    <a href="/ui/events?object={encodeURIComponent(id)}">All its events →</a>
  </section>

  {#if acting !== null && target !== null}
    {@const v = acting}
    <Dialog
      title="{ACTIONS[v].label} {id}?"
      lines={[ACTIONS[v].effect(target)]}
      recorded={recordedAs(v, session)}
      confirm={ACTIONS[v].label}
      destructive={ACTIONS[v].destructive}
      {blocked}
      onConfirm={(note) => act(v, note)}
      onClose={() => (acting = null)}
    />
  {/if}
  {#if snapping && target !== null}
    <Dialog
      title="Snapshot tables of {id}?"
      lines={[
        `A job copies the chosen tables again from the source; ${id} restarts${target.node === null ? '' : ` on ${target.node}`} to take it, then keeps streaming.`,
        truncate
          ? 'Truncate empties each table first: lake readers see it empty or partly copied until its copy ends.'
          : 'Without truncate, lake readers see each table as it is while it copies; rows deleted at the source meanwhile stay in Delta.',
      ]}
      recorded="snapshot started by {session.name} ({session.role})"
      confirm={chosen.length === 1 ? 'Copy 1 table' : `Copy ${chosen.length} tables`}
      blocked={blocked ?? (chosen.length === 0 ? 'Choose the tables to copy.' : null)}
      onConfirm={snapshot}
      onClose={() => (snapping = false)}
    >
      <fieldset class="pick">
        <legend>Tables</legend>
        {#each tables as t (t.table)}
          <label><input type="checkbox" checked={chosen.includes(t.table)} onchange={() => pick(t.table)} /> {t.table}</label>
        {/each}
      </fieldset>
      <label><input type="checkbox" bind:checked={truncate} /> Truncate each table first</label>
    </Dialog>
  {/if}
{/if}

<style>
  h2 {
    display: flex;
    gap: 10px;
    align-items: center;
  }
  .where {
    margin: -6px 0 10px;
  }
  .error {
    color: var(--red);
  }
  .actions {
    display: flex;
    gap: 6px;
    margin-bottom: 12px;
  }
  .destructive {
    color: var(--red);
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
  .schema {
    color: var(--text-2);
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
  dd.orange {
    color: var(--orange);
  }
  dd.red {
    color: var(--red);
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
    border-radius: 4px;
  }
  .activity li.on {
    background: var(--bg);
    outline: 1px solid var(--accent);
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
  .pick {
    border: 1px solid var(--border);
    border-radius: 6px;
    max-height: 160px;
    overflow: auto;
    margin: 8px 0;
  }
  .pick label {
    display: block;
  }
</style>
