<script lang="ts">
  // The pipeline list (Admin_Console §4, mockup 05): worst first; filters by state, node, source
  // and name, and a column's sort, all in the address; ↑/↓ and Enter move and open; select
  // several to act on them together, one at a time, stopping at the first failure.
  import { onMount } from 'svelte';
  import type { Client } from '../lib/api';
  import {
    ACTIONS,
    ask,
    mayAct,
    oneAtATime,
    outcomeText,
    recordedAs,
    VERBS,
    type Verb,
  } from '../lib/actions';
  import Dialog from '../lib/Dialog.svelte';
  import { ago, clock, duration } from '../lib/format';
  import type { Router } from '../lib/router.svelte';
  import { formatValue } from '../lib/series';
  import type { Session } from '../lib/session';
  import type { Model } from '../model/model.svelte';
  import { LAG_TARGET_S } from '../overview/attention';
  import Sparkline from '../overview/Sparkline.svelte';
  import type { LagHistory } from './lag.svelte';
  import {
    counts,
    listRows,
    needingAttention,
    parseQuery,
    queryString,
    SHOWS,
    sortBy,
    view,
    type Column,
    type ListQuery,
    type ListRow,
  } from './rows';

  interface Props {
    model: Model;
    lag: LagHistory;
    session: Session;
    api: Client;
    router: Router;
    now: number;
    /** The live updates stopped: every number is as of `last`. */
    stale: boolean;
    last: number | null;
    /** Offline: actions wait. */
    offline: boolean;
  }
  let { model, lag, session, api, router, now, stale, last, offline }: Props = $props();

  const q = $derived(parseQuery(router.query));
  const all = $derived(listRows(model.facts, lag.lines, now));
  const flagged = $derived(needingAttention(model.facts, now));
  const shown = $derived(view(all, q, flagged));
  const n = $derived(counts(all, flagged));
  const nodes = $derived((model.facts.cluster?.members ?? []).map((m) => m.node).sort());
  const can = $derived(mayAct(session.role));

  let selected = $state<string[]>([]);
  let acting = $state<Verb | null>(null);

  /** The selected pipelines the filters show: the only ones an action can reach. */
  const chosen = $derived(shown.filter((r) => selected.includes(r.id)));
  const offered = $derived(VERBS.filter((v) => chosen.some((r) => ACTIONS[v].takes(r.desired))));

  const href = (id: string) => `/ui/pipelines/${encodeURIComponent(id)}`;
  const at = (l: ListQuery) => `/ui/pipelines${queryString(l)}`;
  const set = (change: Partial<ListQuery>) => router.replace(at({ ...q, ...change }));

  onMount(() => void lag.ensure());

  function toggle(id: string) {
    selected = selected.includes(id) ? selected.filter((s) => s !== id) : [...selected, id];
  }

  /** Ticks or unticks the rows shown; the selection of rows hidden by the filters is kept. */
  function toggleAll() {
    const ids = shown.map((r) => r.id);
    const every = ids.every((id) => selected.includes(id));
    selected = every
      ? selected.filter((id) => !ids.includes(id))
      : [...selected, ...ids.filter((id) => !selected.includes(id))];
  }

  /** The rows' keys and clicks: a click anywhere on a row opens it; ↑/↓ move between rows. */
  function rowsNav(table: HTMLTableElement) {
    const link = (tr: Element | null | undefined) => tr?.querySelector<HTMLAnchorElement>('a.open') ?? null;
    const click = (e: MouseEvent) => {
      const t = e.target as Element;
      if (t.closest('a, button, input, label') !== null) return;
      const a = link(t.closest('tbody tr'));
      if (a !== null) router.go(a.pathname);
    };
    const keys = (e: KeyboardEvent) => {
      if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;
      const tr = (document.activeElement as Element | null)?.closest('tbody tr');
      const next =
        tr === null || tr === undefined
          ? table.querySelector('tbody tr')
          : e.key === 'ArrowDown'
            ? tr.nextElementSibling
            : tr.previousElementSibling;
      const a = link(next);
      if (a === null) return;
      e.preventDefault();
      a.focus();
    };
    table.addEventListener('click', click);
    table.addEventListener('keydown', keys);
    return () => {
      table.removeEventListener('click', click);
      table.removeEventListener('keydown', keys);
    };
  }

  function lines(v: Verb, takes: ListRow[], not: ListRow[]): string[] {
    const out = [takes.map((r) => `${r.id} on ${r.node ?? 'no node'}`).join(' · '), ACTIONS[v].each];
    if (v === 'restart') {
      for (const r of takes.filter((r) => r.job !== null)) {
        out.push(`${r.id}'s running snapshot ${r.job} carries on from its last chunk after the restart.`);
      }
    }
    if (takes.length > 1) out.push('One at a time, in this order; a failure stops the rest and says which.');
    if (not.length > 0) {
      out.push(`Not asked, ${ACTIONS[v].label.toLowerCase()} doesn't apply: ${not.map((r) => `${r.id} (${r.desired})`).join(', ')}.`);
    }
    return out;
  }

  const plan = $derived.by(() => {
    if (acting === null) return null;
    const v = acting;
    const takes = chosen.filter((r) => ACTIONS[v].takes(r.desired));
    const not = chosen.filter((r) => !ACTIONS[v].takes(r.desired));
    const label = ACTIONS[v].label;
    return {
      verb: v,
      ids: takes.map((r) => r.id),
      title: takes.length === 1 ? `${label} ${takes[0].id}?` : `${label} ${takes.length} pipelines?`,
      lines: lines(v, takes, not),
      confirm: takes.length === 1 ? label : `${label} ${takes.length}`,
      recorded: recordedAs(v, session) + (takes.length > 1 ? ', on each pipeline' : ''),
      destructive: ACTIONS[v].destructive,
    };
  });

  async function run(note: string): Promise<string | null> {
    if (plan === null) return null;
    const { verb, ids } = plan;
    const o = await oneAtATime(ids, (id) => ask(api, verb, id, note));
    selected = selected.filter((id) => !o.done.includes(id));
    return outcomeText(o);
  }

  const asOf = $derived(last ?? 0);
  const headings: { column: Column | null; label: string }[] = [
    { column: 'pipeline', label: 'Pipeline' },
    { column: 'state', label: 'State' },
    { column: null, label: 'Source → sink' },
    { column: 'node', label: 'Node' },
    { column: 'lag', label: `Lag, 6 h · now / target` },
    { column: 'changes', label: 'Changes/min' },
    { column: 'commit', label: 'Last commit' },
    { column: null, label: 'Tables' },
    { column: null, label: 'Snapshot' },
    { column: null, label: '' },
  ];
</script>

<h2>Pipelines</h2>
{#if stale}
  <p class="notice" role="status">Not current: as of {clock(asOf, true)} · {duration((now - asOf) / 1000)} old</p>
{/if}
{#if model.read.pipelines.error !== null}
  <p class="notice failing" role="alert">Couldn't be read, shown as last read: {model.read.pipelines.error}</p>
{/if}
{#if lag.error !== null}
  <p class="notice failing" role="alert">The lag lines couldn't be read: {lag.error}</p>
{/if}

<nav class="chips" aria-label="Show">
  {#each SHOWS as s (s.id)}
    <a
      href={at({ ...q, show: s.id })}
      aria-current={q.show === s.id ? 'true' : undefined}
      onclick={(e) => (e.preventDefault(), set({ show: s.id }))}
    >
      {s.label} <b>{n[s.id]}</b>
    </a>
  {/each}
</nav>
<div class="filters">
  <label>
    Node
    <select value={q.node ?? ''} onchange={(e) => set({ node: e.currentTarget.value || null })}>
      <option value="">any</option>
      {#each nodes as node (node)}<option value={node}>{node}</option>{/each}
    </select>
  </label>
  <label>
    Source
    <select
      value={q.source ?? ''}
      onchange={(e) => set({ source: (e.currentTarget.value || null) as ListQuery['source'] })}
    >
      <option value="">any</option>
      <option value="postgres">PostgreSQL</option>
      <option value="mysql">MySQL</option>
    </select>
  </label>
  <input
    type="search"
    aria-label="Filter by name"
    placeholder="filter by name…"
    value={q.text}
    oninput={(e) => set({ text: e.currentTarget.value })}
  />
</div>

{#if can && chosen.length > 0}
  <div class="acting" role="toolbar" aria-label="Act on the selected pipelines">
    <b>{chosen.length} selected</b> ·
    {#each offered as v (v)}
      <button type="button" class:destructive={ACTIONS[v].destructive} disabled={offline} onclick={() => (acting = v)}>
        {ACTIONS[v].label}…
      </button>
    {/each}
    <span class="muted">Only actions your role allows appear.</span>
    <button type="button" class="link" onclick={() => (selected = [])}>Clear</button>
  </div>
{/if}

<table class:stale {@attach rowsNav}>
  <thead>
    <tr>
      {#if can}
        <th>
          <input
            type="checkbox"
            aria-label="Select every pipeline shown"
            checked={shown.length > 0 && shown.every((r) => selected.includes(r.id))}
            onchange={toggleAll}
          />
        </th>
      {/if}
      {#each headings as h (h.label)}
        <th aria-sort={h.column !== null && q.sort === h.column ? (q.desc ? 'descending' : 'ascending') : undefined}>
          {#if h.column !== null}
            {@const column = h.column}
            <a href={at(sortBy(q, column))} onclick={(e) => (e.preventDefault(), set(sortBy(q, column)))}>
              {h.label}
            </a>
          {:else}{h.label}{/if}
        </th>
      {/each}
    </tr>
  </thead>
  <tbody>
    {#each shown as r (r.id)}
      <tr class:selected={selected.includes(r.id)}>
        {#if can}
          <td><input type="checkbox" aria-label="Select {r.id}" checked={selected.includes(r.id)} onchange={() => toggle(r.id)} /></td>
        {/if}
        <td><a class="open" href={href(r.id)}>{r.id}</a></td>
        <td><span class="chip {r.state.tone}">{r.state.label}</span></td>
        <td class="muted">{r.route}</td>
        <td>{r.node ?? '—'}{#if r.away}{' '}<span class="away" title="held away from its ring owner">away</span>{/if}</td>
        <td class="lag">
          <Sparkline points={r.points} target={LAG_TARGET_S} />
          {r.lag === null ? '—' : `${formatValue('seconds', r.lag)} / ${LAG_TARGET_S} s`}
        </td>
        <td>{r.changes === null ? '—' : r.changes}</td>
        <td>{r.lastCommit === null ? '—' : ago(r.lastCommit, now)}</td>
        <td>{r.halted > 0 ? `${r.tables - r.halted} of ${r.tables}` : r.tables}</td>
        <td>{r.snapshot ?? '—'}</td>
        <td class="note muted">{r.state.note ?? ''}</td>
      </tr>
    {:else}
      <tr><td colspan="11" class="muted">No pipeline shown: change the filters.</td></tr>
    {/each}
  </tbody>
</table>
<p class="muted foot">
  Order: red, then orange, then yellow, then by lag against the target. Click a column to sort by
  it. The filters and sort stay in the address, so a link shows the same view. "away" = held away
  from its ring owner.
</p>

{#if plan !== null}
  <Dialog
    title={plan.title}
    lines={plan.lines}
    recorded={plan.recorded}
    confirm={plan.confirm}
    destructive={plan.destructive}
    blocked={offline
      ? 'Offline: actions wait until the console is live again.'
      : plan.ids.length === 0
        ? `None of the selected pipelines takes ${ACTIONS[plan.verb].label.toLowerCase()}.`
        : null}
    onConfirm={run}
    onClose={() => (acting = null)}
  />
{/if}

<style>
  .chips {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-bottom: 8px;
  }
  .chips a {
    padding: 2px 10px;
    border: 1px solid var(--border);
    border-radius: 12px;
    color: var(--text-2);
    text-decoration: none;
  }
  .chips a[aria-current='true'] {
    color: var(--text);
    border-color: var(--accent);
    font-weight: 600;
  }
  .filters {
    display: flex;
    gap: 12px;
    align-items: center;
    margin-bottom: 8px;
    color: var(--text-2);
  }
  .acting {
    display: flex;
    gap: 6px;
    align-items: center;
    padding: 6px 10px;
    margin-bottom: 8px;
    background: var(--bg-panel);
    border: 1px solid var(--accent);
    border-radius: 8px;
  }
  .destructive {
    color: var(--red);
  }
  .link {
    border: none;
    background: none;
    color: var(--accent);
  }
  table {
    width: 100%;
    border-collapse: collapse;
    background: var(--bg-panel);
  }
  th,
  td {
    text-align: left;
    padding: 6px 8px;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  th a {
    color: var(--text-2);
    text-decoration: none;
  }
  tbody tr {
    cursor: pointer;
  }
  tbody tr:hover,
  tr.selected {
    background: var(--bg);
  }
  .away {
    color: var(--orange);
  }
  .note {
    white-space: normal;
  }
  .stale {
    opacity: 0.55;
  }
  .chip {
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
  .foot {
    margin-top: 8px;
  }
</style>
