<script lang="ts">
  // The top bar every page shares (Admin_Console §4; mockups 01 and 11): the sections, whether
  // the page is live, the config generation, and who is signed in with their role; the user menu
  // holds the console's own status.
  import type { Snippet } from 'svelte';
  import { clock } from './lib/format';
  import type { State } from './lib/live.svelte';
  import { SECTIONS, type Section } from './lib/router.svelte';
  import type { Session } from './lib/session';
  import type { StatusRow } from './lib/status';

  interface Props {
    session: Session;
    section: Section;
    generation: number | null;
    state: State;
    /** When the last message came (ms); `null` before the first. */
    last: number | null;
    now: number;
    onRetry: () => void;
    /** The console's status, shown from the user menu. */
    status: StatusRow[];
    /** The user menu opened: what the status reads is read again. */
    onStatus: () => void;
    children?: Snippet;
  }
  let { session, section, generation, state: conn, last, now, onRetry, status, onStatus, children }: Props = $props();

  /** The user menu is open: its status is drawn only then. */
  let open = $state(false);

  const connection = $derived.by(() => {
    if (conn === 'reconnecting' && last !== null) {
      return `reconnecting · last update ${Math.floor((now - last) / 1000)} s ago`;
    }
    return conn;
  });
</script>

<header class="bar">
  <a class="brand" href="/ui/">data-avalanche</a>
  <nav aria-label="Sections">
    {#each SECTIONS as s (s.id)}
      <a href={s.path} aria-current={s.id === section ? 'page' : undefined}>{s.label}</a>
    {/each}
  </nav>
  <div class="who">
    <span class="conn {conn}" role="status">{connection}</span>
    {#if generation !== null}<span>config gen {generation}</span>{/if}
    <details class="menu" bind:open ontoggle={() => open && onStatus()}>
      <summary>{session.name} <b>{session.role}</b></summary>
      {#if open}
        <div class="status">
          <h3>Console status</h3>
          <table>
            <tbody>
              {#each status as r (r.label)}
                <tr>
                  <td>{r.label}</td>
                  <td class={r.tone === 'muted' || r.tone === 'red' ? r.tone : undefined}>
                    {#if r.chip !== null}<span class="chip {r.tone}">{r.chip}</span>{/if}
                    {r.text}
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    </details>
    {#if session.csrf !== null}
      <form method="post" action="/ui/sign-out"><button type="submit">Sign out</button></form>
    {/if}
  </div>
</header>
{#if session.auth === 'none'}
  <div class="banner" role="alert">
    This cluster has no authentication: everyone who reaches the console is admin.
  </div>
{/if}
{#if conn === 'offline'}
  <div class="down" role="alert">
    {#if last !== null}Offline since {clock(last, true)} — everything below is as of then.{:else}Offline:
      the console can't reach the cluster's live updates.{/if}
    <button type="button" onclick={onRetry}>Retry now</button>
  </div>
{/if}
<main>{@render children?.()}</main>

<style>
  .bar {
    display: flex;
    align-items: center;
    gap: 18px;
    padding: 10px 16px;
    background: var(--bg-bar);
    border-bottom: 1px solid var(--border);
  }
  .brand {
    font-weight: 600;
    font-size: 14px;
    color: var(--text);
    text-decoration: none;
  }
  nav a {
    margin-right: 14px;
    color: var(--text-2);
    text-decoration: none;
  }
  nav a[aria-current='page'] {
    color: var(--text);
    font-weight: 600;
    border-bottom: 2px solid var(--accent);
    padding-bottom: 2px;
  }
  .who {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 8px;
    color: var(--text-2);
  }
  .who b {
    color: var(--text);
  }
  .conn::before {
    content: '';
    display: inline-block;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    margin-right: 5px;
    background: var(--grey);
  }
  .conn.live::before {
    background: var(--green);
  }
  .conn.reconnecting::before {
    background: var(--orange);
  }
  .conn.offline::before {
    background: var(--red);
  }
  form {
    display: inline;
  }
  .menu {
    position: relative;
  }
  .menu summary {
    cursor: pointer;
    list-style: none;
  }
  .menu summary b {
    color: var(--text);
  }
  .status {
    position: absolute;
    right: 0;
    top: calc(100% + 8px);
    z-index: 10;
    min-width: 340px;
    padding: 10px 12px;
    background: var(--bg-panel);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
  }
  .status h3 {
    margin: 0 0 6px;
    font-size: 13px;
  }
  .status td {
    padding: 3px 8px 3px 0;
    vertical-align: top;
  }
  .status td:first-child {
    color: var(--text-2);
    white-space: nowrap;
  }
  .status td.red {
    color: var(--red);
  }
  .status td.muted {
    color: var(--text-2);
  }
  .chip {
    display: inline-block;
    padding: 0 6px;
    border-radius: 8px;
    background: var(--grey-bg);
  }
  .chip.ok {
    background: var(--green-bg);
  }
  .chip.yellow {
    background: var(--yellow-bg);
  }
  .chip.red {
    background: var(--red-bg);
  }
  .banner,
  .down {
    padding: 8px 16px;
    border-bottom: 1px solid var(--border);
  }
  .banner {
    background: var(--orange-bg);
  }
  .down {
    background: var(--red-bg);
  }
  main {
    padding: 16px;
  }
</style>
