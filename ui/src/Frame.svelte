<script lang="ts">
  // The top bar every page shares (Admin_Console §4; mockups 01 and 11): the sections, whether
  // the page is live, the config generation, and who is signed in with their role.
  import type { Snippet } from 'svelte';
  import { clock } from './lib/format';
  import type { State } from './lib/live.svelte';
  import { SECTIONS, type Section } from './lib/router.svelte';
  import type { Session } from './lib/session';

  interface Props {
    session: Session;
    section: Section;
    generation: number | null;
    state: State;
    /** When the last message came (ms); `null` before the first. */
    last: number | null;
    now: number;
    onRetry: () => void;
    children?: Snippet;
  }
  let { session, section, generation, state, last, now, onRetry, children }: Props = $props();

  const connection = $derived.by(() => {
    if (state === 'reconnecting' && last !== null) {
      return `reconnecting · last update ${Math.floor((now - last) / 1000)} s ago`;
    }
    return state;
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
    <span class="conn {state}" role="status">{connection}</span>
    {#if generation !== null}<span>config gen {generation}</span>{/if}
    <span>{session.name}</span>
    <b>{session.role}</b>
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
{#if state === 'offline'}
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
