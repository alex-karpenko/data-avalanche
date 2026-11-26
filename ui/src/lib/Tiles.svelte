<script lang="ts">
  // A page's headline tiles (mockups 01 and 03): each a key, a value and a line under it, its
  // value coloured by its tone and its tone said in words too.
  import type { Tile } from '../overview/derive';

  let { tiles, stale = false }: { tiles: Tile[]; stale?: boolean } = $props();
</script>

<div class="tiles" class:stale>
  {#each tiles as t (t.key)}
    <div class="tile {t.tone}">
      <div class="k">{t.key}</div>
      <div class="v">{t.value}</div>
      <div class="s">{t.detail}</div>
    </div>
  {/each}
</div>

<style>
  .tiles {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 10px;
  }
  .stale {
    opacity: 0.55;
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
</style>
