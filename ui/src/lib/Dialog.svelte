<script lang="ts">
  // The one dialog every action opens (Admin_Console §5, mockups 05, 06): what it does, to what,
  // and its effect; an optional note; the line it will be recorded as. It stays open, saying why,
  // when the action fails.
  import { onMount, type Snippet } from 'svelte';
  import { NOTE_MAX } from './actions';

  interface Props {
    title: string;
    /** What it does and its effect, a paragraph each. */
    lines: string[];
    /** `Recorded as: …`, on each pipeline when there are several. */
    recorded: string;
    confirm: string;
    /** An action that stops something, styled as such. */
    destructive?: boolean;
    /** Why it can't be confirmed now (offline, nothing chosen); `null` when it can. */
    blocked?: string | null;
    /** Carries the action out: `null` when it's done, else why it failed. */
    onConfirm: (note: string) => Promise<string | null>;
    onClose: () => void;
    /** The action's own fields, above the note. */
    children?: Snippet;
  }
  let {
    title,
    lines,
    recorded,
    confirm,
    destructive = false,
    blocked = null,
    onConfirm,
    onClose,
    children,
  }: Props = $props();

  let note = $state('');
  let busy = $state(false);
  let failed = $state<string | null>(null);
  let field: HTMLTextAreaElement;

  onMount(() => field.focus());

  async function go() {
    busy = true;
    failed = null;
    let why: string | null;
    try {
      why = await onConfirm(note);
    } catch (e) {
      why = e instanceof Error ? e.message : String(e);
    } finally {
      busy = false;
    }
    if (why === null) onClose();
    else failed = why;
  }

  function key(e: KeyboardEvent) {
    if (e.key === 'Escape' && !busy) onClose();
  }
</script>

<svelte:window onkeydown={key} />
<div class="scrim">
  <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="dialog-title">
    <h3 id="dialog-title">{title}</h3>
    {#each lines as l (l)}<p>{l}</p>{/each}
    {@render children?.()}
    <label>
      Note (optional, kept with the event)
      <textarea bind:this={field} bind:value={note} maxlength={NOTE_MAX} rows="2"></textarea>
    </label>
    <p class="muted">Recorded as: {recorded}</p>
    {#if failed !== null}<p class="failed" role="alert">{failed}</p>{/if}
    {#if blocked !== null}<p class="muted">{blocked}</p>{/if}
    <div class="buttons">
      <button type="button" onclick={onClose} disabled={busy}>Cancel</button>
      <button type="button" class="go" class:destructive onclick={go} disabled={busy || blocked !== null}>
        {busy ? `${confirm}…` : confirm}
      </button>
    </div>
  </div>
</div>

<style>
  .scrim {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.35);
    display: flex;
    align-items: flex-start;
    justify-content: center;
    padding-top: 10vh;
    z-index: 10;
  }
  .dialog {
    width: min(520px, 92vw);
    background: var(--bg-panel);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px 18px;
  }
  h3 {
    margin: 0 0 8px;
    font-size: 15px;
  }
  label {
    display: block;
    margin-top: 10px;
    color: var(--text-2);
  }
  textarea {
    display: block;
    width: 100%;
    margin-top: 4px;
    font: inherit;
    color: var(--text);
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 4px 6px;
  }
  .failed {
    color: var(--red);
  }
  .buttons {
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    margin-top: 12px;
  }
  .go {
    background: var(--accent);
    border-color: var(--accent);
    color: #fff;
  }
  .go.destructive {
    background: var(--red);
    border-color: var(--red);
  }
</style>
