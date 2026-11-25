<script lang="ts">
  // A section whose page isn't built yet (M16 builds them).
  import { SECTIONS, type Section } from './lib/router.svelte';

  let { section }: { section: Section } = $props();
  const label = $derived(SECTIONS.find((s) => s.id === section)?.label ?? '');
</script>

<h2>{label}</h2>
<p class="muted">This page isn't in this build yet.</p>
