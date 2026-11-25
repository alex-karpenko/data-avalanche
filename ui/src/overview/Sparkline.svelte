<script lang="ts">
  // A pipeline's lag over the last hour, as a line; the target dashed once the lag reached it.
  let { points, target }: { points: [number, number][]; target: number } = $props();

  const W = 120;
  const H = 22;
  const line = $derived.by(() => {
    if (points.length < 2) return null;
    const t0 = points[0][0];
    const span = Math.max(1, points[points.length - 1][0] - t0);
    const top = Math.max(...points.map(([, v]) => v), 1);
    const y = (v: number) => (H - 2 - (v / top) * (H - 4)).toFixed(1);
    return {
      points: points.map(([t, v]) => `${(((t - t0) / span) * W).toFixed(1)},${y(v)}`).join(' '),
      target: top >= target ? y(target) : null,
      over: points[points.length - 1][1] > target,
    };
  });
</script>

{#if line === null}
  <span class="muted">—</span>
{:else}
  <svg width={W} height={H} viewBox="0 0 {W} {H}" aria-hidden="true">
    {#if line.target !== null}
      <line class="target" x1="0" x2={W} y1={line.target} y2={line.target} />
    {/if}
    <polyline class:over={line.over} points={line.points} />
  </svg>
{/if}

<style>
  svg {
    vertical-align: middle;
  }
  polyline {
    fill: none;
    stroke: var(--green);
    stroke-width: 1.5;
  }
  polyline.over {
    stroke: var(--orange);
  }
  .target {
    stroke: var(--grey);
    stroke-dasharray: 3 3;
  }
</style>
