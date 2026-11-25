<script lang="ts">
  // A time series as Admin_Console §4.2 draws every one: axes with units and times, the target as
  // a dashed line, events as dotted lines with short labels stacked when close, a crosshair with
  // the time and value at the pointer. Hovering a line or a label hovers its event, which the page
  // highlights in its list too, and the reverse. uPlot is loaded with the first chart, so a page
  // without one never loads it.
  import { onMount } from 'svelte';
  import type uPlot from 'uplot';
  import 'uplot/dist/uPlot.min.css';
  import {
    drawOverlay,
    formatValue,
    markNear,
    placeLabels,
    readout,
    type Mark,
    type Placed,
    type Unit,
  } from './series';

  interface Props {
    label: string;
    unit: Unit;
    /** `[unix seconds, value]`, oldest first. */
    points: [number, number][];
    since: number;
    until: number;
    target?: number | null;
    marks?: Mark[];
    /** The event hovered, here or in the page's list. */
    hovered?: string | null;
    onHover?: (key: string | null) => void;
    height?: number;
  }
  let {
    label,
    unit,
    points,
    since,
    until,
    target = null,
    marks = [],
    hovered = null,
    onHover = () => {},
    height = 180,
  }: Props = $props();

  let box: HTMLDivElement;
  let plot: uPlot | null = null;
  let placed = $state<Placed[]>([]);
  /** Where the plot's area starts in the box (CSS pixels): the labels sit above it. */
  let left = $state(0);
  let at = $state<string | null>(null);

  const data = $derived([points.map(([t]) => t), points.map(([, v]) => v)] as uPlot.AlignedData);
  const now = $derived(points.length > 0 ? formatValue(unit, points[points.length - 1][1]) : null);

  function ink() {
    const css = getComputedStyle(box);
    return {
      target: css.getPropertyValue('--grey').trim() || 'grey',
      mark: css.getPropertyValue('--text-2').trim() || 'grey',
      hovered: css.getPropertyValue('--accent').trim() || 'blue',
    };
  }

  function draw(u: uPlot) {
    const ratio = devicePixelRatio || 1;
    const p = placeLabels(marks, (t) => u.valToPos(t, 'x'), since, until);
    placed = p;
    left = u.bbox.left / ratio;
    drawOverlay(
      u.ctx,
      u.bbox,
      target === null ? null : u.valToPos(target, 'y', true),
      p.map((m) => ({ key: m.key, x: u.valToPos(m.at, 'x', true) })),
      hovered,
      ink(),
      ratio,
    );
  }

  function cursor(u: uPlot) {
    const idx = u.cursor.idx;
    const x = u.cursor.left;
    if (idx === null || idx === undefined || x === undefined || x < 0) {
      at = null;
      onHover(null);
      return;
    }
    at = readout(u.data[0][idx], u.data[1][idx] ?? null, unit);
    onHover(markNear(placed, x));
  }

  onMount(() => {
    let gone = false;
    let resize: ResizeObserver | undefined;
    void import('uplot').then(({ default: UPlot }) => {
      if (gone) return;
      plot = new UPlot(
        {
          width: box.clientWidth || 600,
          height,
          legend: { show: false },
          scales: { x: { time: true, min: since, max: until }, y: { range: (_u, _min, max) => [0, Math.max(max, target ?? 0) * 1.1 || 1] } },
          axes: [{}, { values: (_u, ticks) => ticks.map((v) => formatValue(unit, v)), size: 60 }],
          series: [{}, { label, stroke: ink().hovered, width: 1.5, points: { show: false } }],
          hooks: { draw: [draw], setCursor: [cursor] },
        },
        data,
        box,
      );
      if (typeof ResizeObserver !== 'undefined') {
        resize = new ResizeObserver(() => plot?.setSize({ width: box.clientWidth, height }));
        resize.observe(box);
      }
    });
    return () => {
      gone = true;
      resize?.disconnect();
      plot?.destroy();
      plot = null;
    };
  });

  // New samples, a new range or a new target: the y axis is ranged again too (setData resets the
  // scales, so a spike is not clipped by the old range).
  $effect(() => {
    // Read before the plot is checked: the first run comes before uPlot has loaded, and a
    // dependency read only after `plot?.` would never be tracked.
    const samples = data;
    void target;
    void since;
    void until;
    plot?.setData(samples, true);
    plot?.setScale('x', { min: since, max: until });
  });
  $effect(() => {
    void hovered;
    void marks;
    plot?.redraw(false);
  });
</script>

<figure class="chart">
  <figcaption>
    <span>{label}</span>
    {#if at !== null}<span class="at">{at}</span>{:else if now !== null}<span class="at">now {now}</span>{/if}
  </figcaption>
  <div class="labels" style:left="{left}px">
    {#each placed as m (m.key)}
      <button
        type="button"
        class="label"
        class:on={m.key === hovered}
        style:left="{m.x}px"
        style:top="{m.row * 16}px"
        onmouseenter={() => onHover(m.key)}
        onmouseleave={() => onHover(null)}
        onfocus={() => onHover(m.key)}
        onblur={() => onHover(null)}>{m.label}</button
      >
    {/each}
  </div>
  <div class="plot" bind:this={box}></div>
  {#if points.length === 0}
    <p class="empty muted">No samples in this range yet.</p>
  {/if}
</figure>

<style>
  .chart {
    position: relative;
    margin: 0;
  }
  figcaption {
    display: flex;
    justify-content: space-between;
    color: var(--text-2);
    margin-bottom: 4px;
  }
  .at {
    font-variant-numeric: tabular-nums;
    color: var(--text);
  }
  .labels {
    position: relative;
    height: 48px;
  }
  .label {
    position: absolute;
    padding: 0 4px;
    border: none;
    background: none;
    color: var(--text-2);
    font-size: 11px;
    white-space: nowrap;
  }
  .label.on {
    color: var(--accent);
    font-weight: 600;
  }
  .empty {
    position: absolute;
    top: 50%;
    width: 100%;
    text-align: center;
  }
  .plot :global(.u-axis) {
    color: var(--text-2);
  }
</style>
