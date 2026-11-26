import { flushSync, mount, tick, unmount } from 'svelte';
import { SvelteMap } from 'svelte/reactivity';
import { afterEach, describe, expect, it, vi } from 'vitest';
import Chart from './Chart.svelte';

/** uPlot as far as the chart uses it: a second a pixel from the range's start. */
interface Fake {
  data: number[][];
  destroyed: boolean;
  /** The scale calls the chart made, in order: `setData` with its reset flag, `setScale` with its range. */
  log: unknown[][];
  hover(idx: number | null, left: number): void;
}
const made: Fake[] = [];
vi.mock('uplot', () => ({
  default: class {
    bbox = { left: 120, top: 0, width: 1200, height: 300 };
    ctx = { save() {}, restore() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() {}, setLineDash() {}, lineWidth: 0, strokeStyle: '' };
    cursor: { idx: number | null; left: number } = { idx: null, left: -10 };
    data: number[][];
    destroyed = false;
    log: unknown[][] = [];
    #opts: { scales: { x: { min: number } }; hooks: { draw: ((u: unknown) => void)[]; setCursor: ((u: unknown) => void)[] } };
    constructor(opts: never, data: number[][]) {
      this.data = data;
      this.#opts = opts;
      made.push(this);
      this.redraw();
    }
    valToPos(v: number, axis: string) {
      return axis === 'x' ? v - this.#opts.scales.x.min : 0;
    }
    setData(data: number[][], reset: boolean) {
      this.data = data;
      this.log.push(['setData', data, reset]);
    }
    setScale(key: string, opts: unknown) {
      this.log.push(['setScale', key, opts]);
    }
    setSize() {}
    redraw() {
      for (const h of this.#opts.hooks.draw) h(this);
    }
    hover(idx: number, left: number) {
      this.cursor = { idx, left };
      for (const h of this.#opts.hooks.setCursor) h(this);
    }
    destroy() {
      this.destroyed = true;
    }
  },
}));

let mounted: ReturnType<typeof mount> | undefined;
afterEach(() => {
  if (mounted !== undefined) unmount(mounted);
  mounted = undefined;
  made.length = 0;
});

const points: [number, number][] = [
  [1000, 2],
  [1060, 4],
  [1120, 94],
];
const marks = [
  { key: 'e1', at: 1030, label: 'restarted' },
  { key: 'e2', at: 1100, label: 'failover' },
];

async function render(props: Record<string, unknown>): Promise<HTMLElement> {
  const target = document.createElement('div');
  document.body.replaceChildren(target);
  mounted = mount(Chart, { target, props: { label: 'Lag', unit: 'seconds', points, since: 1000, until: 1200, ...props } as never });
  // uPlot is loaded on its own; the chart is drawn once it is.
  await vi.dynamicImportSettled();
  await tick();
  flushSync();
  return target;
}

describe('a chart', () => {
  it('draws the series over its range, its value now, and its events as labels', async () => {
    const t = await render({ target: 60, marks });
    expect(made[0].data).toEqual([
      [1000, 1060, 1120],
      [2, 4, 94],
    ]);
    expect(t.querySelector('.at')?.textContent).toBe('now 94 s');
    expect([...t.querySelectorAll('.label')].map((l) => l.textContent)).toEqual(['restarted', 'failover']);
  });

  it('says so when the range has no samples yet', async () => {
    const t = await render({ points: [] });
    expect(t.textContent).toContain('No samples in this range yet.');
  });

  it("hovers an event from its label, and shows one hovered in the page's list", async () => {
    const hovered: (string | null)[] = [];
    const t = await render({ marks, hovered: 'e2', onHover: (k: string | null) => hovered.push(k) });
    const labels = t.querySelectorAll('.label');
    expect(labels[1].classList.contains('on')).toBe(true);
    labels[0].dispatchEvent(new MouseEvent('mouseenter'));
    labels[0].dispatchEvent(new MouseEvent('mouseleave'));
    expect(hovered).toEqual(['e1', null]);
  });

  it('reads the time and value at the pointer, and hovers the event whose line is under it', async () => {
    const hovered: (string | null)[] = [];
    const t = await render({ marks, onHover: (k: string | null) => hovered.push(k) });
    // Over the second sample, 2 px from the first event's line (at 30 px).
    made[0].hover(1, 32);
    flushSync();
    expect(t.querySelector('.at')?.textContent).toMatch(/^\d\d:\d\d:\d\d · 4 s$/);
    made[0].hover(2, 80);
    expect(hovered).toEqual(['e1', null]);
  });

  it('clears the hover when the pointer leaves the plot', async () => {
    const hovered: (string | null)[] = [];
    await render({ marks, onHover: (k: string | null) => hovered.push(k) });
    made[0].hover(1, 32);
    made[0].hover(null, -10);
    expect(hovered).toEqual(['e1', null]);
  });

  it('ranges the y axis again when new samples come, and when the target changes', async () => {
    const data = new SvelteMap<string, unknown>([
      ['points', points],
      ['target', null],
    ]);
    const target = document.createElement('div');
    document.body.replaceChildren(target);
    mounted = mount(Chart, {
      target,
      props: {
        label: 'Lag',
        unit: 'seconds',
        since: 1000,
        until: 1200,
        get points() {
          return data.get('points');
        },
        get target() {
          return data.get('target');
        },
      } as never,
    });
    await vi.dynamicImportSettled();
    await tick();
    flushSync();
    made[0].log.length = 0;

    const next: [number, number][] = [...points, [1180, 5]];
    data.set('points', next);
    flushSync();
    expect(made[0].log).toEqual([
      ['setData', [[1000, 1060, 1120, 1180], [2, 4, 94, 5]], true],
      ['setScale', 'x', { min: 1000, max: 1200 }],
    ]);

    made[0].log.length = 0;
    data.set('target', 200);
    flushSync();
    expect(made[0].log).toEqual([
      ['setData', [[1000, 1060, 1120, 1180], [2, 4, 94, 5]], true],
      ['setScale', 'x', { min: 1000, max: 1200 }],
    ]);
  });

  it('lets uPlot go when it goes', async () => {
    await render({});
    unmount(mounted!);
    mounted = undefined;
    expect(made[0].destroyed).toBe(true);
  });
});
