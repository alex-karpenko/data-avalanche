// The console's time series (Admin_Console §4.2): what a chart draws beside its line, worked out
// apart from the drawing so it can be tested. Times are unix seconds, as `/v1/history` gives them.

import { bytes, clock, duration } from './format';

/** The units `/v1/history`'s series are in. */
export type Unit = 'seconds' | 'bytes' | 'rows/s' | 'changes/min' | 'files' | 'cores';

/** An event drawn on a chart: a thin dotted line at its time, with a short label. */
export interface Mark {
  /** The event's key: hovering it here highlights it in the page's list, and the reverse. */
  key: string;
  at: number;
  label: string;
}

/** A mark placed on the chart: `x` in the plot's CSS pixels, `row` its label's line from the top. */
export interface Placed extends Mark {
  x: number;
  row: number;
}

/** The lines labels stack on before they overlap. */
export const LABEL_ROWS = 3;
/** A label's width: its characters at the page's small text, and its padding. */
export const CHAR_PX = 6.5;
export const LABEL_PAD_PX = 10;
/** How near the pointer must be to a mark's line to hover it. */
export const HOVER_PX = 4;

/** The marks between `since` and `until`, oldest first, each label on the first line where it
 * doesn't overlap the label before it; past `LABEL_ROWS` lines, on the last. */
export function placeLabels(
  marks: Mark[],
  x: (at: number) => number,
  since: number,
  until: number,
): Placed[] {
  const ends: number[] = Array.from({ length: LABEL_ROWS }, () => -Infinity);
  return marks
    .filter((m) => m.at >= since && m.at <= until)
    .sort((a, b) => a.at - b.at)
    .map((m) => {
      const px = x(m.at);
      const free = ends.findIndex((end) => end <= px);
      const row = free === -1 ? LABEL_ROWS - 1 : free;
      ends[row] = px + m.label.length * CHAR_PX + LABEL_PAD_PX;
      return { ...m, x: px, row };
    });
}

/** The mark whose line is within `HOVER_PX` of `px`, the nearest; `null` for none. */
export function markNear(placed: Placed[], px: number): string | null {
  let best: Placed | null = null;
  for (const p of placed) {
    const d = Math.abs(p.x - px);
    if (d <= HOVER_PX && (best === null || d < Math.abs(best.x - px))) best = p;
  }
  return best?.key ?? null;
}

/** A number in a few characters: `860`, `41 k`, `2.6 M`. */
export function compact(n: number): string {
  const a = Math.abs(n);
  if (a < 1000) return String(Math.round(n));
  if (a < 1e6) return `${Math.round(n / 1000)} k`;
  return `${(n / 1e6).toFixed(1).replace(/\.0$/, '')} M`;
}

/** A value in its unit, as an axis and the readout write it. */
export function formatValue(unit: Unit, v: number): string {
  switch (unit) {
    case 'seconds':
      return v < 120 ? `${Math.round(v)} s` : duration(v);
    case 'bytes':
      return bytes(v);
    case 'rows/s':
      return `${compact(v)}/s`;
    case 'changes/min':
      return `${compact(v)}/min`;
    case 'files':
      return compact(v);
    case 'cores':
      return v.toFixed(1);
  }
}

/** What the crosshair reads at a point: `13:41:05 · 4 s`. */
export function readout(at: number, v: number | null, unit: Unit): string {
  return `${clock(at * 1000, true)} · ${v === null ? 'no sample' : formatValue(unit, v)}`;
}

/** The part of a drawing context the overlay draws with. */
export type Pen = Pick<
  CanvasRenderingContext2D,
  'save' | 'restore' | 'beginPath' | 'moveTo' | 'lineTo' | 'stroke' | 'setLineDash' | 'lineWidth' | 'strokeStyle'
>;

/** The plot's area, in canvas pixels. */
export interface Area {
  left: number;
  top: number;
  width: number;
  height: number;
}

/** The colours the overlay draws in, read from the page's theme. */
export interface Ink {
  target: string;
  mark: string;
  hovered: string;
}

/** Draws the target as a dashed line across the area at `targetY`, and each mark as a dotted
 * line down it at its `x` (canvas pixels); the hovered one wider, in its own colour. */
export function drawOverlay(
  pen: Pen,
  area: Area,
  targetY: number | null,
  marks: { key: string; x: number }[],
  hovered: string | null,
  ink: Ink,
  ratio: number,
): void {
  pen.save();
  if (targetY !== null) {
    pen.setLineDash([6 * ratio, 4 * ratio]);
    pen.lineWidth = ratio;
    pen.strokeStyle = ink.target;
    pen.beginPath();
    pen.moveTo(area.left, targetY);
    pen.lineTo(area.left + area.width, targetY);
    pen.stroke();
  }
  for (const m of marks) {
    const on = m.key === hovered;
    pen.setLineDash([2 * ratio, 3 * ratio]);
    pen.lineWidth = (on ? 2 : 1) * ratio;
    pen.strokeStyle = on ? ink.hovered : ink.mark;
    pen.beginPath();
    pen.moveTo(m.x, area.top);
    pen.lineTo(m.x, area.top + area.height);
    pen.stroke();
  }
  pen.restore();
}
