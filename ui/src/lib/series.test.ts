import { describe, expect, it } from 'vitest';
import {
  CHAR_PX,
  compact,
  drawOverlay,
  formatValue,
  LABEL_PAD_PX,
  LABEL_ROWS,
  markNear,
  placeLabels,
  type Pen,
} from './series';

const x = (t: number) => t; // a second a pixel

describe("a chart's event labels", () => {
  it('stack on the next line when they would overlap, and keep to the range', () => {
    const wide = 'failover'.length * CHAR_PX + LABEL_PAD_PX;
    const placed = placeLabels(
      [
        { key: 'c', at: 120, label: 'snapshot' },
        { key: 'a', at: 100, label: 'failover' },
        { key: 'd', at: 100 + wide, label: 'restarted' },
        { key: 'early', at: 10, label: 'before the range' },
      ],
      x,
      50,
      1000,
    );
    expect(placed.map((p) => [p.key, p.x, p.row])).toEqual([
      ['a', 100, 0],
      ['c', 120, 1],
      ['d', 100 + wide, 0],
    ]);
  });

  it(`go on the last of ${LABEL_ROWS} lines once every line is taken`, () => {
    const marks = Array.from({ length: LABEL_ROWS + 2 }, (_, i) => ({ key: `m${i}`, at: 100 + i, label: 'halted' }));
    expect(placeLabels(marks, x, 0, 1000).map((p) => p.row)).toEqual([0, 1, 2, 2, 2]);
  });

  it('are hovered when the pointer is within 4 px of their line, the nearest first', () => {
    const placed = placeLabels(
      [
        { key: 'a', at: 100, label: 'a' },
        { key: 'b', at: 106, label: 'b' },
      ],
      x,
      0,
      1000,
    );
    expect(markNear(placed, 103)).toBe('a');
    expect(markNear(placed, 104)).toBe('b');
    expect(markNear(placed, 111)).toBeNull();
  });
});

describe("a chart's values", () => {
  it('are written in their units, short', () => {
    expect(formatValue('seconds', 94)).toBe('94 s');
    expect(formatValue('seconds', 252)).toBe('4m 12s');
    expect(formatValue('bytes', 210 * 1024 ** 2)).toBe('210 MB');
    expect(formatValue('rows/s', 41_200)).toBe('41 k/s');
    expect(formatValue('changes/min', 2_600_000)).toBe('2.6 M/min');
    expect(formatValue('files', 14)).toBe('14');
    expect(formatValue('cores', 0.62)).toBe('0.6');
    expect(compact(1_000_000)).toBe('1 M');
  });
});

describe("a chart's overlay", () => {
  function pen(): Pen & { lines: string[] } {
    const lines: string[] = [];
    let from = '';
    let dash: number[] = [];
    const p = {
      lines,
      lineWidth: 0,
      strokeStyle: '',
      save() {},
      restore() {},
      beginPath() {},
      setLineDash(d: number[]) {
        dash = d;
      },
      moveTo(x: number, y: number) {
        from = `${x},${y}`;
      },
      lineTo(x: number, y: number) {
        lines.push(`${from}→${x},${y} ${p.strokeStyle} w${p.lineWidth} [${dash.join(' ')}]`);
      },
      stroke() {},
    };
    return p;
  }

  it('draws the target dashed across, and each event dotted down, the hovered one wider', () => {
    const p = pen();
    const ink = { target: 'grey', mark: 'dim', hovered: 'blue' };
    drawOverlay(p, { left: 10, top: 5, width: 100, height: 50 }, 30, [{ key: 'a', x: 40 }, { key: 'b', x: 70 }], 'b', ink, 2);
    expect(p.lines).toEqual([
      '10,30→110,30 grey w2 [12 8]',
      '40,5→40,55 dim w2 [4 6]',
      '70,5→70,55 blue w4 [4 6]',
    ]);
  });

  it('draws no target without one', () => {
    const p = pen();
    drawOverlay(p, { left: 0, top: 0, width: 10, height: 10 }, null, [], null, { target: 't', mark: 'm', hovered: 'h' }, 1);
    expect(p.lines).toEqual([]);
  });
});
