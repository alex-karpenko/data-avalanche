import { describe, expect, it } from 'vitest';
import { ago, bytes, clock, duration } from './format';

describe('the console writes', () => {
  it('a duration in its two largest units', () => {
    expect(duration(0)).toBe('0 s');
    expect(duration(21.4)).toBe('21 s');
    expect(duration(252)).toBe('4m 12s');
    expect(duration(7200)).toBe('2h 0m');
    expect(duration(90061)).toBe('1d 1h');
  });

  it('how long ago, from now', () => {
    const now = Date.UTC(2026, 10, 25, 14, 0, 0);
    expect(ago(now - 8_000, now)).toBe('8 s ago');
    expect(ago(now - 180_000, now)).toBe('3 min ago');
    expect(ago(now - 7_200_000, now)).toBe('2 h ago');
    expect(ago(now - 3 * 86_400_000, now)).toBe('3 d ago');
    // A clock a little ahead of the browser's isn't in the future.
    expect(ago(now + 2_000, now)).toBe('0 s ago');
  });

  it('a size as PostgreSQL does, in 1024s', () => {
    expect(bytes(512)).toBe('512 bytes');
    expect(bytes(3.4 * 1024 ** 3)).toBe('3.4 GB');
    expect(bytes(512 * 1024 ** 2)).toBe('512 MB');
    expect(bytes(1536)).toBe('1.5 kB');
  });

  it('the time of day, to the minute or the second', () => {
    const at = new Date(2026, 10, 25, 9, 5, 30).getTime();
    expect(clock(at)).toBe('09:05');
    expect(clock(at, true)).toBe('09:05:30');
  });
});
