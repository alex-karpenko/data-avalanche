import { describe, expect, it } from 'vitest';
import { failingNotice, failures, staleNotice } from './notices';

const NOW = Date.UTC(2026, 10, 25, 14, 2, 0);

describe("a page's notices", () => {
  it("names each part that failed, with its error and how old what's shown is", () => {
    const entries = failures(
      [
        ['the pipelines', { at: NOW - 5_000, error: null }],
        ['the snapshot jobs', { at: NOW - 20 * 60_000, error: 'HTTP 503' }],
        ['the cluster', { at: null, error: 'the request timed out' }],
      ],
      NOW,
    );
    expect(entries).toEqual([
      'the snapshot jobs (HTTP 503), last read 20 min ago',
      'the cluster (the request timed out), never read',
    ]);
    expect(failingNotice(entries)).toBe(
      "Couldn't be read; showing what was last read: the snapshot jobs (HTTP 503), last read 20 min ago; the cluster (the request timed out), never read",
    );
    expect(failingNotice([])).toBeNull();
  });

  it('says how old the numbers are, or that no live update came yet', () => {
    expect(staleNotice(NOW - 34_000, NOW)).toMatch(/^Not current: as of \d\d:\d\d:\d\d · 34 s old$/);
    expect(staleNotice(null, NOW)).toBe('Not current: no live update yet');
  });
});
