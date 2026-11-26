import { describe, expect, it } from 'vitest';
import { HttpError, type Client } from '../lib/api';
import { NOW } from '../model/fixtures';
import { RENEWALS_SERIES, Renewals } from './renewals.svelte';

function api(answers: Record<string, unknown>): Client & { asked: string[] } {
  const asked: string[] = [];
  return {
    asked,
    get: async <T,>(path: string) => {
      asked.push(path);
      if (!(path in answers)) throw new HttpError(503, `${path} is unavailable`);
      return answers[path] as T;
    },
    send: async () => {
      throw new Error('nothing is changed');
    },
  };
}

const t = NOW / 1000;

describe("the members' lease renewals", () => {
  it("reads each member's over the last hour, each on its own, and keeps its slowest", async () => {
    const a = api({
      [`/v1/history?node=n1&series=${RENEWALS_SERIES}`]: {
        series: [{ name: RENEWALS_SERIES, node: 'n1', points: [[t - 120, 0.021], [t - 60, 0.038], [t, 0.012]] }],
      },
      [`/v1/history?node=n3&series=${RENEWALS_SERIES}`]: { series: [{ name: RENEWALS_SERIES, node: 'n3', points: [] }] },
    });
    const r = new Renewals(a, () => NOW);
    await r.load(['n1', 'n2', 'n3']);
    expect(r.max).toEqual({ n1: 0.038 });
    expect(r.read.n1).toEqual({ at: NOW, error: null });
    expect(r.read.n2).toEqual({ at: null, error: `/v1/history?node=n2&series=${RENEWALS_SERIES} is unavailable` });
    expect(r.read.n3).toEqual({ at: NOW, error: null });
  });

  it('keeps what it had for a member whose read fails, and drops a node no longer a member', async () => {
    const one = `/v1/history?node=n1&series=${RENEWALS_SERIES}`;
    const answers: Record<string, unknown> = { [one]: { series: [{ name: RENEWALS_SERIES, node: 'n1', points: [[t, 0.04]] }] } };
    const r = new Renewals(api(answers), () => NOW);
    await r.load(['n1', 'n4']);
    delete answers[one];
    await r.load(['n1']);
    expect(r.max).toEqual({ n1: 0.04 });
    expect(r.read.n1.error).toBe(`${one} is unavailable`);
    expect(Object.keys(r.read)).toEqual(['n1']);
  });
});
