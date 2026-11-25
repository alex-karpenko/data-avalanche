import { describe, expect, it } from 'vitest';
import { HttpError, type Client } from '../lib/api';
import { NOW } from '../model/fixtures';
import { LagHistory, SPAN_S } from './lag.svelte';

function api(answer: unknown | Error): Client & { asked: string[] } {
  const asked: string[] = [];
  return {
    asked,
    get: async <T,>(path: string) => {
      asked.push(path);
      if (answer instanceof Error) throw answer;
      return answer as T;
    },
    send: async () => {
      throw new Error('reads only');
    },
  };
}

const t = NOW / 1000;

describe("pipelines' lag over 6 hours", () => {
  it('reads every line, or one pipeline\'s, at a minute a point', async () => {
    const all = api({ series: [{ name: 'lag_seconds', pipeline: 'orders-pg', points: [[t - 60, 3]] }] });
    const h = new LagHistory(all, null, () => NOW);
    await h.load();
    const since = new Date(NOW - SPAN_S * 1000).toISOString();
    expect(all.asked).toEqual([`/v1/history?series=lag_seconds&since=${since}&step=60`]);
    expect(h.lines).toEqual({ 'orders-pg': [[t - 60, 3]] });
    const one = api({ series: [] });
    await new LagHistory(one, 'orders pg', () => NOW).load();
    expect(one.asked).toEqual([`/v1/history?series=lag_seconds&pipeline=orders%20pg&since=${since}&step=60`]);
  });

  it('reads once however often it is asked to be there', async () => {
    const a = api({ series: [] });
    const h = new LagHistory(a, null, () => NOW);
    await h.ensure();
    await h.ensure();
    expect(a.asked).toHaveLength(1);
  });

  it('adds each sample and keeps 6 hours; one scoped to a pipeline takes its own alone', () => {
    const h = new LagHistory(api({ series: [] }), 'orders-pg', () => NOW);
    h.lines = { 'orders-pg': [[t - SPAN_S, 1], [t - 60, 2]] };
    h.sample({ scope: 'pipeline', pipeline: 'orders-pg', at: t, series: { lag_seconds: 4 } });
    h.sample({ scope: 'pipeline', pipeline: 'crm-pg', at: t, series: { lag_seconds: 94 } });
    h.sample({ scope: 'node', at: t, series: { memory_bytes: 1 } });
    expect(h.lines).toEqual({ 'orders-pg': [[t - 60, 2], [t, 4]] });
  });

  it('keeps its lines and says why when a read fails', async () => {
    const h = new LagHistory(api(new HttpError(503, 'the chart history: slow down')), null, () => NOW);
    h.lines = { 'orders-pg': [[t, 1]] };
    await h.load();
    expect(h.error).toBe('the chart history: slow down');
    expect(h.lines).toEqual({ 'orders-pg': [[t, 1]] });
  });
});
