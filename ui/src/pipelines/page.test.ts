import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { HttpError, type Client } from '../lib/api';
import { event, iso, NOW } from '../model/fixtures';
import { REREAD_AFTER_MS } from '../model/model.svelte';
import { detail, events, jobs, liveFigures } from './fixtures';
import { PipelinePage } from './page.svelte';

/** A client answering each path from `answers` (an error is thrown); others fail with 503. */
function api(answers: Record<string, unknown>): Client & { asked: string[] } {
  const asked: string[] = [];
  return {
    asked,
    get: async <T,>(path: string) => {
      asked.push(path);
      const a = Object.entries(answers).find(([p]) => path.startsWith(p))?.[1];
      if (a === undefined) throw new HttpError(503, `${path} is unavailable`);
      if (a instanceof Error) throw a;
      return a as T;
    },
    send: async () => {
      throw new Error('reads only');
    },
  };
}

const READS = {
  '/v1/pipelines/orders-pg/live': liveFigures(),
  '/v1/pipelines/orders-pg/snapshots': { jobs: jobs() },
  '/v1/pipelines/orders-pg': detail(),
  '/v1/events?object=orders-pg&limit=50': { events: events() },
  '/v1/history?series=lag_seconds&pipeline=orders-pg': { series: [{ name: 'lag_seconds', pipeline: 'orders-pg', points: [[NOW / 1000, 21]] }] },
};

describe("a pipeline's page data", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('draws the figures the stream sent at once, then reads each part', async () => {
    const seed = { ...liveFigures(), owner_epoch: 1 };
    const a = api(READS);
    const p = new PipelinePage(a, 'orders-pg', seed, () => NOW);
    expect(p.figures).toEqual(seed);
    await p.load();
    expect(p.detail?.position).toBe('pg:3/A41C2B10');
    expect(p.figures?.owner_epoch).toBe(2);
    expect(p.jobs).toHaveLength(3);
    expect(p.events).toHaveLength(7);
    expect(p.lag.lines['orders-pg']).toEqual([[NOW / 1000, 21]]);
    expect(Object.values(p.read).every((r) => r.error === null)).toBe(true);
  });

  it('says a pipeline the generation lacks is missing, and keeps a part that fails to read', async () => {
    const p = new PipelinePage(
      api({ ...READS, '/v1/pipelines/orders-pg/live': new HttpError(409, "no node holds orders-pg's lease"), '/v1/pipelines/orders-pg': new HttpError(404, 'no such pipeline') }),
      'orders-pg',
      liveFigures(),
      () => NOW,
    );
    await p.load();
    expect(p.missing).toBe(true);
    expect(p.read.live.error).toBe("no node holds orders-pg's lease");
    expect(p.figures).not.toBeNull();
  });

  it("takes its own figures from the stream, the newest owner's, and no other pipeline's", () => {
    const p = new PipelinePage(api({}), 'orders-pg', liveFigures(), () => NOW);
    p.figuresMsg({ ...liveFigures(), id: 'crm-pg', owner_epoch: 9 });
    p.figuresMsg({ ...liveFigures(), owner_epoch: 1, node: 'n1' });
    expect(p.figures?.node).toBe('n2');
    p.figuresMsg({ ...liveFigures(), owner_epoch: 3, node: 'n3' });
    expect(p.figures?.node).toBe('n3');
  });

  it('adds its events once, and reads again, once, what a burst of them changes', async () => {
    const a = api(READS);
    const p = new PipelinePage(a, 'orders-pg', null, () => NOW);
    const done = event({ kind: 'job.completed', at: iso(NOW), pipeline: 'orders-pg', text: 'job 0192f0c4 completed' });
    p.event(done);
    p.event(done);
    p.event(event({ kind: 'action.pipeline.pause', at: iso(NOW), pipeline: 'orders-pg', text: 'pause', outcome: 'accepted' }));
    p.event(event({ kind: 'job.failed', at: iso(NOW), pipeline: 'crm-pg', text: 'not this one' }));
    expect(p.events.map((e) => e.kind)).toEqual(['action.pipeline.pause', 'job.completed']);
    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
    expect(a.asked.sort()).toEqual(['/v1/pipelines/orders-pg', '/v1/pipelines/orders-pg/snapshots']);
  });

  it("takes a state change from the node that runs it, and reads its detail again", async () => {
    const a = api(READS);
    const p = new PipelinePage(a, 'orders-pg', liveFigures(), () => NOW);
    p.pipelineMsg({ pipeline: 'orders-pg', actual: 'paused', error: null, node: 'n2', owner_epoch: 2 });
    p.pipelineMsg({ pipeline: 'orders-pg', actual: 'standby', error: null, node: 'n1', owner_epoch: 1 });
    expect(p.figures?.actual).toBe('paused');
    await vi.advanceTimersByTimeAsync(REREAD_AFTER_MS);
    expect(a.asked).toEqual(['/v1/pipelines/orders-pg']);
  });
});
