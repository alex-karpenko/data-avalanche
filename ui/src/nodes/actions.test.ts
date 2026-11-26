import { describe, expect, it } from 'vitest';
import type { Client } from '../lib/api';
import { facts } from '../model/fixtures';
import { askNode, mayAdmin, NODE_ACTIONS, nodeTarget, recordedOn, typedOk } from './actions';

describe("a node's actions", () => {
  it('are an admin\'s only', () => {
    expect([mayAdmin('viewer'), mayAdmin('operator'), mayAdmin('admin')]).toEqual([false, false, true]);
  });

  it('say what a leave hands off, where a running snapshot goes on, and how long it takes', () => {
    const t = nodeTarget('n1', facts(), 'n2');
    expect(t).toEqual({ name: 'n1', pipelines: ['billing-mysql', 'customers'], snapshots: [{ job: '019ab5d0', pipeline: 'customers' }], members: 3, carriers: ['n2', 'n3'], via: 'n2', drainSeconds: 60 });
    expect(NODE_ACTIONS.leave.title('n1')).toBe('n1 leaves the cluster?');
    expect(NODE_ACTIONS.leave.lines(t)).toEqual([
      'n1 hands its 2 pipelines to the others, one at a time, and leaves the ring. The process keeps running, out of the cluster, until it is stopped or restarted.',
      'Its pipelines: billing-mysql, customers. The ring places each on another node.',
      'Its running snapshot 019ab5d0 of customers goes on from its last chunk where customers goes.',
      "Each hand-off pauses its pipeline for a while; lag rises meanwhile. It ends when every hand-off is done, or after drain_timeout (60 s) for each round.",
    ]);
  });

  it("says a shutdown leaves first, then exits, and who carries its pipelines meanwhile", () => {
    const t = nodeTarget('n2', facts(), 'n1');
    expect(NODE_ACTIONS.shutdown.title('n2')).toBe('Shut down n2?');
    expect(NODE_ACTIONS.shutdown.lines(t)).toEqual([
      'n2 leaves the cluster as a leave does, handing off its 1 pipeline, then exits. A supervisor such as Kubernetes starts it again, and it rejoins; without one, nothing restarts it.',
      'With 3 nodes, the other 2 carry everything until it is back.',
    ]);
    expect([NODE_ACTIONS.leave.destructive, NODE_ACTIONS.shutdown.destructive]).toEqual([true, true]);
    expect([NODE_ACTIONS.leave.typed, NODE_ACTIONS.shutdown.typed]).toEqual([false, true]);
  });

  it('says only the others that can carry its pipelines carry them: active, not suspect', () => {
    const f = facts();
    f.cluster!.members[2].suspect = true;
    const t = nodeTarget('n2', f, 'n1');
    expect(t.carriers).toEqual(['n1']);
    expect(NODE_ACTIONS.shutdown.lines(t)[1]).toBe('With 3 nodes, only 1 of the others can carry everything until it is back: n1.');
    f.cluster!.members[0].phase = 'draining';
    expect(NODE_ACTIONS.shutdown.lines(nodeTarget('n2', f, 'n1'))[1]).toBe(
      'No other node can carry its pipelines now: nothing runs them until it is back.',
    );
    f.cluster!.members = f.cluster!.members.filter((m) => m.node === 'n2');
    expect(NODE_ACTIONS.shutdown.lines(nodeTarget('n2', f, 'n1'))[1]).toBe('It is the only node: nothing runs its pipelines until it is back.');
  });

  it("says what follows when the node is the one the console's live updates come through", () => {
    const t = nodeTarget('n2', facts(), 'n2');
    expect(NODE_ACTIONS.leave.lines(t).at(-1)).toBe(
      "This console's live updates come through n2, which keeps answering out of the cluster: to follow the cluster, open the console through another node.",
    );
    expect(NODE_ACTIONS.shutdown.lines(t).at(-1)).toBe(
      "This console's live updates come through n2: once it exits, the page reconnects through the address it was opened at, which reaches another node only behind a load balancer.",
    );
  });

  it('says a node holding nothing just leaves the ring', () => {
    const t = nodeTarget('n9', facts(), 'n1');
    expect(NODE_ACTIONS.leave.lines(t)[0]).toBe(
      'n9 holds no pipeline: it leaves the ring. The process keeps running, out of the cluster, until it is stopped or restarted.',
    );
  });

  it('is recorded under the name of who asked, and needs the name typed to shut down', () => {
    const s = { name: 'Oleksii', role: 'admin' as const };
    expect(recordedOn('leave', 'n2', s)).toBe('n2 left the cluster, asked by Oleksii (admin)');
    expect(recordedOn('shutdown', 'n2', s)).toBe('n2 shut down by Oleksii (admin)');
    expect([typedOk('n2', 'n2'), typedOk(' n2 ', 'n2'), typedOk('n', 'n2'), typedOk('N2', 'n2')]).toEqual([true, true, false, false]);
  });

  it("asks the node through any node, with the note", async () => {
    const sent: unknown[][] = [];
    const api: Client = {
      get: async () => {
        throw new Error('nothing is read');
      },
      send: async <T,>(method: string, path: string, body?: unknown) => (sent.push([method, path, body]), {} as T),
    };
    await askNode(api, 'shutdown', 'node b', ' kernel patch ');
    await askNode(api, 'leave', 'n2', '');
    expect(sent).toEqual([
      ['POST', '/v1/nodes/node%20b/shutdown', { note: 'kernel patch' }],
      ['POST', '/v1/nodes/n2/leave', undefined],
    ]);
  });
});
