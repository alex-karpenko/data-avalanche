import { describe, expect, it } from 'vitest';
import { clock } from '../lib/format';
import { facts, nodeFigures, NOW } from '../model/fixtures';
import { nodeDetail, nodeSeries } from './fixtures';
import { cpuNote, gossipLines, memoryNote, nodeHeader, nodePipelines, renewalNote } from './view';

const MIN = 60_000;

describe("a node's page", () => {
  it('heads with its state, where its API and gossip answer, its build, generation and start', () => {
    expect(nodeHeader('n2', facts(), nodeDetail(), NOW)).toEqual({
      state: { label: 'alive', tone: 'green' },
      line: '10.0.1.12:8080 · gossip 10.0.1.12:7946 · 1.1.0 · config gen 42 · started 3 d ago',
    });
  });

  it("heads with what the cluster's status says of it until its own detail is read", () => {
    expect(nodeHeader('n3', facts(), null, NOW)).toEqual({
      state: { label: 'alive', tone: 'green' },
      line: '10.0.1.13:8080 · gossip 10.0.1.13:7946 · 1.1.0 · config gen 41 · started 2 h ago',
    });
  });

  it('says a node the cluster no longer lists is not a member, its detail kept for the line', () => {
    const f = facts();
    f.cluster!.members = f.cluster!.members.filter((m) => m.node !== 'n2');
    expect(nodeHeader('n2', f, nodeDetail(), NOW)).toEqual({
      state: { label: 'not a member', tone: 'grey' },
      line: '10.0.1.12:8080 · gossip 10.0.1.12:7946 · 1.1.0 · config gen 42 · started 3 d ago',
    });
  });

  it("heads with its detail's phase while the cluster is not read yet", () => {
    const f = facts();
    f.cluster = null;
    expect(nodeHeader('n2', f, { ...nodeDetail(), phase: 'draining' }, NOW)!.state).toEqual({ label: 'leaving', tone: 'grey' });
    expect(nodeHeader('n2', f, nodeDetail(), NOW)!.state).toEqual({ label: 'alive', tone: 'green' });
  });

  it("says a node the console's stream can't reach is unreachable, and a name no member has is none", () => {
    const f = facts();
    f.unreachable = { n2: NOW - 40_000 };
    expect(nodeHeader('n2', f, nodeDetail(), NOW)!.state).toEqual({ label: 'alive · unreachable 40 s', tone: 'orange' });
    expect(nodeHeader('n9', facts(), null, NOW)).toBeNull();
  });

  it('lists the pipelines it holds, the worst first, with their lag and their lease', () => {
    expect(nodePipelines('n2', facts(), nodeDetail(), NOW)).toEqual([
      {
        id: 'orders-pg',
        href: '/ui/pipelines/orders-pg',
        state: { label: '1 halted', tone: 'red' },
        lag: '21 s',
        lease: 'epoch 2 · away from owner n1',
      },
    ]);
    expect(nodePipelines('n1', facts(), null, NOW).map((r) => [r.id, r.state.label, r.lag, r.lease])).toEqual([
      ['billing-mysql', 'lagging', '4m 12s', 'epoch 3'],
      ['customers', 'snapshot 62%', '—', 'epoch 1'],
    ]);
  });

  it('takes its own detail for what it holds while the cluster is not read yet', () => {
    const f = facts();
    f.cluster = null;
    expect(nodePipelines('n2', f, nodeDetail(), NOW).map((r) => [r.id, r.lease])).toEqual([['orders-pg', 'epoch 2 · away from owner n1']]);
  });

  it('says what its gossip sees: its peers, its last probe, the suspicions of the last day', () => {
    expect(gossipLines(nodeDetail().gossip, NOW)).toEqual([
      { label: 'Sees', value: 'n1 alive · n3 alive' },
      { label: 'Last probe', value: 'n1 0 s ago · RTT 1.2 ms' },
      { label: 'Suspicions, 24 h', value: `raised 1 (n1 ${clock(NOW - 8 * MIN)}) · received 0` },
    ]);
    const g = nodeFigures('n2', { gossip: { ...nodeDetail().gossip, peers: [{ node: 'n1', state: 'suspect', phase: 'active' }], last_probe: null, probe_rtt_ms: null } }).gossip;
    expect(gossipLines(g, NOW).slice(0, 2)).toEqual([
      { label: 'Sees', value: 'n1 suspect' },
      { label: 'Last probe', value: 'none yet' },
    ]);
  });

  it("heads its charts with the renewals' peak, the memory against its limit and the CPU against its", () => {
    expect(renewalNote(nodeSeries().lease_renew_seconds)).toBe(`peak 610 ms at ${clock(NOW - 7 * MIN)}`);
    expect(renewalNote([])).toBe('no samples yet');
    const p = nodeDetail().process;
    expect(memoryNote(p)).toBe('1.3 GB of 2 GB limit');
    expect(memoryNote({ ...p, memory_limit_bytes: null })).toBe('1.3 GB · no limit');
    expect(cpuNote(p)).toBe('0.6 cores · limit 2');
    expect(cpuNote({ ...p, cpu_limit_cores: null })).toBe('0.6 cores · no limit');
    expect(cpuNote({ ...p, cpu_cores: null })).toBe('—');
  });
});
