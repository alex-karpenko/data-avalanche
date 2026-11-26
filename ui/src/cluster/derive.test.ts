import { describe, expect, it } from 'vitest';
import { clock } from '../lib/format';
import { event, facts, iso, NOW } from '../model/fixtures';
import { awayRows, clusterTiles, leaseRows, memberRows, removalRows } from './derive';

const MIN = 60_000;
const takeover = () =>
  event({ kind: 'lease.takeover', at: iso(NOW - 7 * MIN), pipeline: 'orders-pg', text: 'orders-pg taken over from n1 by n2' });

describe('the cluster', () => {
  it("heads with mockup 03's tiles: members, leases, the config generation, away from owner, the cluster's settings", () => {
    const f = facts();
    f.leaseLosses = [takeover()];
    expect(clusterTiles(f, NOW)).toEqual([
      { key: 'Members', value: '3 of 3', detail: 'all alive', tone: 'green' },
      { key: 'Leases', value: '4', detail: 'all held', tone: 'normal' },
      { key: 'Config generation', value: '42', detail: 'n3 still on 41', tone: 'orange' },
      {
        key: 'Away from owner',
        value: '1',
        detail: `last taken over at ${clock(NOW - 7 * MIN)} · 1 lost or taken over in 1 h`,
        tone: 'orange',
      },
      { key: 'Cluster', value: '128 tokens', detail: 'lease 15 s · lock: storage', tone: 'normal' },
    ]);
  });

  it('counts members not alive and leases with no holder in the tiles', () => {
    const f = facts();
    f.cluster!.members[1].suspect = true;
    f.cluster!.members[2].phase = 'leaving';
    f.cluster!.pipelines[3] = { ...f.cluster!.pipelines[3], holder: null, expires_at: null };
    const [members, leases] = clusterTiles(f, NOW);
    expect(members).toEqual({ key: 'Members', value: '1 of 3', detail: '1 suspect · 1 leaving', tone: 'orange' });
    expect(leases).toEqual({ key: 'Leases', value: '4', detail: '1 with no holder or not renewing', tone: 'red' });
  });

  it('counts a lease not renewed as unheld, and names a member in any other state', () => {
    const f = facts();
    f.cluster!.pipelines[0] = { ...f.cluster!.pipelines[0], holder_stale: true };
    f.cluster!.members[2].phase = 'joining';
    const [members, leases] = clusterTiles(f, NOW);
    expect(leases).toEqual({ key: 'Leases', value: '4', detail: '1 with no holder or not renewing', tone: 'red' });
    expect(members).toEqual({ key: 'Members', value: '2 of 3', detail: '1 joining', tone: 'orange' });
    f.cluster!.members[1].suspect = true;
    f.cluster!.members[0].phase = 'joining';
    expect(clusterTiles(f, NOW)[0]).toEqual({ key: 'Members', value: '0 of 3', detail: '1 suspect · 2 joining', tone: 'orange' });
  });

  it('lists each member: its state, address, version, generation, pipelines held and owned, lease renewal, how long it is up', () => {
    const rows = memberRows(facts(), { n1: 0.038, n2: 0.61 }, NOW);
    expect(rows.map((r) => [r.node, r.href, r.state.label, r.address, r.build, r.generation.value, r.pipelines, r.renewal, r.up])).toEqual([
      ['n1', '/ui/cluster/nodes/n1', 'alive', '10.0.1.11:8080', '1.1.0', '42', '2 held · owns 3, 1 away · 1 snapshot', '38 ms', '3d 0h'],
      ['n2', '/ui/cluster/nodes/n2', 'alive', '10.0.1.12:8080', '1.1.0', '42', '1 held · owns 0 · 1 not its own', '610 ms', '3d 0h'],
      ['n3', '/ui/cluster/nodes/n3', 'alive', '10.0.1.13:8080', '1.1.0', '41', '1 held · owns 1', '—', '2h 0m'],
    ]);
    expect(rows.map((r) => r.generation.tone)).toEqual(['normal', 'normal', 'orange']);
  });

  it("says a member is suspect, leaving, or out of the console's reach, in words and colour", () => {
    const f = facts();
    f.cluster!.members[1].suspect = true;
    f.cluster!.members[2].phase = 'leaving';
    f.unreachable = { n1: NOW - 40_000 };
    expect(memberRows(f, {}, NOW).map((r) => [r.node, r.state.label, r.state.tone])).toEqual([
      ['n1', 'alive · unreachable 40 s', 'orange'],
      ['n2', 'suspect', 'orange'],
      ['n3', 'leaving', 'grey'],
    ]);
  });

  it('lists the pipelines held away from their owner: since when and why, and when they go back', () => {
    const f = facts();
    f.leaseLosses = [takeover()];
    expect(awayRows(f, NOW)).toEqual([
      {
        id: 'orders-pg',
        href: '/ui/pipelines/orders-pg',
        holder: 'n2',
        owner: 'n1',
        since: NOW - 7 * MIN,
        why: 'orders-pg taken over from n1 by n2',
        back: 'once n1 has been in the ring 15 s, 1 hand-off at a time',
        tone: 'orange',
      },
    ]);
  });

  it('turns an away pipeline red when no hand-off in the last hour explains it', () => {
    const [row] = awayRows(facts(), NOW);
    expect([row.since, row.why, row.tone]).toEqual([null, 'away for over an hour: no hand-off since', 'red']);
  });

  it('lists every lease: no holder first, then away from owner, then the soonest to end', () => {
    const f = facts();
    f.cluster!.pipelines[3] = { ...f.cluster!.pipelines[3], holder: null, expires_at: null };
    f.cluster!.pipelines[1].expires_at = iso(NOW + 3_000);
    f.cluster!.pipelines[2].expires_at = iso(NOW - 2_000);
    expect(leaseRows(f, NOW).map((r) => [r.id, r.holder, r.owner, r.epoch, r.expires, r.state.label, r.state.tone])).toEqual([
      ['customers', null, 'n1', null, '—', 'no holder', 'red'],
      ['orders-pg', 'n2', 'n1', 2, 'in 8 s', 'away from owner', 'orange'],
      ['inventory', 'n3', 'n3', 1, 'expired 2 s ago', 'held', 'normal'],
      ['billing-mysql', 'n1', 'n1', 3, 'in 3 s', 'held', 'normal'],
    ]);
  });

  it('says a holder that stopped renewing is not renewing', () => {
    const f = facts();
    f.cluster!.pipelines[1].holder_stale = true;
    const row = leaseRows(f, NOW).find((r) => r.id === 'billing-mysql')!;
    expect(row.state).toEqual({ label: 'not renewing', tone: 'red' });
  });

  it('lists the removals waiting for their teardown, oldest first, past the threshold in orange', () => {
    const f = facts();
    f.removals = [
      { id: 'payments', generation: 41, removed_at: iso(NOW - 4 * MIN) },
      { id: 'audit', generation: 42, removed_at: iso(NOW - 30_000) },
    ];
    expect(removalRows(f, NOW)).toEqual([
      { id: 'payments', generation: 41, since: '4 min ago', tone: 'orange' },
      { id: 'audit', generation: 42, since: '30 s ago', tone: 'normal' },
    ]);
  });
});
