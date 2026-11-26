// The tests' node n2 (mockup 04's node-b): up three days, holding orders-pg away from its owner
// n1 since a failover, with a day's suspicion it raised.

import { event, iso, nodeFigures, NOW } from '../model/fixtures';
import type { EventItem, NodeDetail } from '../model/types';

const MIN = 60_000;
const GIB = 1024 ** 3;
const t = NOW / 1000;

export function nodeDetail(over: Partial<NodeDetail> = {}): NodeDetail {
  const live = nodeFigures('n2');
  return {
    node: 'n2',
    api_url: 'https://10.0.1.12:8080',
    gossip_addr: '10.0.1.12:7946',
    build: '1.1.0',
    incarnation: NOW - 3 * 24 * 60 * MIN,
    started_at: iso(NOW - 3 * 24 * 60 * MIN),
    phase: 'active',
    config_generation: 42,
    file: { hash: 'a1b2c3d4e5f60718', generation_hash: 'a1b2c3d4e5f60718', matches: true },
    restart_pending: [],
    process: live.process,
    gossip: { ...live.gossip, suspicions: { raised: [{ node: 'n1', at: iso(NOW - 8 * MIN) }], received: 0, raised_more: 0, received_more: 0 } },
    pipelines: [{ id: 'orders-pg', owner_epoch: 2, ring_owner: 'n1' }],
    ...over,
  };
}

/** n2's charts over 6 hours: a renewal peak at the failover, memory and CPU. */
export function nodeSeries(): Record<string, [number, number][]> {
  return {
    lease_renew_seconds: [
      [t - 600, 0.041],
      [t - 420, 0.61],
      [t, 0.038],
    ],
    memory_bytes: [
      [t - 300, 1.2 * GIB],
      [t, 1.3 * GIB],
    ],
    cpu_cores: [[t, 0.6]],
  };
}

/** What n2 recorded, newest first. */
export function nodeEvents(): EventItem[] {
  return [
    event({ key: 'e3', kind: 'lease.takeover', at: iso(NOW - 7 * MIN), node: 'n2', pipeline: 'orders-pg', text: 'orders-pg taken over from n1 by n2' }),
    event({ key: 'e2', kind: 'member.restarted', at: iso(NOW - 10 * MIN), node: 'n2', object: 'n3', text: 'n3 rejoined' }),
    event({ key: 'e1', kind: 'config.adopted', at: iso(NOW - 4 * 60 * MIN), node: 'n2', text: 'adopted config generation 42' }),
  ];
}
