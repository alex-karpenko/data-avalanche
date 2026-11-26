// A node's page (Admin_Console §4, mockup 04), worked out from what the console knows of it: the
// header, the pipelines it holds, its gossip, and the notes heading its charts.

import { stateOf } from '../cluster/derive';
import { ago, bytes, clock, duration } from '../lib/format';
import { formatValue } from '../lib/series';
import { at, type Facts } from '../model/facts';
import type { Gossip, NodeDetail, Process } from '../model/types';
import { rows, type Tone } from '../overview/derive';

export interface NodeHeader {
  state: { label: string; tone: Tone };
  /** `10.0.1.12:8080 · gossip 10.0.1.12:7946 · 1.1.0 · config gen 42 · started 3 d ago`. */
  line: string;
}

/**
 * Its header: from its own detail where read, else what the cluster's status says; `null` for a
 * name no member has. Its state is the cluster's; its detail's phase (read as the page opened)
 * only until the cluster is read: a node the cluster no longer lists is not a member.
 */
export function nodeHeader(name: string, f: Facts, d: NodeDetail | null, now: number): NodeHeader | null {
  const m = f.cluster?.members.find((x) => x.node === name);
  if (m === undefined && d === null) return null;
  let state: NodeHeader['state'] =
    m !== undefined
      ? stateOf(m)
      : f.cluster === null
        ? stateOf({ suspect: false, phase: d?.phase ?? 'unknown' })
        : { label: 'not a member', tone: 'grey' };
  const unreachable = f.unreachable[name];
  if (unreachable !== undefined) {
    state = { label: `${state.label} · unreachable ${duration((now - unreachable) / 1000)}`, tone: 'orange' };
  }
  const api = (d?.api_url ?? m?.api_url ?? null)?.replace(/^https?:\/\//, '') ?? null;
  const gossip = d?.gossip_addr ?? m?.addr ?? null;
  const generation = d?.config_generation ?? f.config?.nodes.find((n) => n.node === name)?.generation ?? m?.config_generation ?? null;
  const started = at(d?.started_at ?? m?.started_at);
  const line = [
    api,
    gossip === null ? null : `gossip ${gossip}`,
    d?.build ?? m?.build ?? null,
    generation === null ? null : `config gen ${generation}`,
    started === null ? null : `started ${ago(started, now)}`,
  ]
    .filter((p) => p !== null)
    .join(' · ');
  return { state, line };
}

export interface NodePipeline {
  id: string;
  href: string;
  state: { label: string; tone: Tone };
  lag: string;
  /** `epoch 7 · away from owner n1`. */
  lease: string;
}

/** The pipelines it holds, the worst first: by the cluster's leases, or its own detail before them. */
export function nodePipelines(name: string, f: Facts, d: NodeDetail | null, now: number): NodePipeline[] {
  const held =
    f.cluster?.pipelines
      .filter((l) => l.holder === name)
      .map((l) => ({ id: l.id, owner_epoch: l.owner_epoch, ring_owner: l.ring_owner })) ??
    d?.pipelines ??
    [];
  const order = rows(f, now);
  const rank = (id: string) => {
    const i = order.findIndex((r) => r.id === id);
    return i === -1 ? Infinity : i;
  };
  return [...held]
    .sort((a, b) => rank(a.id) - rank(b.id) || a.id.localeCompare(b.id))
    .map((l) => {
      const r = order.find((x) => x.id === l.id);
      return {
        id: l.id,
        href: `/ui/pipelines/${encodeURIComponent(l.id)}`,
        state: r === undefined ? { label: 'unknown', tone: 'grey' } : { label: r.state.label, tone: r.state.tone },
        lag: r?.lag == null ? '—' : formatValue('seconds', r.lag),
        lease: `epoch ${l.owner_epoch}${l.ring_owner !== null && l.ring_owner !== name ? ` · away from owner ${l.ring_owner}` : ''}`,
      };
    });
}

/** What its gossip sees: its peers, its last probe, the suspicions of the last day. */
export function gossipLines(g: Gossip, now: number): { label: string; value: string }[] {
  const probe = g.last_probe;
  const probedAt = at(probe?.at);
  const raised = g.suspicions.raised;
  return [
    { label: 'Sees', value: g.peers.length === 0 ? 'no peers' : g.peers.map((p) => `${p.node} ${p.state}`).join(' · ') },
    {
      label: 'Last probe',
      value:
        probe === null || probedAt === null
          ? 'none yet'
          : `${probe.node} ${ago(probedAt, now)}${g.probe_rtt_ms === null ? '' : ` · RTT ${g.probe_rtt_ms.toFixed(1)} ms`}`,
    },
    {
      label: 'Suspicions, 24 h',
      value: `raised ${raised.length + g.suspicions.raised_more}${
        raised.length === 0 ? '' : ` (${raised.map((r) => `${r.node} ${clock(at(r.at) ?? now)}`).join(', ')})`
      } · received ${g.suspicions.received + g.suspicions.received_more}`,
    },
  ];
}

/** The lease renewals' chart note: their slowest, and when. */
export function renewalNote(points: [number, number][]): string {
  if (points.length === 0) return 'no samples yet';
  const [t, v] = points.reduce((a, b) => (b[1] > a[1] ? b : a));
  return `peak ${formatValue('seconds', v)} at ${clock(t * 1000)}`;
}

/** The memory chart's note: in use, against its limit. */
export function memoryNote(p: Process): string {
  if (p.memory_bytes === null) return '—';
  return p.memory_limit_bytes === null
    ? `${bytes(p.memory_bytes)} · no limit`
    : `${bytes(p.memory_bytes)} of ${bytes(p.memory_limit_bytes)} limit`;
}

/** The CPU chart's note: cores in use, against its limit. */
export function cpuNote(p: Process): string {
  if (p.cpu_cores === null) return '—';
  return `${p.cpu_cores.toFixed(1)} cores · ${p.cpu_limit_cores === null ? 'no limit' : `limit ${p.cpu_limit_cores}`}`;
}
