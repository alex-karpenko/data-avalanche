// The Cluster page (Admin_Console §4, mockup 03), worked out from what the console knows: the
// tiles, the members, the pipelines held away from their owner, the leases, the pending removals.

import { ago, clock, duration } from '../lib/format';
import { formatValue } from '../lib/series';
import { at, type Facts } from '../model/facts';
import { LEASE_LOSS_WITHIN_S, nodeHref, REMOVAL_PENDING_S } from '../overview/attention';
import type { Tile, Tone } from '../overview/derive';

/** A pipeline away from its owner for longer than this, with no hand-off to say why, is red. */
export const AWAY_RED_AFTER_S = 3600;

const pipelineHref = (id: string) => `/ui/pipelines/${encodeURIComponent(id)}`;

/** A member's state in words: SWIM's suspicion first, then its phase. */
export function stateOf(m: { suspect: boolean; phase: string }): { label: string; tone: Tone } {
  if (m.suspect) return { label: 'suspect', tone: 'orange' };
  if (m.phase === 'active') return { label: 'alive', tone: 'green' };
  return { label: m.phase === 'draining' ? 'leaving' : m.phase, tone: 'grey' };
}

/** The five tiles: members, leases, the config generation, away from owner, the settings. */
export function clusterTiles(f: Facts, _now: number): Tile[] {
  const members = f.cluster?.members ?? [];
  const labels = members.map((m) => stateOf(m).label);
  const alive = labels.filter((l) => l === 'alive').length;
  // Every state but alive, counted by its label: suspect, leaving, then any other (`joining`).
  const counts = new Map<string, number>([
    ['suspect', 0],
    ['leaving', 0],
  ]);
  for (const l of labels.filter((x) => x !== 'alive').sort()) counts.set(l, (counts.get(l) ?? 0) + 1);
  const others = [...counts].filter(([, n]) => n > 0).map(([l, n]) => `${n} ${l}`);
  const leases = f.cluster?.pipelines ?? [];
  const unheld = leases.filter((l) => l.holder === null || l.holder_stale).length;
  const behind = (f.config?.nodes ?? []).filter((n) => n.generation !== null && n.generation !== f.config?.generation);
  const away = leases.filter((l) => l.holder !== null && l.ring_owner !== null && l.holder !== l.ring_owner).length;
  const last = f.leaseLosses[0];
  const lastAt = at(last?.at);
  const s = f.cluster?.settings;
  return [
    {
      key: 'Members',
      value: `${alive} of ${members.length}`,
      detail: others.length === 0 ? 'all alive' : others.join(' · '),
      tone: others.length === 0 ? 'green' : 'orange',
    },
    {
      key: 'Leases',
      value: String(leases.length),
      detail: unheld === 0 ? 'all held' : `${unheld} with no holder or not renewing`,
      tone: unheld === 0 ? 'normal' : 'red',
    },
    {
      key: 'Config generation',
      value: f.config === null ? '—' : String(f.config.generation),
      detail: behind.length === 0 ? 'every node on it' : behind.map((n) => `${n.node} still on ${n.generation}`).join(' · '),
      tone: behind.length === 0 ? 'normal' : 'orange',
    },
    {
      key: 'Away from owner',
      value: String(away),
      detail:
        lastAt === null
          ? `no lease lost or taken over in ${LEASE_LOSS_WITHIN_S / 3600} h`
          : `last taken over at ${clock(lastAt)} · ${f.leaseLosses.length} lost or taken over in ${LEASE_LOSS_WITHIN_S / 3600} h`,
      tone: away === 0 ? 'normal' : 'orange',
    },
    {
      key: 'Cluster',
      value: s === undefined ? '—' : `${s.tokens} tokens`,
      detail: s === undefined ? '' : `lease ${s.lease_seconds} s · lock: ${s.lock_backend}`,
      tone: 'normal',
    },
  ];
}

export interface MemberRow {
  node: string;
  href: string;
  state: { label: string; tone: Tone };
  /** Where its API answers. */
  address: string;
  build: string;
  generation: { value: string; tone: Tone };
  /** `3 held · owns 5, 2 away · 1 snapshot`. */
  pipelines: string;
  /** Its slowest lease renewal in the last hour. */
  renewal: string;
  up: string;
}

/** Each member, in name order. `renewals`: each one's slowest lease renewal (s), where read. */
export function memberRows(f: Facts, renewals: Record<string, number>, now: number): MemberRow[] {
  const leases = f.cluster?.pipelines ?? [];
  const running = new Set(f.jobs.filter((j) => j.status === 'running').map((j) => j.pipeline));
  return [...(f.cluster?.members ?? [])]
    .sort((a, b) => a.node.localeCompare(b.node))
    .map((m) => {
      const held = leases.filter((l) => l.holder === m.node);
      const owns = leases.filter((l) => l.ring_owner === m.node);
      const away = owns.filter((l) => l.holder !== null && l.holder !== m.node).length;
      const notOwn = held.filter((l) => l.ring_owner !== m.node).length;
      const snapshots = held.filter((l) => running.has(l.id)).length;
      const parts = [`${held.length} held`, `owns ${owns.length}${away > 0 ? `, ${away} away` : ''}`];
      if (notOwn > 0) parts.push(`${notOwn} not its own`);
      if (snapshots > 0) parts.push(`${snapshots} snapshot${snapshots === 1 ? '' : 's'}`);
      let state = stateOf(m);
      const unreachable = f.unreachable[m.node];
      if (unreachable !== undefined) {
        state = { label: `${state.label} · unreachable ${duration((now - unreachable) / 1000)}`, tone: 'orange' };
      }
      const known = f.config?.nodes.find((n) => n.node === m.node)?.generation ?? m.config_generation;
      const started = at(m.started_at);
      const renewal = renewals[m.node];
      return {
        node: m.node,
        href: nodeHref(m.node),
        state,
        address: m.api_url == null ? (m.addr ?? '—') : m.api_url.replace(/^https?:\/\//, ''),
        build: m.build ?? '—',
        generation: {
          value: known === null ? '—' : String(known),
          tone: f.config !== null && known !== null && known !== f.config.generation ? 'orange' : 'normal',
        },
        pipelines: parts.join(' · '),
        renewal: renewal === undefined ? '—' : formatValue('seconds', renewal),
        up: started === null ? '—' : duration((now - started) / 1000),
      };
    });
}

export interface AwayRow {
  id: string;
  href: string;
  holder: string;
  owner: string;
  /** When the hand-off that put it there happened (ms), when the last hour's events say. */
  since: number | null;
  why: string;
  /** When it goes back to its owner. */
  back: string;
  tone: Tone;
}

/** The pipelines held away from their ring owner, the longest away first. */
export function awayRows(f: Facts, now: number): AwayRow[] {
  const s = f.cluster?.settings;
  return (f.cluster?.pipelines ?? [])
    .filter((l) => l.holder !== null && l.ring_owner !== null && l.holder !== l.ring_owner)
    .map((l) => {
      const e = f.leaseLosses.find((x) => x.pipeline === l.id);
      const since = at(e?.at);
      const handoffs = s?.max_concurrent_handoffs ?? 1;
      return {
        id: l.id,
        href: pipelineHref(l.id),
        holder: l.holder!,
        owner: l.ring_owner!,
        since,
        why: e?.text ?? 'away for over an hour: no hand-off since',
        back:
          s === undefined
            ? `once ${l.ring_owner} is stable in the ring`
            : `once ${l.ring_owner} has been in the ring ${s.rebalance_stabilization_seconds} s, ${handoffs} hand-off${handoffs === 1 ? '' : 's'} at a time`,
        tone: since === null || now - since > AWAY_RED_AFTER_S * 1000 ? 'red' : 'orange',
      } satisfies AwayRow;
    })
    .sort((a, b) => (a.since ?? -Infinity) - (b.since ?? -Infinity) || a.id.localeCompare(b.id));
}

export interface LeaseRow {
  id: string;
  href: string;
  holder: string | null;
  owner: string | null;
  epoch: number | null;
  /** `in 8 s`, `expired 2 s ago`, or `—` with no holder. */
  expires: string;
  state: { label: string; tone: Tone };
}

const LEASE_RANK = { 'no holder': 0, 'not renewing': 0, 'away from owner': 1, held: 2 } as const;

/** Every lease: no holder (or one not renewing) first, then away from owner, then the soonest to end. */
export function leaseRows(f: Facts, now: number): LeaseRow[] {
  return (f.cluster?.pipelines ?? [])
    .map((l) => {
      const ends = at(l.expires_at);
      const state: LeaseRow['state'] =
        l.holder === null
          ? { label: 'no holder', tone: 'red' }
          : l.holder_stale
            ? { label: 'not renewing', tone: 'red' }
            : l.ring_owner !== null && l.holder !== l.ring_owner
              ? { label: 'away from owner', tone: 'orange' }
              : { label: 'held', tone: 'normal' };
      return {
        row: {
          id: l.id,
          href: pipelineHref(l.id),
          holder: l.holder,
          owner: l.ring_owner,
          epoch: l.holder === null ? null : l.owner_epoch,
          expires:
            l.holder === null || ends === null
              ? '—'
              : ends > now
                ? `in ${duration((ends - now) / 1000)}`
                : `expired ${duration((now - ends) / 1000)} ago`,
          state,
        },
        rank: LEASE_RANK[state.label as keyof typeof LEASE_RANK],
        ends: ends ?? Infinity,
      };
    })
    .sort((a, b) => a.rank - b.rank || a.ends - b.ends || a.row.id.localeCompare(b.row.id))
    .map((x) => x.row);
}

export interface RemovalRow {
  id: string;
  generation: number;
  since: string;
  tone: Tone;
}

/** The removals waiting for their teardown, the oldest first: past the rule's threshold, orange. */
export function removalRows(f: Facts, now: number): RemovalRow[] {
  return [...f.removals]
    .sort((a, b) => (at(a.removed_at) ?? 0) - (at(b.removed_at) ?? 0))
    .map((r) => {
      const since = at(r.removed_at) ?? now;
      return {
        id: r.id,
        generation: r.generation,
        since: ago(since, now),
        tone: now - since > REMOVAL_PENDING_S * 1000 ? 'orange' : 'normal',
      };
    });
}
