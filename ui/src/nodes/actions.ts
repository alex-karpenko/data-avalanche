// A node's two actions (Admin_Console §5, mockup 11): leave the cluster, or shut down. An admin's
// only; sent through whichever node serves the page, the target checking the caller again and
// recording it. Neither can be undone from the console, so both are styled as destructive, and
// a shutdown needs the node's name typed.

import type { Client } from '../lib/api';
import { noteBody } from '../lib/actions';
import type { Role, Session } from '../lib/session';
import type { Facts } from '../model/facts';
import { shortJob } from '../overview/attention';

export type NodeVerb = 'leave' | 'shutdown';
export const NODE_VERBS: NodeVerb[] = ['leave', 'shutdown'];

/** What a node's dialog says it acts on. */
export interface NodeTarget {
  name: string;
  /** The pipelines whose lease it holds. */
  pipelines: string[];
  /** Their running snapshot jobs, by short id. */
  snapshots: { job: string; pipeline: string }[];
  /** How many members the cluster has, it among them. */
  members: number;
  /** The other members that can carry its pipelines: active and not suspect, by name. */
  carriers: string[];
  /** The node the console's live updates come through. */
  via: string | null;
  drainSeconds: number | null;
}

export function nodeTarget(name: string, f: Facts, via: string | null): NodeTarget {
  const pipelines = (f.cluster?.pipelines ?? [])
    .filter((l) => l.holder === name)
    .map((l) => l.id)
    .sort();
  return {
    name,
    pipelines,
    snapshots: f.jobs
      .filter((j) => j.status === 'running' && pipelines.includes(j.pipeline))
      .map((j) => ({ job: shortJob(j.job_id), pipeline: j.pipeline })),
    members: f.cluster?.members.length ?? 0,
    carriers: (f.cluster?.members ?? [])
      .filter((m) => m.node !== name && m.phase === 'active' && !m.suspect)
      .map((m) => m.node)
      .sort(),
    via,
    drainSeconds: f.cluster?.settings?.drain_timeout_seconds ?? null,
  };
}

const plural = (n: number, one: string) => `${n} ${one}${n === 1 ? '' : 's'}`;
const STAYS_UP = 'The process keeps running, out of the cluster, until it is stopped or restarted.';

/**
 * The line when the console's live updates come through the node: a leave keeps its process and
 * its HTTP server up, so the stream stays on it; after a shutdown the page reconnects through the
 * address it was opened at.
 */
function viaLine(v: NodeVerb, t: NodeTarget): string[] {
  if (t.via !== t.name) return [];
  return v === 'leave'
    ? [
        `This console's live updates come through ${t.name}, which keeps answering out of the cluster: to follow the cluster, open the console through another node.`,
      ]
    : [
        `This console's live updates come through ${t.name}: once it exits, the page reconnects through the address it was opened at, which reaches another node only behind a load balancer.`,
      ];
}

/** Who carries a shut-down node's pipelines until it is back. */
function carriersLine(t: NodeTarget): string {
  const others = t.members - 1;
  if (others <= 0) return 'It is the only node: nothing runs its pipelines until it is back.';
  if (t.carriers.length >= others) return `With ${t.members} nodes, the other ${others} carry everything until it is back.`;
  if (t.carriers.length === 0) return 'No other node can carry its pipelines now: nothing runs them until it is back.';
  return `With ${t.members} nodes, only ${t.carriers.length} of the others can carry everything until it is back: ${t.carriers.join(', ')}.`;
}

export interface NodeAction {
  label: string;
  title: (name: string) => string;
  confirm: (name: string) => string;
  destructive: boolean;
  /** Confirmed only once the node's name is typed. */
  typed: boolean;
  lines: (t: NodeTarget) => string[];
}

export const NODE_ACTIONS: Record<NodeVerb, NodeAction> = {
  leave: {
    label: 'Leave',
    title: (n) => `${n} leaves the cluster?`,
    confirm: () => 'Leave',
    destructive: true,
    typed: false,
    lines: (t) => {
      const k = t.pipelines.length;
      if (k === 0) return [`${t.name} holds no pipeline: it leaves the ring. ${STAYS_UP}`, ...viaLine('leave', t)];
      return [
        `${t.name} hands its ${plural(k, 'pipeline')} to the others, one at a time, and leaves the ring. ${STAYS_UP}`,
        `Its pipelines: ${t.pipelines.join(', ')}. The ring places each on another node.`,
        ...t.snapshots.map(
          (s) => `Its running snapshot ${s.job} of ${s.pipeline} goes on from its last chunk where ${s.pipeline} goes.`,
        ),
        `Each hand-off pauses its pipeline for a while; lag rises meanwhile. It ends when every hand-off is done, or after drain_timeout${
          t.drainSeconds === null ? '' : ` (${t.drainSeconds} s)`
        } for each round.`,
        ...viaLine('leave', t),
      ];
    },
  },
  shutdown: {
    label: 'Shut down',
    title: (n) => `Shut down ${n}?`,
    confirm: (n) => `Shut down ${n}`,
    destructive: true,
    typed: true,
    lines: (t) => {
      const k = t.pipelines.length;
      return [
        `${t.name} leaves the cluster as a leave does${k === 0 ? '' : `, handing off its ${plural(k, 'pipeline')}`}, then exits. A supervisor such as Kubernetes starts it again, and it rejoins; without one, nothing restarts it.`,
        carriersLine(t),
        ...viaLine('shutdown', t),
      ];
    },
  },
};

/** Whether `role` may act on nodes: an admin's only. */
export function mayAdmin(role: Role): boolean {
  return role === 'admin';
}

/** The line a node's action will be recorded as. */
export function recordedOn(verb: NodeVerb, name: string, s: Pick<Session, 'name' | 'role'>): string {
  return verb === 'leave'
    ? `${name} left the cluster, asked by ${s.name} (${s.role})`
    : `${name} shut down by ${s.name} (${s.role})`;
}

/** Whether what was typed names the node. */
export function typedOk(typed: string, name: string): boolean {
  return typed.trim() === name;
}

/** Asks `name` to leave or shut down, through any node, with `note`. */
export async function askNode(api: Client, verb: NodeVerb, name: string, note: string): Promise<void> {
  await api.send('POST', `/v1/nodes/${encodeURIComponent(name)}/${verb}`, noteBody(note));
}
