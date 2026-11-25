// What the console lets a person do to a pipeline (Admin_Console §5): each action's dialog says
// what it does, to what, and its effect, takes an optional note, and names the line it will be
// recorded as. The server checks the role again and records the action whatever the page says.

import type { Client } from './api';
import type { Role, Session } from './session';
import type { Actual, Desired } from '../model/types';

export type Verb = 'flush' | 'pause' | 'resume' | 'restart' | 'stop';
/** The order the actions are offered in (mockups 05, 06). */
export const VERBS: Verb[] = ['flush', 'pause', 'resume', 'restart', 'stop'];
/** The longest note the server keeps (`MAX_NOTE`). */
export const NOTE_MAX = 500;

/** A pipeline as an action's dialog describes it. */
export interface Target {
  id: string;
  /** The node that runs it; `null` when none holds its lease. */
  node: string | null;
  desired: Desired;
  actual: Actual | null;
  /** Its running snapshot job's short id, `null` without one. */
  job: string | null;
}

interface Action {
  label: string;
  /** As the event names it: `restarted by Oleksii (operator)`. */
  past: string;
  /** What it does to each of several, in the list's dialog. */
  each: string;
  /** Styled as one that stops something. */
  destructive: boolean;
  /** Whether the server takes it in the desired state `desired` (else it answers `409`). */
  takes: (desired: Desired) => boolean;
  effect: (t: Target) => string;
}

const on = (t: Target) => (t.node === null ? '' : ` on ${t.node}`);

export const ACTIONS: Record<Verb, Action> = {
  flush: {
    label: 'Flush',
    past: 'flushed',
    each: 'Each one commits what it has buffered now, whatever its flush policy says.',
    destructive: false,
    takes: (d) => d !== 'stopped',
    effect: (t) => `${t.id} commits what it has buffered now${on(t)}, whatever its flush policy says.`,
  },
  pause: {
    label: 'Pause',
    past: 'paused',
    each: "Each one finishes its open transaction, makes it durable, then reads nothing more; its source connection and its lease stay. Lag grows until it's resumed.",
    destructive: false,
    takes: (d) => d === 'running',
    effect: (t) =>
      `${t.id} finishes its open transaction${on(t)}, makes it durable, then reads nothing more; its source connection and its lease stay. Lag grows until it's resumed.`,
  },
  resume: {
    label: 'Resume',
    past: 'resumed',
    each: 'Each one reads again from its committed position.',
    destructive: false,
    takes: (d) => d !== 'running',
    effect: (t) => `${t.id} reads again from its committed position.`,
  },
  restart: {
    label: 'Restart',
    past: 'restarted',
    each: "Each one stops, drops what it hasn't committed and reads it again from its committed position: nothing is lost or written twice. Lag rises for about 30 s on each.",
    destructive: false,
    takes: (d) => d === 'running',
    effect: (t) =>
      `${t.id} stops${on(t)}, drops what it hasn't committed and reads it again from its committed position: nothing is lost or written twice. Lag rises for about 30 s.` +
      (t.job === null ? '' : ` Its running snapshot ${t.job} carries on from its last chunk.`),
  },
  stop: {
    label: 'Stop',
    past: 'stopped',
    each: "Each one flushes and closes its source connection; its lease stays. The source keeps its log for it until it's resumed: a PostgreSQL slot holds WAL meanwhile.",
    destructive: true,
    takes: (d) => d !== 'stopped',
    effect: (t) =>
      `${t.id} flushes and closes its source connection${on(t)}; its lease stays. The source keeps its log for it until it's resumed: a PostgreSQL slot holds WAL meanwhile.`,
  },
};

/** Whether `role` may act on pipelines: an operator's or an admin's. */
export function mayAct(role: Role): boolean {
  return role === 'operator' || role === 'admin';
}

/** The line an action will be recorded as: `restarted by Oleksii (operator)`. */
export function recordedAs(verb: Verb, s: Pick<Session, 'name' | 'role'>): string {
  return `${ACTIONS[verb].past} by ${s.name} (${s.role})`;
}

/** The body an action sends: its note when there is one. */
export function noteBody(note: string): { note: string } | undefined {
  const n = note.trim();
  return n === '' ? undefined : { note: n };
}

/** Asks for `verb` on pipeline `id`, with `note`. */
export async function ask(api: Client, verb: Verb, id: string, note: string): Promise<void> {
  await api.send('POST', `/v1/pipelines/${encodeURIComponent(id)}/${verb}`, noteBody(note));
}

/** How a run over several pipelines ended. */
export interface Outcome {
  done: string[];
  /** The one that failed, and why; the run stopped there. */
  failed: { id: string; error: string } | null;
  /** Those not asked, after a failure. */
  left: string[];
}

/** Runs `each` on `ids` one at a time, in order, stopping at the first failure (mockup 05). */
export async function oneAtATime(ids: string[], each: (id: string) => Promise<void>): Promise<Outcome> {
  const done: string[] = [];
  for (const [i, id] of ids.entries()) {
    try {
      await each(id);
      done.push(id);
    } catch (e) {
      return { done, failed: { id, error: e instanceof Error ? e.message : String(e) }, left: ids.slice(i + 1) };
    }
  }
  return { done, failed: null, left: [] };
}

/** What a run over several pipelines says when it ends: `null` when every one was asked. */
export function outcomeText(o: Outcome): string | null {
  if (o.failed === null) return null;
  const parts = [`${o.failed.id}: ${o.failed.error}.`];
  if (o.done.length > 0) parts.push(`Asked already: ${o.done.join(', ')}.`);
  if (o.left.length > 0) parts.push(`Not asked: ${o.left.join(', ')}.`);
  return parts.join(' ');
}
