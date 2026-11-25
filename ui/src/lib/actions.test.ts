import { describe, expect, it } from 'vitest';
import { HttpError, type Client } from './api';
import { ACTIONS, ask, mayAct, noteBody, oneAtATime, outcomeText, recordedAs, VERBS, type Target } from './actions';

const orders: Target = { id: 'orders-pg', node: 'node-b', desired: 'running', actual: 'running', job: null };

describe('pipeline actions', () => {
  it('are offered to operators and admins, never to viewers', () => {
    expect([mayAct('viewer'), mayAct('operator'), mayAct('admin')]).toEqual([false, true, true]);
  });

  it('are offered in the states the server takes them in', () => {
    const offered = (desired: Target['desired']) => VERBS.filter((v) => ACTIONS[v].takes(desired));
    expect(offered('running')).toEqual(['flush', 'pause', 'restart', 'stop']);
    // The server answers a paused pipeline's flush `unchanged`: the pause flushed it already.
    expect(offered('paused')).toEqual(['resume', 'stop']);
    expect(offered('stopped')).toEqual(['resume']);
  });

  it('say what they do, on which node, and what follows', () => {
    expect(ACTIONS.restart.effect(orders)).toBe(
      "orders-pg stops on node-b, drops what it hasn't committed and reads it again from its committed position: nothing is lost or written twice. Lag rises for about 30 s.",
    );
    expect(ACTIONS.restart.effect({ ...orders, job: '7c3a9d5e' })).toContain(
      'Its running snapshot 7c3a9d5e carries on from its last chunk.',
    );
    expect(ACTIONS.flush.effect({ ...orders, node: null })).toBe(
      'orders-pg commits what it has buffered now, whatever its flush policy says.',
    );
    expect(ACTIONS.restart.each).toBe(
      "Each one stops, drops what it hasn't committed and reads it again from its committed position: nothing is lost or written twice. Lag rises for about 30 s on each.",
    );
    expect(ACTIONS.stop.destructive).toBe(true);
    expect(VERBS.filter((v) => ACTIONS[v].destructive)).toEqual(['stop']);
  });

  it('name the line they will be recorded as', () => {
    expect(recordedAs('restart', { name: 'Oleksii', role: 'operator' })).toBe('restarted by Oleksii (operator)');
    expect(recordedAs('pause', { name: 'anonymous', role: 'admin' })).toBe('paused by anonymous (admin)');
  });

  it('send their note, trimmed, and no body without one', async () => {
    expect(noteBody('  apply sink.prefix \n')).toEqual({ note: 'apply sink.prefix' });
    expect(noteBody('   ')).toBeUndefined();
    const sent: unknown[] = [];
    const api = { send: async (...a: unknown[]) => sent.push(a) } as unknown as Client;
    await ask(api, 'restart', 'orders pg', 'why');
    expect(sent).toEqual([['POST', '/v1/pipelines/orders%20pg/restart', { note: 'why' }]]);
  });
});

describe('an action on several pipelines', () => {
  it('asks one at a time, in order, and stops at the first failure, saying which', async () => {
    const asked: string[] = [];
    const o = await oneAtATime(['crm-pg', 'ledger-my', 'payments-pg'], async (id) => {
      asked.push(id);
      if (id === 'ledger-my') throw new HttpError(409, 'a paused pipeline restarts once resumed');
    });
    expect(asked).toEqual(['crm-pg', 'ledger-my']);
    expect(o).toEqual({
      done: ['crm-pg'],
      failed: { id: 'ledger-my', error: 'a paused pipeline restarts once resumed' },
      left: ['payments-pg'],
    });
    expect(outcomeText(o)).toBe(
      'ledger-my: a paused pipeline restarts once resumed. Asked already: crm-pg. Not asked: payments-pg.',
    );
  });

  it('says nothing more when every one was asked', async () => {
    const o = await oneAtATime(['a', 'b'], async () => {});
    expect(o).toEqual({ done: ['a', 'b'], failed: null, left: [] });
    expect(outcomeText(o)).toBeNull();
  });
});
