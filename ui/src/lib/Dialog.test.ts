import { flushSync, mount, tick, unmount } from 'svelte';
import { afterEach, describe, expect, it } from 'vitest';
import Dialog from './Dialog.svelte';

let mounted: ReturnType<typeof mount> | undefined;
afterEach(() => {
  if (mounted !== undefined) unmount(mounted);
  mounted = undefined;
});

function render(props: Record<string, unknown>): HTMLElement {
  const target = document.createElement('div');
  document.body.replaceChildren(target);
  mounted = mount(Dialog, {
    target,
    props: {
      title: 'Restart orders-pg?',
      lines: ['orders-pg stops on node-b…'],
      recorded: 'restarted by Oleksii (operator)',
      confirm: 'Restart',
      onConfirm: async () => null,
      onClose: () => {},
      ...props,
    } as never,
  });
  flushSync();
  return target;
}

const settle = async () => {
  await tick();
  await Promise.resolve();
  flushSync();
};

function type(t: HTMLElement, note: string) {
  const area = t.querySelector('textarea')!;
  area.value = note;
  area.dispatchEvent(new Event('input'));
  flushSync();
}

describe("an action's dialog", () => {
  it('says what the action does and how it will be recorded, the note field first', () => {
    const t = render({});
    expect(t.querySelector('[role="dialog"] h3')?.textContent).toBe('Restart orders-pg?');
    expect(t.textContent).toContain('orders-pg stops on node-b…');
    expect(t.textContent).toContain('Recorded as: restarted by Oleksii (operator)');
    expect(document.activeElement).toBe(t.querySelector('textarea'));
    expect(t.querySelector('textarea')?.maxLength).toBe(500);
  });

  it('carries the action out with the note, then closes', async () => {
    const notes: string[] = [];
    let closed = 0;
    const t = render({
      onConfirm: async (n: string) => (notes.push(n), null),
      onClose: () => closed++,
    });
    type(t, 'apply sink.prefix');
    t.querySelector<HTMLButtonElement>('.go')!.click();
    await settle();
    expect(notes).toEqual(['apply sink.prefix']);
    expect(closed).toBe(1);
  });

  it('stays open, saying why, when the action fails', async () => {
    let closed = 0;
    const t = render({ onConfirm: async () => 'no node holds its lease', onClose: () => closed++ });
    t.querySelector<HTMLButtonElement>('.go')!.click();
    await settle();
    expect(t.querySelector('[role="alert"]')?.textContent).toBe('no node holds its lease');
    expect(closed).toBe(0);
  });

  it('stays open, free to try again, when the action throws', async () => {
    let closed = 0;
    const t = render({
      onConfirm: async () => {
        throw new Error('the connection dropped');
      },
      onClose: () => closed++,
    });
    t.querySelector<HTMLButtonElement>('.go')!.click();
    await settle();
    expect(t.querySelector('[role="alert"]')?.textContent).toBe('the connection dropped');
    expect(t.querySelector<HTMLButtonElement>('.go')!.disabled).toBe(false);
    expect(t.querySelector<HTMLButtonElement>('.buttons button')!.disabled).toBe(false);
    expect(closed).toBe(0);
  });

  it("can't be confirmed while blocked, and says why", () => {
    const t = render({ blocked: 'Offline: actions wait until the console is live again.' });
    expect(t.querySelector<HTMLButtonElement>('.go')!.disabled).toBe(true);
    expect(t.textContent).toContain('Offline: actions wait until the console is live again.');
  });

  it('closes on Escape or Cancel, and marks a destructive action', () => {
    let closed = 0;
    const t = render({ destructive: true, onClose: () => closed++ });
    expect(t.querySelector('.go')?.classList.contains('destructive')).toBe(true);
    window.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }));
    t.querySelector<HTMLButtonElement>('.buttons button')!.click();
    expect(closed).toBe(2);
  });
});
