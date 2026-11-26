import { expect, test, type Page } from '@playwright/test';
import { env } from './common';

/** Admin_Console §8: the first page drawn under 1 s, moving between pages under 300 ms, with 50
 * pipelines. */
const FIRST_PAGE_MS = 1_000;
const MOVE_MS = 300;

test(`the Overview is drawn within ${FIRST_PAGE_MS} ms of asking for it`, async ({ page }) => {
  const pipelines = env('DA_PIPELINES');
  // Once, for the browser's cache of the app's files (kept for good, as a returning person's).
  await page.goto('/ui/');
  await expect(page.locator('tbody tr')).toHaveCount(Number(pipelines));
  await page.goto('/ui/');
  // The time since navigation at the first frame that shows every pipeline, taken in the page:
  // the test's own polling isn't the page's.
  const drawn = await page.waitForFunction(
    (n) =>
      document.querySelectorAll('tbody tr').length === n &&
      document.querySelector('.tile .v')?.textContent === String(n) &&
      performance.now(),
    Number(pipelines),
    { polling: 'raf' },
  );
  const ms = Number(await drawn.jsonValue());
  console.log(`drawn in ${Math.round(ms)} ms`);
  expect(ms).toBeLessThan(FIRST_PAGE_MS);
});

/** Clicks `link` in the page, or goes Back without one, and times until `ready` matches `n`
 * elements, taken in the page at each frame. */
async function move(page: Page, link: string | null, ready: string, n: number): Promise<number> {
  return page.evaluate(
    ({ link, ready, n }) =>
      new Promise<number>((done) => {
        const t0 = performance.now();
        if (link === null) history.back();
        else (document.querySelector(link) as HTMLElement).click();
        const check = () =>
          document.querySelectorAll(ready).length >= n ? done(performance.now() - t0) : requestAnimationFrame(check);
        requestAnimationFrame(check);
      }),
    { link, ready, n },
  );
}

test(`moving between pages takes under ${MOVE_MS} ms`, async ({ page }) => {
  const pipelines = Number(env('DA_PIPELINES'));
  await page.goto('/ui/');
  await expect(page.locator('tbody tr')).toHaveCount(pipelines);
  await expect(page.locator('.conn')).toHaveText('live', { timeout: 15_000 });
  // A pipeline's page draws its tables from the figures the stream sent; until they come (each
  // node sends its pipelines' every 5 s, once the page follows it) it reads them from the node that
  // runs it. A pipeline's worker draws none until it runs, and a node starts its workers one after
  // another. The moves are timed once the stream has sent every pipeline's figures with its tables.
  await page.evaluate(
    (n) =>
      new Promise<void>((done, fail) => {
        const ready = new Set<string>();
        const es = new EventSource('/v1/stream?kind=figures');
        const end = setTimeout(() => (es.close(), fail(new Error(`tables for ${ready.size} of ${n}`))), 45_000);
        es.addEventListener('figures', (e) => {
          const f = JSON.parse((e as MessageEvent).data);
          if (f.tables?.length > 0) ready.add(f.id);
          if (ready.size === n) (clearTimeout(end), es.close(), done());
        });
      }),
    pipelines,
  );
  const moves = [
    ['the Overview to the pipelines', await move(page, 'nav a[href="/ui/pipelines"]', 'tbody a.open', pipelines)],
    ['the pipelines to a pipeline', await move(page, 'tbody a.open[href="/ui/pipelines/p01"]', '.cols tbody td a', 1)],
    ['a pipeline back to the pipelines', await move(page, null, 'tbody a.open', pipelines)],
    ['the pipelines to the Overview', await move(page, 'nav a[href="/ui/"]', '.tile', 5)],
    ['the Overview to the Cluster', await move(page, 'nav a[href="/ui/cluster"]', '.leases tbody tr', pipelines)],
    ['the Cluster to a node', await move(page, '.members a[href="/ui/cluster/nodes/n2"]', '.pipelines tbody tr a[href^="/ui/pipelines/"]', 1)],
  ] as const;
  for (const [what, ms] of moves) console.log(`${what}: ${Math.round(ms)} ms`);
  for (const [what, ms] of moves) expect(ms, what).toBeLessThan(MOVE_MS);
});
