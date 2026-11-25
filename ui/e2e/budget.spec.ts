import { expect, test } from '@playwright/test';
import { env } from './common';

/** Admin_Console §8: the first page drawn under 1 s, with 50 pipelines. */
const FIRST_PAGE_MS = 1_000;

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
