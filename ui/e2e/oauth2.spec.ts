import { expect, test } from '@playwright/test';
import { env, overview, row } from './common';

const pipeline = env('DA_PIPELINE');

test("the provider's sign-in comes back to the console, and the sign-out goes on to it", async ({
  page,
}) => {
  await page.goto('/ui/');
  await expect(page).toHaveURL(/\/ui\/sign-in\?next=/);
  await page.click('#continue');
  await overview(page, 'alice.smith', 'operator');
  await expect(row(page, pipeline).locator('.chip')).toHaveText('running');
  await page.click('text=Sign out');
  await expect(page).toHaveURL(/\/end-session\?/);
  await page.goto('/ui/');
  await expect(page).toHaveURL(/\/ui\/sign-in\?next=/);
});
