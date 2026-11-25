import { expect, test } from '@playwright/test';

test('a cluster with the console off serves none of it', async ({ page }) => {
  for (const path of ['/ui/', '/ui/sign-in', '/ui/app/index.js']) {
    const r = await page.goto(path);
    expect(r?.status()).toBe(404);
    await expect(page.locator('body')).toContainText('globals.console.enabled');
  }
});
