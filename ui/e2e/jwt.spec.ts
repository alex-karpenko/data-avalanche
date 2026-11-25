import { expect, test } from '@playwright/test';
import { env, overview } from './common';

const header = (token: string) => ({ Authorization: `Bearer ${token}` });

test.describe('with the proxy sending a viewer token', () => {
  test.use({ extraHTTPHeaders: header(env('DA_VIEWER_TOKEN')) });
  test('the Overview is shown, with no sign-out', async ({ page }) => {
    await page.goto('/ui/');
    await overview(page, 'alice', 'viewer');
    await expect(page.locator('form[action="/ui/sign-out"]')).toHaveCount(0);
  });
});

test.describe('with a token whose roles map to none', () => {
  test.use({ extraHTTPHeaders: header(env('DA_UNMAPPED_TOKEN')) });
  test('the page says Forbidden', async ({ page }) => {
    const r = await page.goto('/ui/');
    expect(r?.status()).toBe(403);
    await expect(page.locator('.title')).toHaveText('Forbidden');
  });
});

test('without a token the page says so', async ({ page }) => {
  const r = await page.goto('/ui/');
  expect(r?.status()).toBe(401);
  await expect(page.locator('.title')).toHaveText('No valid token');
});
