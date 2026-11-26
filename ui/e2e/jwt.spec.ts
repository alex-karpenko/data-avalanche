import { expect, test } from '@playwright/test';
import { env, overview } from './common';

const header = (token: string) => ({ Authorization: `Bearer ${token}` });
const pipeline = env('DA_PIPELINE');

test.describe('with the proxy sending a viewer token', () => {
  test.use({ extraHTTPHeaders: header(env('DA_VIEWER_TOKEN')) });
  test('the Overview is shown, with no sign-out', async ({ page }) => {
    await page.goto('/ui/');
    await overview(page, 'alice', 'viewer');
    await expect(page.locator('form[action="/ui/sign-out"]')).toHaveCount(0);
  });

  test('the pipelines are shown with nothing to select or act on', async ({ page }) => {
    await page.goto('/ui/pipelines');
    const open = page.locator(`tbody a.open[href="/ui/pipelines/${pipeline}"]`);
    await expect(open).toBeVisible();
    await expect(page.locator('input[type="checkbox"]')).toHaveCount(0);
    await open.click();
    await expect(page.locator('h2')).toContainText(pipeline);
    await expect(page.locator('.where')).toBeVisible();
    await expect(page.locator('.actions')).toHaveCount(0);
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
