import { expect, test } from '@playwright/test';
import { act, env, overview, row, signIn } from './common';

const user = env('DA_USER');
const password = env('DA_PASSWORD');
const pipeline = env('DA_PIPELINE');

test('a page asked for without a session goes to the sign-in, then back to it', async ({ page }) => {
  await page.goto('/ui/pipelines');
  await expect(page).toHaveURL(/\/ui\/sign-in\?next=%2Fui%2Fpipelines$/);
  await signIn(page, user, 'not the password');
  await expect(page.locator('[role="alert"]')).toBeVisible();
  await signIn(page, user, password);
  await expect(page).toHaveURL(/\/ui\/pipelines$/);
  await expect(page.locator('nav [aria-current="page"]')).toHaveText('Pipelines');
  await expect(page.locator('h2')).toHaveText('Pipelines');
  await page.click('nav >> text=Overview');
  await expect(page).toHaveURL(/\/ui\/$/);
  await overview(page, user, 'admin');
});

test('the Overview is live: an action shows as it happens', async ({ page }) => {
  await page.goto('/ui/');
  await signIn(page, user, password);
  await overview(page, user, 'admin');
  await expect(page.locator('.tile .v').first()).toHaveText('1');
  await expect(row(page, pipeline).locator('.chip')).toHaveText('running');
  // Without the anti-forgery token the session may not act; with it, it does.
  expect(await act(page, `/v1/pipelines/${pipeline}/pause`, false)).toBe(403);
  expect(await act(page, `/v1/pipelines/${pipeline}/pause`)).toBe(202);
  await expect(row(page, pipeline).locator('.chip')).toHaveText('paused', { timeout: 15_000 });
  await expect(page.locator('.ev')).toContainText(`pause ${pipeline} by ${user} (admin)`);
  expect(await act(page, `/v1/pipelines/${pipeline}/resume`)).toBe(202);
  await expect(row(page, pipeline).locator('.chip')).toHaveText('running', { timeout: 15_000 });
});

test('signing out ends the session; a session that no longer opens says why', async ({
  page,
  context,
}) => {
  await page.goto('/ui/');
  await signIn(page, user, password);
  await overview(page, user, 'admin');
  await page.click('text=Sign out');
  await expect(page).toHaveURL(/\/ui\/sign-in\?signed_out$/);
  await page.goto('/ui/');
  await expect(page).toHaveURL(/\/ui\/sign-in\?next=/);
  // Signed in again, the session's cookie altered: it no longer opens.
  await signIn(page, user, password);
  await overview(page, user, 'admin');
  const altered = (await context.cookies()).map((c) =>
    c.name === '__Host-da_session' ? { ...c, value: `${c.value.slice(0, -4)}AAAA` } : c,
  );
  await context.clearCookies();
  await context.addCookies(altered);
  const ended = await page.goto('/ui/');
  expect(ended?.status()).toBe(401);
  await expect(page.locator('.title')).toHaveText('Your session has ended');
});
