import { expect, type Page } from '@playwright/test';

/** A value the E2E suite passes; the flows don't run without it. */
export function env(name: string): string {
  const v = process.env[name];
  if (v === undefined || v === '') throw new Error(`${name} isn't set: run the flows through the E2E suite`);
  return v;
}

/** Signs in through the `basic` form the page shows. */
export async function signIn(page: Page, user: string, password: string): Promise<void> {
  await page.fill('input[name="user"]', user);
  await page.fill('input[name="password"]', password);
  await page.click('button[type="submit"]');
}

/** The Overview's pipeline row of `id`. */
export function row(page: Page, id: string) {
  return page.locator('tbody tr', { has: page.locator(`a[href="/ui/pipelines/${id}"]`) });
}

/** A call the page makes to `/v1` with its session's anti-forgery token, or without it. */
export async function act(page: Page, path: string, token = true): Promise<number> {
  return page.evaluate(
    async ({ path, token }) => {
      const s = (await (await fetch('/ui/session')).json()) as { csrf: string | null };
      const headers: Record<string, string> = token && s.csrf ? { 'X-CSRF-Token': s.csrf } : {};
      return (await fetch(path, { method: 'POST', headers })).status;
    },
    { path, token },
  );
}

/** The page shows the Overview, live, for `name` with `role`. */
export async function overview(page: Page, name: string, role: string): Promise<void> {
  await expect(page.locator('h2')).toHaveText('Overview');
  await expect(page.locator('.who')).toContainText(name);
  await expect(page.locator('.who b')).toHaveText(role);
  await expect(page.locator('.conn')).toHaveText('live', { timeout: 15_000 });
}
