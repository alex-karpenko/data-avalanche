import { expect, test, type Page } from '@playwright/test';
import { env, signIn } from './common';

const user = env('DA_USER');
const password = env('DA_PASSWORD');
const pipeline = env('DA_PIPELINE');

/** Signs in on the way to `path`, and waits for the page to be live. */
async function signedIn(page: Page, path: string): Promise<void> {
  await page.goto(path);
  await signIn(page, user, password);
  await expect(page.locator('.conn')).toHaveText('live', { timeout: 15_000 });
}

const dialog = (page: Page) => page.locator('[role="dialog"]');

test('the list keeps its filters in the address, opens a row from the keyboard, and Back keeps them', async ({
  page,
}) => {
  await signedIn(page, '/ui/pipelines');
  await expect(page.locator('h2')).toHaveText('Pipelines');
  const row = page.locator('tbody tr', { has: page.locator(`a.open[href="/ui/pipelines/${pipeline}"]`) });
  await expect(row).toContainText(" → Δ ");
  await expect(row.locator('.chip')).toHaveText('running', { timeout: 15_000 });
  await page.click('.chips >> text=Paused');
  await expect(page).toHaveURL(/\/ui\/pipelines\?show=paused$/);
  await expect(page.locator('tbody')).toContainText('No pipeline shown');
  await page.click('.chips >> text=Running');
  await expect(page).toHaveURL(/\/ui\/pipelines\?show=running$/);
  await row.locator('a.open').focus();
  await page.keyboard.press('Enter');
  await expect(page).toHaveURL(new RegExp(`/ui/pipelines/${pipeline}$`));
  await expect(page.locator('h2')).toContainText(pipeline);
  await page.goBack();
  await expect(page).toHaveURL(/\/ui\/pipelines\?show=running$/);
  await expect(page.locator('.chips [aria-current="true"]')).toContainText('Running');
});

test("a pipeline's page: its tables, source and lag chart; paused and resumed through their dialogs, each recorded with its note", async ({
  page,
}) => {
  await signedIn(page, `/ui/pipelines/${pipeline}`);
  await expect(page.locator('.where')).toContainText('PostgreSQL');
  await expect(page.locator(`a[href^="/ui/tables/${pipeline}/"]`).first()).toBeVisible();
  // The worker reads its slot at its first heartbeat (60 s by default): until then the page says so.
  await expect(page.locator('aside')).toContainText(/Slot|Not read yet/);
  // uPlot draws under the console's Content-Security-Policy.
  await expect(page.locator('figure.chart canvas')).toBeVisible();
  await page.click('.actions >> text=Pause');
  await expect(dialog(page)).toContainText(`Recorded as: paused by ${user} (admin)`);
  await page.fill('[role="dialog"] textarea', 'maintenance window');
  await page.click('[role="dialog"] button.go');
  await expect(dialog(page)).toHaveCount(0);
  await expect(page.locator('h2 .chip')).toHaveText('paused', { timeout: 15_000 });
  await expect(page.locator('.activity')).toContainText('"maintenance window"');
  await page.click('.actions >> text=Resume');
  await page.click('[role="dialog"] button.go');
  await expect(page.locator('h2 .chip')).toHaveText('running', { timeout: 15_000 });
});

test('an action on the pipelines selected', async ({ page }) => {
  await signedIn(page, '/ui/pipelines');
  await page.check(`input[aria-label="Select ${pipeline}"]`);
  await page.click('.acting >> text=Flush…');
  await expect(dialog(page).locator('h3')).toHaveText(`Flush ${pipeline}?`);
  await page.click('[role="dialog"] button.go');
  await expect(dialog(page)).toHaveCount(0);
  await expect(page.locator('.acting')).toHaveCount(0);
});
