import { expect, test, type Page } from '@playwright/test';
import { env, signIn } from './common';

const user = env('DA_USER');
const password = env('DA_PASSWORD');

// The flows change the cluster (a leave, then a shutdown): they run in this order.
test.describe.configure({ mode: 'serial' });

/** Signs in on the way to `path`, and waits for the page to be live. */
async function signedIn(page: Page, path: string): Promise<void> {
  await page.goto(path);
  await signIn(page, user, password);
  await expect(page.locator('.conn')).toHaveText('live', { timeout: 15_000 });
}

const dialog = (page: Page) => page.locator('[role="dialog"]');
const member = (page: Page, n: string) => page.locator('.members tbody tr', { has: page.locator(`a[href="/ui/cluster/nodes/${n}"]`) });

test("the Cluster page: three members alive, their leases, and the console's status from the user menu", async ({ page }) => {
  await signedIn(page, '/ui/cluster');
  await expect(page.locator('h2')).toHaveText('Cluster');
  await expect(page.locator('.members tbody tr')).toHaveCount(3);
  for (const n of ['n1', 'n2', 'n3']) await expect(member(page, n)).toContainText('alive');
  await expect(page.locator('.leases tbody tr')).toHaveCount(3);
  await page.click('.who summary');
  const status = (label: string) =>
    page.locator('.who .status tr', { has: page.getByRole('cell', { name: label, exact: true }) });
  await expect(status('Live updates')).toContainText('connected via n1');
  await expect(status('Nodes answering')).toContainText('3 of 3');
  await expect(status('Built-in history')).toContainText('24 h');
  await expect(status('Prometheus')).toContainText('not configured');
  await expect(status('Event store')).toContainText('ok 30 days');
});

test("a node's page: its pipelines, its charts drawn, what its gossip sees", async ({ page }) => {
  await signedIn(page, '/ui/cluster');
  await page.click('.members a[href="/ui/cluster/nodes/n2"]');
  await expect(page).toHaveURL(/\/ui\/cluster\/nodes\/n2$/);
  await expect(page.locator('h2 .chip')).toHaveText('alive');
  await expect(page.locator('.charts canvas')).toHaveCount(3);
  await expect(page.locator('.gossip')).toContainText(/n1 alive/, { timeout: 15_000 });
});

test('n3 leaves through its dialog with a note, and the others carry on without it', async ({ page }) => {
  await signedIn(page, '/ui/cluster/nodes/n3');
  await page.click('.actions >> text=Leave…');
  await expect(dialog(page)).toContainText('n3 leaves the cluster?');
  await expect(dialog(page)).toContainText(`Recorded as: n3 left the cluster, asked by ${user} (admin)`);
  await page.fill('[role="dialog"] textarea', 'node pool upgrade');
  await page.click('[role="dialog"] button.go');
  await expect(dialog(page)).toHaveCount(0);
  await page.click('nav a[href="/ui/cluster"]');
  await expect(member(page, 'n3')).toHaveCount(0, { timeout: 60_000 });
  await expect(page.locator('.leases tbody tr')).toHaveCount(3);
});

test('n2 shuts down only once its name is typed', async ({ page }) => {
  await signedIn(page, '/ui/cluster/nodes/n2');
  await page.click('.actions >> text=Shut down…');
  const go = page.locator('[role="dialog"] button.go');
  await expect(dialog(page)).toContainText('Type n2 to confirm.');
  await expect(go).toBeDisabled();
  await page.fill('[role="dialog"] input.typed', 'n');
  await expect(go).toBeDisabled();
  await page.fill('[role="dialog"] input.typed', 'n2');
  await page.click('[role="dialog"] button.go');
  await expect(dialog(page)).toHaveCount(0);
  await page.click('nav a[href="/ui/cluster"]');
  await expect(member(page, 'n2')).toHaveCount(0, { timeout: 60_000 });
  await expect(page.locator('.members tbody tr')).toHaveCount(1);
});
