import { expect, test } from '@playwright/test';
import { overview } from './common';

test('without authentication everyone is admin, and the page says so', async ({ page }) => {
  await page.goto('/ui/');
  await overview(page, 'anonymous', 'admin');
  await expect(page.locator('.banner')).toHaveText(
    'This cluster has no authentication: everyone who reaches the console is admin.',
  );
});
