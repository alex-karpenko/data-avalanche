import { defineConfig, devices } from '@playwright/test';

// The console's flows in a browser (Admin_Console §10). The E2E suite starts the cluster each
// file needs and gives its address in DA_CONSOLE_URL (crates/data-avalanche/tests/e2e/
// console_app.rs): run them through it, not alone.
export default defineConfig({
  testDir: 'e2e',
  workers: 1,
  retries: 0,
  timeout: 60_000,
  reporter: 'list',
  use: { baseURL: process.env.DA_CONSOLE_URL, trace: 'retain-on-failure' },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
});
