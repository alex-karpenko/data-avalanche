import { svelte } from '@sveltejs/vite-plugin-svelte';
import { defineConfig } from 'vitest/config';

// The node serves the app under /ui/ (Admin_Console §10), its built files under /ui/app/: the
// server's own pages keep /ui/assets/.
export default defineConfig(({ mode }) => ({
  base: '/ui/',
  plugins: [svelte()],
  build: { assetsDir: 'app', target: 'es2022' },
  // The tests mount components in jsdom: Svelte's browser build.
  resolve: mode === 'test' ? { conditions: ['browser'] } : undefined,
  test: { environment: 'jsdom', include: ['src/**/*.test.ts'] },
}));
