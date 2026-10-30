// The documentation site (M11b): Starlight, built by `npm run build` into `dist/`, deployed to
// GitHub Pages by `.github/workflows/docs.yml`. The links validator fails the build on a link to
// a missing page or anchor. `src/content/docs/reference/` and `public/openapi.json` are
// generated: `cargo xtask docs`.
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';
import mermaid from 'astro-mermaid';

export default defineConfig({
  site: 'https://alex-karpenko.github.io',
  base: '/data-avalanche',
  integrations: [
    mermaid({ autoTheme: true }),
    starlight({
      title: 'data-avalanche',
      customCss: ['./src/styles/custom.css'],
      description: 'Change data capture from PostgreSQL, MySQL and MariaDB into Delta Lake tables on object storage.',
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/alex-karpenko/data-avalanche' },
      ],
      editLink: { baseUrl: 'https://github.com/alex-karpenko/data-avalanche/edit/main/docs/' },
      plugins: [
        starlightLinksValidator({
          // A file of `public/`, not a page.
          exclude: ['/data-avalanche/openapi.json'],
        }),
      ],
      sidebar: [
        { label: 'Overview', slug: 'index' },
        { label: 'Getting started', slug: 'getting-started' },
        { label: 'Configuration', items: [{ autogenerate: { directory: 'configuration' } }] },
        { label: 'Architecture', items: [{ autogenerate: { directory: 'architecture' } }] },
        { label: 'Deployment', items: [{ autogenerate: { directory: 'deployment' } }] },
        { label: 'Runbooks', items: [{ autogenerate: { directory: 'runbooks' } }] },
        { label: 'Reference', items: [{ autogenerate: { directory: 'reference' } }] },
      ],
    }),
  ],
});
