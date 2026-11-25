import { describe, expect, it } from 'vitest';
import { isAppLink, sectionOf } from './router.svelte';

describe('the pages', () => {
  it('are named by their first name under /ui/', () => {
    expect(sectionOf('/ui/')).toBe('overview');
    expect(sectionOf('/ui/pipelines')).toBe('pipelines');
    expect(sectionOf('/ui/pipelines/orders?tab=tables')).toBe('pipelines');
    expect(sectionOf('/ui/nowhere')).toBe('overview');
  });

  it("follow a plain click on the app's own links only", () => {
    const link = (href: string, attrs: Record<string, string> = {}) => {
      const a = document.createElement('a');
      a.href = href;
      for (const [k, v] of Object.entries(attrs)) a.setAttribute(k, v);
      return a;
    };
    const click = (init: MouseEventInit = {}) => new MouseEvent('click', { button: 0, ...init });
    expect(isAppLink(link('/ui/pipelines'), click())).toBe(true);
    expect(isAppLink(link('/ui/pipelines'), click({ metaKey: true }))).toBe(false);
    expect(isAppLink(link('/ui/pipelines', { target: '_blank' }), click())).toBe(false);
    expect(isAppLink(link('/ui/sign-in?next=%2Fui%2F'), click())).toBe(false);
    expect(isAppLink(link('/ui/app/index-x.js'), click())).toBe(false);
    expect(isAppLink(link('/v1/openapi.json'), click())).toBe(false);
    expect(isAppLink(link('https://example.com/ui/'), click())).toBe(false);
  });
});
