// The console's pages under `/ui/` (Admin_Console §4): each section at its own address, so a
// link shows the same view; links move between pages without loading the app again, and Back
// works. The server's own routes under `/ui/` (sign-in, sign-out, the app's files) are left to
// the browser.

export const SECTIONS = [
  { id: 'overview', label: 'Overview', path: '/ui/' },
  { id: 'cluster', label: 'Cluster', path: '/ui/cluster' },
  { id: 'pipelines', label: 'Pipelines', path: '/ui/pipelines' },
  { id: 'tables', label: 'Tables', path: '/ui/tables' },
  { id: 'snapshots', label: 'Snapshots', path: '/ui/snapshots' },
  { id: 'events', label: 'Events', path: '/ui/events' },
  { id: 'config', label: 'Config', path: '/ui/config' },
] as const;
export type Section = (typeof SECTIONS)[number]['id'];

/** The server's routes under `/ui/`, which aren't the app's pages. */
const SERVER = ['/ui/sign-in', '/ui/sign-out', '/ui/session', '/ui/oauth2/', '/ui/assets/', '/ui/app/'];

/** The section a path under `/ui/` shows: its first name after `/ui/`, the Overview else. */
export function sectionOf(path: string): Section {
  const first = path.replace(/^\/ui\/?/, '').split(/[/?#]/)[0];
  return SECTIONS.find((s) => s.id === first)?.id ?? 'overview';
}

/** Whether a click on `a` is the app's to handle: a plain left click on one of its pages. */
export function isAppLink(a: HTMLAnchorElement, e: MouseEvent): boolean {
  if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) {
    return false;
  }
  if (a.target !== '' || a.hasAttribute('download') || a.origin !== location.origin) return false;
  const path = a.pathname;
  return path.startsWith('/ui/') && !SERVER.some((p) => path === p || path.startsWith(p));
}

/** Where the page is: its path and query. */
export class Router {
  path = $state(location.pathname + location.search);
  section = $derived(sectionOf(this.path));

  /** Follows the app's links, and Back and Forward; returns the way to stop. */
  listen(): () => void {
    const click = (e: MouseEvent) => {
      const a = (e.target as Element | null)?.closest('a');
      if (a === null || a === undefined || !isAppLink(a, e)) return;
      e.preventDefault();
      this.go(a.pathname + a.search);
    };
    const pop = () => (this.path = location.pathname + location.search);
    document.addEventListener('click', click);
    window.addEventListener('popstate', pop);
    return () => {
      document.removeEventListener('click', click);
      window.removeEventListener('popstate', pop);
    };
  }

  go(to: string): void {
    if (to === this.path) return;
    history.pushState(null, '', to);
    this.path = to;
    window.scrollTo(0, 0);
  }
}
