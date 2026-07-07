import { useState, useEffect } from 'react';
import AccessSidebar from './components/AccessSidebar';
import GroupPermissionChecker from './pages/GroupPermissionChecker';
import IdentityAudit from './pages/IdentityAudit';

// End-user "Access Control" app — a separate Vite build entry (see
// access-control/index.html + vite.config.js's rollupOptions.input) so this
// audience only ever downloads these two features' code, not the full
// Pattern Assessment bundle (quiz, Payload Library, OPA/Ranger, HSM Demo,
// Governance Lifecycle). The page components themselves are imported
// unchanged from ../pages — same files the main app uses, no duplication.

function useIsMobile() {
  const [isMobile, setIsMobile] = useState(() => window.innerWidth < 768);
  useEffect(() => {
    const handler = () => setIsMobile(window.innerWidth < 768);
    window.addEventListener('resize', handler);
    return () => window.removeEventListener('resize', handler);
  }, []);
  return isMobile;
}

// Distinct API root for the end-user audience — same backend routers as the
// main app (see server.js, mounted at both /api/pattern/assessment and
// /api/access/control), just a different path so this audience's network
// requests are visibly, distinctly namespaced from the app-user surface.
// Derived directly from BASE_URL (the single source of truth for this repo's
// Istio path prefix — see ADR-009) rather than hardcoded, so it still tracks
// correctly if that prefix ever changes.
const API_BASE = import.meta.env.BASE_URL.replace('pattern/assessment', 'access/control');

// Views are addressed by URL *hash* (#identity-audit), not a real pushState
// path. With only 2 tabs, a real server-visible path bought little (still
// bookmarkable via hash) but cost real risk: a page-route slug and a backend
// API route slug can collide by name (this app hit exactly that — the page
// route and the /identity-audit POST endpoint shared a name, so a direct
// visit or hard refresh landed on the API's 404 instead of the page). Hash
// fragments are never sent to the server, so this class of bug is now
// structurally impossible rather than merely avoided by careful naming.
const HASH_TO_VIEW = { '#identity-audit': 'identityAudit', '#permission-checker': 'permissionChecker' };
const VIEW_TO_HASH = { identityAudit: '#identity-audit', permissionChecker: '#permission-checker' };

function viewFromHash(hash) {
  return HASH_TO_VIEW[hash] || 'permissionChecker';
}

export default function AccessApp() {
  const [currentView, setCurrentView] = useState(() => viewFromHash(window.location.hash));
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const isMobile = useIsMobile();

  const [theme, setTheme] = useState(() => localStorage.getItem('pa-theme') || 'light');
  useEffect(() => {
    const root = document.documentElement;
    theme === 'light' ? root.removeAttribute('data-theme') : root.setAttribute('data-theme', theme);
    localStorage.setItem('pa-theme', theme);
  }, [theme]);

  useEffect(() => {
    function onHashChange() {
      setCurrentView(viewFromHash(window.location.hash));
    }
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);

  function handleNavigate(view) {
    window.location.hash = VIEW_TO_HASH[view] || VIEW_TO_HASH.permissionChecker;
    setCurrentView(view);
    setSidebarOpen(false);
  }

  return (
    <div style={styles.layout}>
      {isMobile && (
        <button style={styles.hamburger} onClick={() => setSidebarOpen((o) => !o)} aria-label="Toggle menu">
          {sidebarOpen ? '✕' : '☰'}
        </button>
      )}

      <AccessSidebar
        currentView={currentView}
        onNavigate={handleNavigate}
        isOpen={sidebarOpen}
        onToggle={() => setSidebarOpen((o) => !o)}
        isMobile={isMobile}
        theme={theme}
        onThemeChange={setTheme}
      />

      <main style={styles.main}>
        {currentView === 'permissionChecker' && <GroupPermissionChecker apiBase={API_BASE} />}
        {currentView === 'identityAudit' && <IdentityAudit apiBase={API_BASE} />}
      </main>
    </div>
  );
}

const styles = {
  layout: { display: 'flex', minHeight: '100vh', background: 'var(--bg)', position: 'relative' },
  main: { flex: 1, overflowY: 'auto', minWidth: 0 },
  hamburger: {
    position: 'fixed', top: '1rem', left: '1rem', zIndex: 100, width: 40, height: 40,
    borderRadius: '10px', border: '1.5px solid var(--border)', background: 'var(--surface)',
    fontSize: '1.1rem', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center',
    fontFamily: 'inherit', boxShadow: 'var(--shadow)', color: 'var(--text-primary)',
  },
};
