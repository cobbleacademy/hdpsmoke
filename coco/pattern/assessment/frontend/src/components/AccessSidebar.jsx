// Minimal sidebar for the end-user "Access Control" app — deliberately not a
// variant of the main app's Sidebar.jsx. That component is sized for 11 items
// plus a hasResult/lock-icon quiz concept, a desktop icon-rail collapse, etc.
// This app only ever has 2 destinations, so a dedicated, much smaller
// component is simpler than threading a "mode" prop through the full one.
// Visual language (tokens, fonts, spacing) intentionally matches Sidebar.jsx
// so this still feels like part of the same product family.

const NAV_ITEMS = [
  {
    id: 'permissionChecker',
    label: 'Permission Checker',
    icon: '🧭',
    description: 'Group access tier evaluation',
  },
  {
    id: 'identityAudit',
    label: 'Identity Audit',
    icon: '🔎',
    description: 'Entra group footprint audit',
  },
];

const THEMES = [
  { id: 'light', icon: '☀️', label: 'Light' },
  { id: 'dim',   icon: '🌙', label: 'Dim'   },
  { id: 'slate', icon: '▪️',  label: 'Slate' },
  { id: 'mocha', icon: '☕', label: 'Mocha' },
];

export default function AccessSidebar({ currentView, onNavigate, isOpen, onToggle, isMobile, theme, onThemeChange }) {
  return (
    <>
      {isMobile && isOpen && <div onClick={onToggle} style={styles.backdrop} />}

      <aside
        style={{
          ...styles.sidebar,
          ...(isMobile
            ? { position: 'fixed', left: isOpen ? 0 : -240, boxShadow: isOpen ? '4px 0 32px rgba(0,0,0,0.14)' : 'none' }
            : { position: 'sticky', left: 0 }),
        }}
      >
        <div style={styles.brand}>
          <div style={styles.brandIcon}>🔐</div>
          <div>
            <div style={styles.brandName}>Access Control</div>
            <div style={styles.brandSub}>User access management</div>
          </div>
        </div>

        <nav style={styles.nav}>
          <p style={styles.sectionLabel}>NAVIGATION</p>
          {NAV_ITEMS.map((item) => {
            const active = currentView === item.id;
            return (
              <button
                key={item.id}
                onClick={() => onNavigate(item.id)}
                title={item.description}
                style={{ ...styles.navItem, ...(active ? styles.navItemActive : {}) }}
              >
                <span style={styles.navIcon}>{item.icon}</span>
                <div style={styles.navText}>
                  <span style={{ ...styles.navLabel, ...(active ? { color: 'var(--accent)' } : {}) }}>
                    {item.label}
                  </span>
                  <span style={styles.navDesc}>{item.description}</span>
                </div>
                {active && <span style={styles.activeBar} />}
              </button>
            );
          })}
        </nav>

        <div style={styles.themeSection}>
          <p style={styles.sectionLabel}>APPEARANCE</p>
          <div style={styles.modeRow}>
            {THEMES.map((t) => (
              <button
                key={t.id}
                onClick={() => onThemeChange(t.id)}
                title={t.label}
                style={{ ...styles.modeBtn, ...(theme === t.id ? styles.modeBtnActive : {}) }}
              >
                <span style={styles.modeIcon}>{t.icon}</span>
                <span style={styles.modeName}>{t.label}</span>
              </button>
            ))}
          </div>
        </div>

        <div style={styles.footer}>
          <span style={styles.footerText}>v1.0 · Access Control</span>
        </div>
      </aside>
    </>
  );
}

const styles = {
  backdrop: { position: 'fixed', inset: 0, background: 'rgba(26, 23, 48, 0.45)', zIndex: 98, backdropFilter: 'blur(3px)', WebkitBackdropFilter: 'blur(3px)' },
  sidebar: {
    width: 240, minWidth: 240, top: 0, height: '100vh', background: 'var(--surface)',
    borderRight: '1px solid var(--border)', display: 'flex', flexDirection: 'column',
    zIndex: 99, transition: 'left 0.28s cubic-bezier(0.4, 0, 0.2, 1)', overflowY: 'auto',
  },
  brand: { display: 'flex', alignItems: 'center', gap: '0.75rem', padding: '1.375rem 1.25rem 1.25rem', borderBottom: '1px solid var(--border)' },
  brandIcon: { fontSize: '1.5rem', lineHeight: 1, flexShrink: 0 },
  brandName: { fontSize: '0.9375rem', fontWeight: 700, color: 'var(--text-primary)', lineHeight: 1.2 },
  brandSub: { fontSize: '0.68rem', color: 'var(--text-secondary)', marginTop: '2px' },
  nav: { flex: 1, padding: '1rem 0.75rem', display: 'flex', flexDirection: 'column', gap: '2px' },
  sectionLabel: { fontSize: '0.62rem', fontWeight: 700, letterSpacing: '0.1em', color: 'var(--text-secondary)', padding: '0 0.625rem', marginBottom: '0.5rem', marginTop: '0.25rem' },
  navItem: {
    display: 'flex', alignItems: 'center', gap: '0.625rem', padding: '0.625rem 0.75rem',
    borderRadius: '10px', border: 'none', background: 'transparent', cursor: 'pointer',
    width: '100%', textAlign: 'left', fontFamily: 'inherit', color: 'var(--text-primary)',
    transition: 'all 0.15s ease', position: 'relative',
  },
  navItemActive: { background: 'var(--accent-light)' },
  navIcon: { fontSize: '1.05rem', width: '1.375rem', textAlign: 'center', flexShrink: 0, lineHeight: 1 },
  navText: { display: 'flex', flexDirection: 'column', flex: 1, minWidth: 0 },
  navLabel: { fontSize: '0.85rem', fontWeight: 600, color: 'var(--text-primary)', lineHeight: 1.2 },
  navDesc: { fontSize: '0.68rem', color: 'var(--text-secondary)', marginTop: '1px', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' },
  activeBar: { position: 'absolute', right: 0, top: '20%', bottom: '20%', width: 3, borderRadius: '3px 0 0 3px', background: 'var(--accent)' },
  themeSection: { padding: '0.75rem 0.75rem 0.5rem', borderTop: '1px solid var(--border)' },
  modeRow: { display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '4px', marginBottom: '0.5rem' },
  modeBtn: {
    display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '2px', padding: '0.35rem 0.2rem',
    borderRadius: '8px', border: '1.5px solid transparent', background: 'transparent', cursor: 'pointer',
    fontFamily: 'inherit', color: 'var(--text-secondary)', transition: 'all 0.15s',
  },
  modeBtnActive: { background: 'var(--accent-light)', border: '1.5px solid var(--accent)', color: 'var(--accent)' },
  modeIcon: { fontSize: '0.85rem', lineHeight: 1 },
  modeName: { fontSize: '0.56rem', fontWeight: 700, letterSpacing: '0.02em', textTransform: 'uppercase' },
  footer: { padding: '0.875rem 1.25rem', borderTop: '1px solid var(--border)' },
  footerText: { fontSize: '0.68rem', color: 'var(--text-secondary)' },
};
