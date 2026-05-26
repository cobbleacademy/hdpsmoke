const NAV_ITEMS = [
  {
    id: 'quiz',
    label: 'Take Assessment',
    icon: '▶',
    description: 'Start or retake the quiz',
    alwaysOn: true,
  },
  {
    id: 'result',
    label: 'My Result',
    icon: '📊',
    description: 'View your pattern result',
    needsResult: true,
  },
  {
    id: 'explorer',
    label: 'Pattern Explorer',
    icon: '🗺️',
    description: 'Browse all 4 patterns',
    alwaysOn: true,
  },
  {
    id: 'howItWorks',
    label: 'How It Works',
    icon: 'ℹ️',
    description: 'See the scoring method',
    alwaysOn: true,
  },
  {
    id: 'payloadLibrary',
    label: 'Payload Library',
    icon: '📦',
    description: 'Browse sample API payloads',
    alwaysOn: true,
  },
];

export default function Sidebar({ currentView, onNavigate, hasResult, isOpen, onToggle, isMobile }) {
  return (
    <>
      {/* Mobile backdrop overlay */}
      {isMobile && isOpen && (
        <div onClick={onToggle} style={styles.backdrop} />
      )}

      <aside
        style={{
          ...styles.sidebar,
          ...(isMobile
            ? { position: 'fixed', left: isOpen ? 0 : -260, boxShadow: isOpen ? '4px 0 32px rgba(0,0,0,0.14)' : 'none' }
            : { position: 'sticky', left: 0 }),
        }}
      >
        {/* Brand */}
        <div style={styles.brand}>
          <div style={styles.brandIcon}>🧠</div>
          <div>
            <div style={styles.brandName}>Pattern App</div>
            <div style={styles.brandSub}>Work-style assessment</div>
          </div>
        </div>

        {/* Navigation */}
        <nav style={styles.nav}>
          <p style={styles.sectionLabel}>NAVIGATION</p>

          {NAV_ITEMS.map((item) => {
            const enabled = item.alwaysOn || (item.needsResult && hasResult);
            const active = currentView === item.id;

            return (
              <button
                key={item.id}
                disabled={!enabled}
                onClick={() => enabled && onNavigate(item.id)}
                title={!enabled ? 'Complete the quiz to unlock' : item.description}
                style={{
                  ...styles.navItem,
                  ...(active ? styles.navItemActive : {}),
                  ...(!enabled ? styles.navItemDisabled : {}),
                }}
              >
                <span style={styles.navIcon}>{item.icon}</span>
                <div style={styles.navText}>
                  <span style={{ ...styles.navLabel, ...(active ? { color: 'var(--accent)' } : {}) }}>
                    {item.label}
                  </span>
                  <span style={styles.navDesc}>{item.description}</span>
                </div>
                {item.needsResult && !hasResult && (
                  <span style={styles.lockIcon} title="Complete quiz to unlock">🔒</span>
                )}
                {active && <span style={styles.activeBar} />}
              </button>
            );
          })}
        </nav>

        {/* Quiz progress indicator */}
        {hasResult && (
          <div style={styles.resultBadge}>
            <span style={styles.resultDot} />
            <span style={styles.resultText}>Result ready</span>
          </div>
        )}

        {/* Footer */}
        <div style={styles.footer}>
          <span style={styles.footerText}>v1.0 · Pattern Assessment</span>
        </div>
      </aside>
    </>
  );
}

const styles = {
  backdrop: {
    position: 'fixed',
    inset: 0,
    background: 'rgba(26, 23, 48, 0.45)',
    zIndex: 98,
    backdropFilter: 'blur(3px)',
    WebkitBackdropFilter: 'blur(3px)',
  },
  sidebar: {
    width: 240,
    minWidth: 240,
    top: 0,
    height: '100vh',
    background: 'var(--surface)',
    borderRight: '1px solid var(--border)',
    display: 'flex',
    flexDirection: 'column',
    zIndex: 99,
    transition: 'left 0.28s cubic-bezier(0.4, 0, 0.2, 1)',
    overflowY: 'auto',
  },
  brand: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.75rem',
    padding: '1.375rem 1.25rem 1.25rem',
    borderBottom: '1px solid var(--border)',
  },
  brandIcon: {
    fontSize: '1.5rem',
    lineHeight: 1,
    flexShrink: 0,
  },
  brandName: {
    fontSize: '0.9375rem',
    fontWeight: 700,
    color: 'var(--text-primary)',
    lineHeight: 1.2,
  },
  brandSub: {
    fontSize: '0.68rem',
    color: 'var(--text-secondary)',
    marginTop: '2px',
  },
  nav: {
    flex: 1,
    padding: '1rem 0.75rem',
    display: 'flex',
    flexDirection: 'column',
    gap: '2px',
  },
  sectionLabel: {
    fontSize: '0.62rem',
    fontWeight: 700,
    letterSpacing: '0.1em',
    color: 'var(--text-secondary)',
    padding: '0 0.625rem',
    marginBottom: '0.5rem',
    marginTop: '0.25rem',
  },
  navItem: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.625rem',
    padding: '0.625rem 0.75rem',
    borderRadius: '10px',
    border: 'none',
    background: 'transparent',
    cursor: 'pointer',
    width: '100%',
    textAlign: 'left',
    fontFamily: 'inherit',
    color: 'var(--text-primary)',
    transition: 'all 0.15s ease',
    position: 'relative',
  },
  navItemActive: {
    background: 'var(--accent-light)',
  },
  navItemDisabled: {
    opacity: 0.38,
    cursor: 'not-allowed',
  },
  navIcon: {
    fontSize: '1.05rem',
    width: '1.375rem',
    textAlign: 'center',
    flexShrink: 0,
    lineHeight: 1,
  },
  navText: {
    display: 'flex',
    flexDirection: 'column',
    flex: 1,
    minWidth: 0,
  },
  navLabel: {
    fontSize: '0.85rem',
    fontWeight: 600,
    color: 'var(--text-primary)',
    lineHeight: 1.2,
  },
  navDesc: {
    fontSize: '0.68rem',
    color: 'var(--text-secondary)',
    marginTop: '1px',
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
  },
  lockIcon: {
    fontSize: '0.6rem',
    flexShrink: 0,
    opacity: 0.5,
  },
  activeBar: {
    position: 'absolute',
    right: 0,
    top: '20%',
    bottom: '20%',
    width: 3,
    borderRadius: '3px 0 0 3px',
    background: 'var(--accent)',
  },
  resultBadge: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
    margin: '0 0.75rem 0.75rem',
    padding: '0.5rem 0.75rem',
    borderRadius: '8px',
    background: '#f0fdf4',
    border: '1px solid #bbf7d0',
  },
  resultDot: {
    width: 7,
    height: 7,
    borderRadius: '50%',
    background: '#22c55e',
    flexShrink: 0,
    boxShadow: '0 0 0 2px #dcfce7',
  },
  resultText: {
    fontSize: '0.72rem',
    fontWeight: 600,
    color: '#15803d',
  },
  footer: {
    padding: '0.875rem 1.25rem',
    borderTop: '1px solid var(--border)',
  },
  footerText: {
    fontSize: '0.68rem',
    color: 'var(--text-secondary)',
  },
};
