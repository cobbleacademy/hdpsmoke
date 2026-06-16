import { useState, useEffect, useRef } from 'react';

const BASE = import.meta.env.BASE_URL;

// ── Service type icon ─────────────────────────────────────────────────────────
function serviceIcon(type) {
  const map = { hive: '🐝', hdfs: '📂', hbase: '🗄️', tag: '🏷️' };
  return map[(type || '').toLowerCase()] || '📋';
}

function serviceTypeColor(type) {
  const map = {
    hive:  { background: '#ede9fe', color: '#6d28d9' },
    hdfs:  { background: '#e0f2fe', color: '#0369a1' },
    hbase: { background: '#dcfce7', color: '#15803d' },
    tag:   { background: '#fef9c3', color: '#854d0e' },
  };
  return map[(type || '').toLowerCase()] || { background: 'var(--accent-light)', color: 'var(--accent)' };
}

/**
 * RangerPolicyTree — left panel of the Ranger Library.
 *
 * Props:
 *   envId            string
 *   policies         [{ policyKey, name, serviceType, service, hasPolicy, lastGenerated }]
 *   selectedKey      string | null
 *   onSelect         (policyKey) => void
 *   onAdd            () => void          — open "add policy" modal/flow in parent
 *   onDelete         (policyKey) => void
 *   onRefresh        () => void
 */
export default function RangerPolicyTree({
  envId,
  policies = [],
  selectedKey,
  onSelect,
  onAdd,
  onDelete,
  onRefresh,
}) {
  const [searchQuery, setSearchQuery]       = useState('');
  const [collapsedGroups, setCollapsedGroups] = useState(new Set());
  const [confirmDelete, setConfirmDelete]   = useState(null); // policyKey to confirm
  const [deleting, setDeleting]             = useState(null);
  const searchRef = useRef(null);

  // ⌘K / Ctrl+K focuses search
  useEffect(() => {
    function onKey(e) {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        searchRef.current?.focus();
      }
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  // ── Filter ────────────────────────────────────────────────────────────────
  const q = searchQuery.toLowerCase();
  const filtered = q
    ? policies.filter(
        (p) =>
          p.name?.toLowerCase().includes(q) ||
          p.policyKey?.toLowerCase().includes(q) ||
          p.serviceType?.toLowerCase().includes(q) ||
          p.service?.toLowerCase().includes(q)
      )
    : policies;

  // ── Group by serviceType ──────────────────────────────────────────────────
  const groups = {};
  for (const p of filtered) {
    const key = p.serviceType || 'unknown';
    if (!groups[key]) groups[key] = [];
    groups[key].push(p);
  }
  const groupKeys = Object.keys(groups).sort();

  // Auto-expand selected item's group
  useEffect(() => {
    if (!selectedKey) return;
    const found = policies.find((p) => p.policyKey === selectedKey);
    if (found) {
      const grp = found.serviceType || 'unknown';
      setCollapsedGroups((prev) => {
        if (!prev.has(grp)) return prev;
        const next = new Set(prev);
        next.delete(grp);
        return next;
      });
    }
  }, [selectedKey, policies]);

  function toggleGroup(grp) {
    setCollapsedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(grp)) next.delete(grp); else next.add(grp);
      return next;
    });
  }

  function expandAll()   { setCollapsedGroups(new Set()); }
  function collapseAll() { setCollapsedGroups(new Set(groupKeys)); }

  // ── Delete flow ───────────────────────────────────────────────────────────
  async function doDelete(policyKey) {
    setDeleting(policyKey);
    try {
      const resp = await fetch(
        `${BASE}ranger-policy/${encodeURIComponent(envId)}/${encodeURIComponent(policyKey)}`,
        { method: 'DELETE' }
      );
      if (resp.ok) {
        onDelete?.(policyKey);
      } else {
        const data = await resp.json().catch(() => ({}));
        console.error('[RangerPolicyTree] Delete failed:', data.error);
      }
    } catch (err) {
      console.error('[RangerPolicyTree] Delete error:', err.message);
    } finally {
      setDeleting(null);
      setConfirmDelete(null);
    }
  }

  return (
    <div style={s.tree}>
      {/* ── Toolbar ── */}
      <div style={s.toolbar}>
        <div style={s.searchWrap}>
          <span style={s.searchIcon}>⌕</span>
          <input
            ref={searchRef}
            style={s.searchInput}
            placeholder="Search policies… (⌘K)"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
          {searchQuery && (
            <button style={s.clearSearch} onClick={() => setSearchQuery('')}>✕</button>
          )}
        </div>

        <div style={s.toolbarRow}>
          <button style={s.ghostBtn} onClick={expandAll}   title="Expand All">⊞</button>
          <button style={s.ghostBtn} onClick={collapseAll} title="Collapse All">⊟</button>
          <button style={s.ghostBtn} onClick={onRefresh}   title="Refresh">↺</button>
          <button style={s.addBtn}   onClick={onAdd}       title="Add Policy">+ Add Policy</button>
        </div>
      </div>

      {/* ── Empty state ── */}
      {policies.length === 0 && (
        <div style={s.empty}>
          <span style={s.emptyIcon}>🏹</span>
          <p style={s.emptyTitle}>No policies yet</p>
          <p style={s.emptyDesc}>Click "+ Add Policy" to add a new Ranger policy for this environment.</p>
        </div>
      )}

      {filtered.length === 0 && policies.length > 0 && (
        <div style={s.empty}>
          <p style={s.emptyDesc}>No policies match "{searchQuery}"</p>
        </div>
      )}

      {/* ── Groups ── */}
      <div style={s.groupList}>
        {groupKeys.map((grp) => {
          const items = groups[grp];
          const collapsed = collapsedGroups.has(grp);
          const hasSelected = items.some((p) => p.policyKey === selectedKey);

          return (
            <div key={grp} style={s.group}>
              {/* Group header */}
              <div style={s.groupHeader} onClick={() => toggleGroup(grp)}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <span style={{ ...s.chevron, transform: collapsed ? 'rotate(0deg)' : 'rotate(90deg)' }}>▶</span>
                  <span style={{ fontSize: '1rem' }}>{serviceIcon(grp)}</span>
                  <span style={s.groupLabel}>{grp.toUpperCase()}</span>
                  <span style={s.groupCount}>{items.length}</span>
                  {collapsed && hasSelected && (
                    <span style={s.accentDot} title="Contains selected policy" />
                  )}
                </div>
              </div>

              {/* Policy leaves */}
              {!collapsed && (
                <div style={s.leafList}>
                  {items.map((p) => {
                    const isActive = p.policyKey === selectedKey;
                    const isConfirming = confirmDelete === p.policyKey;
                    const isDeleting   = deleting === p.policyKey;

                    return (
                      <div
                        key={p.policyKey}
                        style={{ ...s.leaf, ...(isActive ? s.leafActive : {}) }}
                        onClick={() => !isConfirming && onSelect?.(p.policyKey)}
                      >
                        <div style={s.leafMain}>
                          <div style={s.leafName}>
                            <span style={{ ...s.serviceChip, ...serviceTypeColor(p.serviceType) }}>
                              {(p.serviceType || 'hive').toLowerCase()}
                            </span>
                            <span style={s.leafNameText}>{p.name || p.policyKey}</span>
                          </div>
                          {p.service && (
                            <div style={s.leafMeta}>{p.service}</div>
                          )}
                          {!p.hasPolicy && (
                            <div style={s.pendingBadge}>pending</div>
                          )}
                        </div>

                        {/* Delete button / confirmation */}
                        <div style={s.leafActions} onClick={(e) => e.stopPropagation()}>
                          {isConfirming ? (
                            <div style={{ display: 'flex', gap: 4 }}>
                              <button
                                style={s.confirmDeleteBtn}
                                onClick={() => doDelete(p.policyKey)}
                                disabled={isDeleting}
                              >
                                {isDeleting ? '…' : '✓'}
                              </button>
                              <button
                                style={s.cancelDeleteBtn}
                                onClick={() => setConfirmDelete(null)}
                              >
                                ✕
                              </button>
                            </div>
                          ) : (
                            <button
                              style={s.deleteBtn}
                              onClick={() => setConfirmDelete(p.policyKey)}
                              title="Delete policy"
                            >
                              🗑
                            </button>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────
const s = {
  tree: {
    width: 280, minWidth: 220, maxWidth: 320,
    display: 'flex', flexDirection: 'column',
    borderRight: '1px solid var(--border)',
    overflowY: 'auto', background: 'var(--surface)',
    flexShrink: 0,
  },
  toolbar: {
    padding: '0.75rem 0.75rem 0.5rem',
    borderBottom: '1px solid var(--border)',
    display: 'flex', flexDirection: 'column', gap: '0.5rem',
  },
  searchWrap: {
    position: 'relative', display: 'flex', alignItems: 'center',
  },
  searchIcon: {
    position: 'absolute', left: 8,
    color: 'var(--text-secondary)', fontSize: '1rem', pointerEvents: 'none',
  },
  searchInput: {
    width: '100%', padding: '0.4rem 1.8rem 0.4rem 1.8rem',
    borderRadius: 7, border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)',
    fontSize: '0.8rem', fontFamily: 'inherit', outline: 'none',
    boxSizing: 'border-box',
  },
  clearSearch: {
    position: 'absolute', right: 6,
    background: 'none', border: 'none', cursor: 'pointer',
    color: 'var(--text-secondary)', fontSize: '0.75rem', padding: 2,
  },
  toolbarRow: {
    display: 'flex', gap: 4, alignItems: 'center',
  },
  ghostBtn: {
    padding: '0.3rem 0.5rem', borderRadius: 6,
    border: '1px solid var(--border)', background: 'transparent',
    color: 'var(--text-secondary)', fontSize: '0.85rem',
    cursor: 'pointer', fontFamily: 'inherit',
  },
  addBtn: {
    marginLeft: 'auto',
    padding: '0.3rem 0.7rem', borderRadius: 6, border: 'none',
    background: 'var(--accent)', color: '#fff',
    fontSize: '0.75rem', fontWeight: 700,
    cursor: 'pointer', fontFamily: 'inherit',
  },

  empty: {
    flex: 1, display: 'flex', flexDirection: 'column',
    alignItems: 'center', justifyContent: 'center',
    gap: '0.5rem', padding: '2rem 1rem', textAlign: 'center',
  },
  emptyIcon:  { fontSize: '2rem' },
  emptyTitle: { fontSize: '0.9rem', fontWeight: 700, color: 'var(--text-primary)', margin: 0 },
  emptyDesc:  { fontSize: '0.8rem', color: 'var(--text-secondary)', margin: 0, maxWidth: 220 },

  groupList: { flex: 1, overflowY: 'auto', padding: '0.25rem 0' },

  group: { },
  groupHeader: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    padding: '0.45rem 0.75rem',
    cursor: 'pointer', userSelect: 'none',
    borderBottom: '1px solid var(--border)',
  },
  groupLabel: {
    fontSize: '0.7rem', fontWeight: 800, letterSpacing: '0.07em',
    color: 'var(--text-secondary)', textTransform: 'uppercase',
  },
  groupCount: {
    fontSize: '0.65rem', background: 'var(--border)', color: 'var(--text-secondary)',
    borderRadius: 10, padding: '1px 6px', fontWeight: 700,
  },
  chevron: {
    fontSize: '0.5rem', color: 'var(--text-secondary)',
    transition: 'transform 0.15s ease', display: 'inline-block', flexShrink: 0,
  },
  accentDot: {
    width: 6, height: 6, borderRadius: '50%',
    background: 'var(--accent)', flexShrink: 0,
  },

  leafList: { },
  leaf: {
    display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between',
    padding: '0.5rem 0.75rem',
    cursor: 'pointer', borderBottom: '1px solid var(--border)',
    transition: 'background 0.1s',
  },
  leafActive: { background: 'var(--accent-light, rgba(108,99,255,0.08))' },
  leafMain: { flex: 1, minWidth: 0 },
  leafName: { display: 'flex', alignItems: 'center', gap: 5, flexWrap: 'wrap' },
  leafNameText: {
    fontSize: '0.82rem', fontWeight: 600, color: 'var(--text-primary)',
    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  leafMeta: {
    fontSize: '0.7rem', color: 'var(--text-secondary)',
    marginTop: 2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  pendingBadge: {
    fontSize: '0.62rem', background: '#fff7ed', color: '#c2410c',
    border: '1px solid #fed7aa', borderRadius: 4, padding: '1px 5px',
    fontWeight: 700, marginTop: 3, display: 'inline-block',
  },
  serviceChip: {
    fontSize: '0.6rem', fontWeight: 700, letterSpacing: '0.04em',
    padding: '1px 5px', borderRadius: 4, flexShrink: 0,
  },

  leafActions: { flexShrink: 0, marginLeft: 6, paddingTop: 2 },
  deleteBtn: {
    background: 'none', border: 'none', cursor: 'pointer',
    color: 'var(--text-secondary)', fontSize: '0.8rem', padding: '2px 4px',
    borderRadius: 4, opacity: 0.6,
  },
  confirmDeleteBtn: {
    padding: '2px 8px', borderRadius: 4, border: 'none',
    background: '#dc2626', color: '#fff', fontSize: '0.72rem',
    fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
  },
  cancelDeleteBtn: {
    padding: '2px 8px', borderRadius: 4,
    border: '1px solid var(--border)', background: 'transparent',
    color: 'var(--text-secondary)', fontSize: '0.72rem',
    cursor: 'pointer', fontFamily: 'inherit',
  },
};
