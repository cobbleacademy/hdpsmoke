import { useState, useMemo } from 'react';

// ── Tree builder ──────────────────────────────────────────────────────────────

/**
 * Convert a flat array of manifest nodes into a nested Map structure:
 *   catalog → { _policies: [], schemas: Map<schema, { _policies: [], tables: Map }> }
 */
function buildTree(nodes, searchQuery = '') {
  const q = searchQuery.toLowerCase().trim();
  const filtered = q
    ? nodes.filter(
        (n) =>
          (n.policyName || '').toLowerCase().includes(q) ||
          (n.catalog    || '').toLowerCase().includes(q) ||
          (n.schema     || '').toLowerCase().includes(q) ||
          (n.table      || '').toLowerCase().includes(q)
      )
    : nodes;

  // catalog name → { _policies, schemas }
  const tree = new Map();

  for (const node of filtered) {
    const cat = node.catalog || '(uncategorised)';
    if (!tree.has(cat)) tree.set(cat, { _policies: [], schemas: new Map() });
    const catEntry = tree.get(cat);

    if (!node.schema) {
      catEntry._policies.push(node);
    } else {
      if (!catEntry.schemas.has(node.schema))
        catEntry.schemas.set(node.schema, { _policies: [], tables: new Map() });
      const schEntry = catEntry.schemas.get(node.schema);

      if (!node.table) {
        schEntry._policies.push(node);
      } else {
        if (!schEntry.tables.has(node.table))
          schEntry.tables.set(node.table, { _policies: [] });
        schEntry.tables.get(node.table)._policies.push(node);
      }
    }
  }

  return tree;
}

// ── Stale/pending dot inheritance ─────────────────────────────────────────────

function policiesHaveDot(policies) {
  return policies.some((p) => p.status === 'stale' || p.status === 'pending');
}

function tableHasDot(tblEntry) {
  return policiesHaveDot(tblEntry._policies);
}

function schemaHasDot(schEntry) {
  if (policiesHaveDot(schEntry._policies)) return true;
  for (const t of schEntry.tables.values()) if (tableHasDot(t)) return true;
  return false;
}

function catalogHasDot(catEntry) {
  if (policiesHaveDot(catEntry._policies)) return true;
  for (const s of catEntry.schemas.values()) if (schemaHasDot(s)) return true;
  return false;
}

// ── Status helpers ────────────────────────────────────────────────────────────

function statusIcon(status) {
  switch (status) {
    case 'current': return { icon: '✓', color: 'var(--success)' };
    case 'stale':   return { icon: '⚠', color: 'var(--warning)' };
    case 'error':   return { icon: '✗', color: 'var(--error)'   };
    default:        return { icon: '·', color: 'var(--text-secondary)' };
  }
}

function scopeColor(scope) {
  switch (scope) {
    case 'catalog': return { bg: '#ede9fe', text: '#6d28d9' };
    case 'schema':  return { bg: '#e0f2fe', text: '#0369a1' };
    case 'table':   return { bg: '#dcfce7', text: '#15803d' };
    default:        return { bg: 'var(--accent-light)', text: 'var(--accent)' };
  }
}

// ── Sub-components ────────────────────────────────────────────────────────────

function PolicyLeaf({ node, isSelected, onSelect }) {
  const { icon, color } = statusIcon(node.status);
  const sc = scopeColor(node.scope);

  return (
    <button
      style={{
        ...s.leaf,
        ...(isSelected ? s.leafSelected : {}),
      }}
      onClick={() => onSelect(node)}
      title={`${node.catalog || ''}${node.schema ? '.' + node.schema : ''}${node.table ? '.' + node.table : ''} — ${node.scope}-level`}
    >
      <span style={{ ...s.statusIcon, color }}>{icon}</span>
      <span style={s.leafName}>{node.policyName}</span>
      <span style={{ ...s.scopeBadge, background: sc.bg, color: sc.text }}>
        {node.scope}
      </span>
      {node.ruleCount != null && (
        <span style={s.ruleCount}>{node.ruleCount}r</span>
      )}
      {node.status === 'stale' && (
        <span style={s.stalePill}>stale</span>
      )}
    </button>
  );
}

function TableNode({ name, data, collapsed, onToggle, selectedKey, onSelect }) {
  const hasDot = tableHasDot(data);
  const count  = data._policies.length;

  return (
    <div style={s.treeSection}>
      <button style={s.nodeHeader} onClick={onToggle}>
        <span style={{ ...s.chevron, transform: collapsed ? 'rotate(0deg)' : 'rotate(90deg)' }}>▶</span>
        <span style={s.nodeTypeTag}>TBL</span>
        <span style={s.nodeName}>{name}</span>
        <span style={s.nodeCount}>{count}</span>
        {hasDot && !collapsed && <span style={s.accentDot} />}
      </button>
      {!collapsed && (
        <div style={s.nodeChildren}>
          {data._policies.map((n) => (
            <PolicyLeaf
              key={n.policyKey}
              node={n}
              isSelected={n.policyKey === selectedKey}
              onSelect={onSelect}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function SchemaNode({ name, data, isCollapsed, onToggle, selectedKey, onSelect, collapsedSet, onNodeToggle }) {
  const hasDot = schemaHasDot(data);
  const count  = data._policies.length + [...data.tables.values()].reduce((s, t) => s + t._policies.length, 0);

  return (
    <div style={s.treeSection}>
      <button style={s.nodeHeader} onClick={onToggle}>
        <span style={{ ...s.chevron, transform: isCollapsed ? 'rotate(0deg)' : 'rotate(90deg)' }}>▶</span>
        <span style={{ ...s.nodeTypeTag, background: '#e0f2fe', color: '#0369a1' }}>SCH</span>
        <span style={s.nodeName}>{name}</span>
        <span style={s.nodeCount}>{count}</span>
        {hasDot && isCollapsed && <span style={s.accentDot} />}
      </button>
      {!isCollapsed && (
        <div style={s.nodeChildren}>
          {/* Schema-level policies */}
          {data._policies.map((n) => (
            <PolicyLeaf
              key={n.policyKey}
              node={n}
              isSelected={n.policyKey === selectedKey}
              onSelect={onSelect}
            />
          ))}
          {/* Table nodes */}
          {[...data.tables.entries()].map(([tblName, tblData]) => {
            const tblKey = `tbl:${name}.${tblName}`;
            return (
              <TableNode
                key={tblName}
                name={tblName}
                data={tblData}
                collapsed={collapsedSet.has(tblKey)}
                onToggle={() => onNodeToggle(tblKey)}
                selectedKey={selectedKey}
                onSelect={onSelect}
              />
            );
          })}
        </div>
      )}
    </div>
  );
}

// ── Main PolicyTree ───────────────────────────────────────────────────────────

export default function PolicyTree({
  nodes = [],
  selectedKey,
  onSelect,
  onAddNodes,
  onDeleteNode,
  searchQuery,
  onSearchChange,
}) {
  const [collapsed, setCollapsed]     = useState(new Set());
  const [addOpen, setAddOpen]         = useState(false);
  const [addMode, setAddMode]         = useState('github'); // 'github' | 'manual'
  const [addFilePath, setAddFilePath] = useState('');
  const [addBranch, setAddBranch]     = useState('');
  const [addCatalog, setAddCatalog]   = useState('');
  const [addSchema, setAddSchema]     = useState('');
  const [addTable, setAddTable]       = useState('');
  const [addPolicyName, setAddPolicyName] = useState('');
  const [addParsing, setAddParsing]   = useState(false);
  const [addParsed, setAddParsed]     = useState(null); // parsed suggestions
  const [addError, setAddError]       = useState('');

  const BASE = import.meta.env.BASE_URL;

  const tree = useMemo(() => buildTree(nodes, searchQuery), [nodes, searchQuery]);

  function toggleNode(key) {
    setCollapsed((prev) => {
      const next = new Set(prev);
      next.has(key) ? next.delete(key) : next.add(key);
      return next;
    });
  }

  function expandAll() {
    setCollapsed(new Set());
  }

  function collapseAll() {
    const keys = new Set();
    for (const [catName, catData] of tree) {
      keys.add(`cat:${catName}`);
      for (const schName of catData.schemas.keys()) {
        keys.add(`sch:${catName}.${schName}`);
        const schEntry = catData.schemas.get(schName);
        for (const tblName of schEntry.tables.keys()) {
          keys.add(`tbl:${schName}.${tblName}`);
        }
      }
    }
    setCollapsed(keys);
  }

  // ── Add policy handlers ─────────────────────────────────────────────────────

  async function handleParse() {
    setAddError('');
    setAddParsed(null);
    setAddParsing(true);
    try {
      const body = addMode === 'github'
        ? { sourceMode: 'github', filePath: addFilePath, branch: addBranch || undefined }
        : null;

      if (addMode === 'manual') {
        // Single manual entry
        if (!addPolicyName.trim() || !addCatalog.trim())
          return setAddError('Catalog and policy name are required');
        const entry = {
          catalog: addCatalog.trim() || null,
          schema:  addSchema.trim()  || null,
          table:   addTable.trim()   || null,
          policyName: addPolicyName.trim(),
          scope: addTable.trim() ? 'table' : addSchema.trim() ? 'schema' : 'catalog',
          status: 'pending',
        };
        setAddParsed([entry]);
        return;
      }

      if (!addFilePath.trim()) return setAddError('File path is required');

      const resp = await fetch(`${BASE}opa-parse`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await resp.json();
      if (!resp.ok) { setAddError(data.error || 'Parse failed'); return; }
      if (!data.policies?.length) { setAddError('No CREATE POLICY statements found in that file'); return; }
      setAddParsed(data.policies);
    } catch {
      setAddError('Network error');
    } finally {
      setAddParsing(false);
    }
  }

  function handleConfirmAdd() {
    if (!addParsed?.length) return;
    onAddNodes?.(addParsed);
    setAddParsed(null);
    setAddOpen(false);
    setAddFilePath(''); setAddBranch('');
    setAddCatalog(''); setAddSchema(''); setAddTable(''); setAddPolicyName('');
  }

  const totalCount = nodes.length;

  return (
    <div style={s.panel}>
      {/* Search bar */}
      <div style={s.searchRow}>
        <div style={s.searchWrap}>
          <span style={s.searchIcon}>🔍</span>
          <input
            style={s.searchInput}
            placeholder="Search policies…"
            value={searchQuery}
            onChange={(e) => onSearchChange(e.target.value)}
          />
          {searchQuery && (
            <button style={s.searchClear} onClick={() => onSearchChange('')}>✕</button>
          )}
        </div>
      </div>

      {/* Toolbar */}
      <div style={s.toolbar}>
        <span style={s.policyCount}>{totalCount} {totalCount === 1 ? 'policy' : 'policies'}</span>
        <div style={{ display: 'flex', gap: 4 }}>
          <button style={s.toolBtn} onClick={expandAll}>Expand All</button>
          <button style={s.toolBtn} onClick={collapseAll}>Collapse All</button>
        </div>
      </div>

      {/* Tree */}
      <div style={s.treeBody}>
        {tree.size === 0 && (
          <p style={s.emptyMsg}>
            {searchQuery ? 'No policies match your search.' : 'No policies yet — click + Add below.'}
          </p>
        )}

        {[...tree.entries()].map(([catName, catData]) => {
          const catKey    = `cat:${catName}`;
          const catCollapsed = collapsed.has(catKey);
          const hasDot    = catalogHasDot(catData);
          const catCount  =
            catData._policies.length +
            [...catData.schemas.values()].reduce(
              (acc, sch) =>
                acc + sch._policies.length +
                [...sch.tables.values()].reduce((a, t) => a + t._policies.length, 0),
              0
            );

          return (
            <div key={catName} style={s.treeSection}>
              {/* Catalog header */}
              <button style={s.catalogHeader} onClick={() => toggleNode(catKey)}>
                <span style={{ ...s.chevron, transform: catCollapsed ? 'rotate(0deg)' : 'rotate(90deg)' }}>▶</span>
                <span style={s.catIcon}>🗄</span>
                <span style={s.catalogName}>{catName}</span>
                <span style={s.nodeCount}>{catCount}</span>
                {hasDot && catCollapsed && <span style={s.accentDot} />}
              </button>

              {!catCollapsed && (
                <div style={s.nodeChildren}>
                  {/* Catalog-level policy leaves */}
                  {catData._policies.map((n) => (
                    <PolicyLeaf
                      key={n.policyKey}
                      node={n}
                      isSelected={n.policyKey === selectedKey}
                      onSelect={onSelect}
                    />
                  ))}

                  {/* Schema nodes */}
                  {[...catData.schemas.entries()].map(([schName, schData]) => {
                    const schKey = `sch:${catName}.${schName}`;
                    return (
                      <SchemaNode
                        key={schName}
                        name={schName}
                        data={schData}
                        isCollapsed={collapsed.has(schKey)}
                        onToggle={() => toggleNode(schKey)}
                        selectedKey={selectedKey}
                        onSelect={onSelect}
                        collapsedSet={collapsed}
                        onNodeToggle={toggleNode}
                      />
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Add Policy */}
      <div style={s.addSection}>
        <button style={s.addBtn} onClick={() => { setAddOpen((o) => !o); setAddParsed(null); setAddError(''); }}>
          {addOpen ? '✕ Cancel' : '+ Add Policy'}
        </button>

        {addOpen && (
          <div style={s.addForm}>
            {/* Mode toggle */}
            <div style={s.addModeRow}>
              {['github', 'manual'].map((m) => (
                <button
                  key={m}
                  style={{ ...s.modeBtn, ...(addMode === m ? s.modeBtnActive : {}) }}
                  onClick={() => { setAddMode(m); setAddParsed(null); setAddError(''); }}
                >
                  {m === 'github' ? '🐙 From GitHub' : '✏️ Manual'}
                </button>
              ))}
            </div>

            {addMode === 'github' && (
              <>
                <input style={s.addInput} placeholder="File path (e.g. policies/dev/demos.sql)" value={addFilePath} onChange={(e) => setAddFilePath(e.target.value)} />
                <input style={s.addInput} placeholder="Branch (leave blank for env default)" value={addBranch} onChange={(e) => setAddBranch(e.target.value)} />
              </>
            )}

            {addMode === 'manual' && (
              <>
                <input style={s.addInput} placeholder="Catalog *" value={addCatalog} onChange={(e) => setAddCatalog(e.target.value)} />
                <input style={s.addInput} placeholder="Schema (optional)" value={addSchema} onChange={(e) => setAddSchema(e.target.value)} />
                <input style={s.addInput} placeholder="Table (optional)" value={addTable} onChange={(e) => setAddTable(e.target.value)} />
                <input style={s.addInput} placeholder="Policy name *" value={addPolicyName} onChange={(e) => setAddPolicyName(e.target.value)} />
              </>
            )}

            {addError && <p style={s.addError}>{addError}</p>}

            {/* Parsed suggestions */}
            {addParsed && (
              <div style={s.parsedList}>
                <p style={s.parsedHeader}>{addParsed.length} {addParsed.length === 1 ? 'policy' : 'policies'} found:</p>
                {addParsed.map((p, i) => {
                  const sc = scopeColor(p.scope);
                  return (
                    <div key={i} style={s.parsedItem}>
                      <span style={{ ...s.scopeBadge, background: sc.bg, color: sc.text }}>{p.scope}</span>
                      <span style={s.leafName}>
                        {[p.catalog, p.schema, p.table].filter(Boolean).join('.')} → {p.policyName}
                      </span>
                    </div>
                  );
                })}
                <button style={s.confirmBtn} onClick={handleConfirmAdd}>
                  Add {addParsed.length === 1 ? 'this policy' : `all ${addParsed.length} policies`}
                </button>
              </div>
            )}

            {!addParsed && (
              <button style={{ ...s.confirmBtn, opacity: addParsing ? 0.6 : 1 }} onClick={handleParse} disabled={addParsing}>
                {addParsing ? 'Parsing…' : addMode === 'github' ? '🔍 Parse & Preview' : '+ Add Entry'}
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────

const s = {
  panel: {
    width: 300,
    minWidth: 300,
    borderRight: '1px solid var(--border)',
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
    background: 'var(--surface)',
    overflow: 'hidden',
  },
  searchRow: { padding: '0.75rem 0.75rem 0.25rem' },
  searchWrap: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    background: 'var(--bg)',
    border: '1.5px solid var(--border)',
    borderRadius: 8,
    padding: '0.3rem 0.6rem',
  },
  searchIcon:  { fontSize: '0.75rem', color: 'var(--text-secondary)', flexShrink: 0 },
  searchInput: {
    flex: 1,
    border: 'none',
    background: 'transparent',
    fontSize: '0.8rem',
    color: 'var(--text-primary)',
    fontFamily: 'inherit',
    outline: 'none',
    minWidth: 0,
  },
  searchClear: {
    background: 'none', border: 'none', cursor: 'pointer',
    color: 'var(--text-secondary)', fontSize: '0.7rem', padding: 0, flexShrink: 0,
    fontFamily: 'inherit',
  },

  toolbar: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '0.25rem 0.75rem 0.5rem',
    borderBottom: '1px solid var(--border)',
  },
  policyCount: { fontSize: '0.72rem', color: 'var(--text-secondary)', fontWeight: 600 },
  toolBtn: {
    padding: '0.2rem 0.5rem',
    borderRadius: 6,
    border: '1px solid var(--border)',
    background: 'transparent',
    color: 'var(--text-secondary)',
    fontSize: '0.68rem',
    cursor: 'pointer',
    fontFamily: 'inherit',
  },

  treeBody: {
    flex: 1,
    overflowY: 'auto',
    padding: '0.4rem 0',
  },
  emptyMsg: {
    fontSize: '0.78rem',
    color: 'var(--text-secondary)',
    textAlign: 'center',
    padding: '2rem 1rem',
  },

  treeSection: { marginBottom: 1 },

  // Catalog header
  catalogHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 5,
    width: '100%',
    padding: '0.45rem 0.75rem',
    border: 'none',
    background: 'transparent',
    cursor: 'pointer',
    textAlign: 'left',
    fontFamily: 'inherit',
    color: 'var(--text-primary)',
    position: 'relative',
  },
  catIcon: { fontSize: '0.9rem', lineHeight: 1, flexShrink: 0 },
  catalogName: { fontSize: '0.85rem', fontWeight: 700, flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' },

  // Generic node header (schema / table)
  nodeHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 4,
    width: '100%',
    padding: '0.35rem 0.75rem 0.35rem 1.5rem',
    border: 'none',
    background: 'transparent',
    cursor: 'pointer',
    textAlign: 'left',
    fontFamily: 'inherit',
    color: 'var(--text-primary)',
  },
  nodeTypeTag: {
    fontSize: '0.58rem',
    fontWeight: 700,
    letterSpacing: '0.04em',
    padding: '1px 4px',
    borderRadius: 3,
    background: '#f0fdf4',
    color: '#15803d',
    flexShrink: 0,
  },
  nodeName: { fontSize: '0.82rem', fontWeight: 600, flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' },
  nodeCount: { fontSize: '0.68rem', color: 'var(--text-secondary)', flexShrink: 0 },

  nodeChildren: { paddingLeft: 12 },

  chevron: {
    fontSize: '0.5rem',
    color: 'var(--text-secondary)',
    transition: 'transform 0.18s ease',
    display: 'inline-block',
    flexShrink: 0,
    marginRight: 2,
  },
  accentDot: {
    width: 6, height: 6,
    borderRadius: '50%',
    background: 'var(--accent)',
    flexShrink: 0,
    marginLeft: 2,
  },

  // Policy leaf
  leaf: {
    display: 'flex',
    alignItems: 'center',
    gap: 5,
    width: '100%',
    padding: '0.3rem 0.75rem 0.3rem 1.75rem',
    border: 'none',
    background: 'transparent',
    cursor: 'pointer',
    textAlign: 'left',
    fontFamily: 'inherit',
    color: 'var(--text-primary)',
    borderRadius: 6,
    transition: 'background 0.12s',
  },
  leafSelected: {
    background: 'var(--accent-light)',
  },
  statusIcon: { fontSize: '0.75rem', fontWeight: 700, flexShrink: 0, width: 12, textAlign: 'center' },
  leafName:   { fontSize: '0.8rem', fontWeight: 600, flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' },
  scopeBadge: {
    fontSize: '0.6rem',
    fontWeight: 700,
    letterSpacing: '0.03em',
    padding: '1px 5px',
    borderRadius: 3,
    flexShrink: 0,
  },
  ruleCount: { fontSize: '0.65rem', color: 'var(--text-secondary)', flexShrink: 0 },
  stalePill: {
    fontSize: '0.6rem',
    fontWeight: 700,
    padding: '1px 4px',
    borderRadius: 3,
    background: '#fff7ed',
    color: '#c2410c',
    flexShrink: 0,
  },

  // Add section
  addSection: {
    borderTop: '1px solid var(--border)',
    padding: '0.6rem 0.75rem',
  },
  addBtn: {
    width: '100%',
    padding: '0.45rem',
    borderRadius: 8,
    border: '1.5px dashed var(--border)',
    background: 'transparent',
    color: 'var(--accent)',
    fontSize: '0.8rem',
    fontWeight: 600,
    cursor: 'pointer',
    fontFamily: 'inherit',
    transition: 'all 0.15s',
  },
  addForm:    { marginTop: '0.6rem', display: 'flex', flexDirection: 'column', gap: '0.4rem' },
  addModeRow: { display: 'flex', gap: 4 },
  modeBtn: {
    flex: 1,
    padding: '0.3rem 0.4rem',
    borderRadius: 6,
    border: '1.5px solid var(--border)',
    background: 'transparent',
    color: 'var(--text-secondary)',
    fontSize: '0.72rem',
    fontWeight: 600,
    cursor: 'pointer',
    fontFamily: 'inherit',
  },
  modeBtnActive: {
    background: 'var(--accent-light)',
    border: '1.5px solid var(--accent)',
    color: 'var(--accent)',
  },
  addInput: {
    padding: '0.35rem 0.6rem',
    borderRadius: 6,
    border: '1.5px solid var(--border)',
    background: 'var(--bg)',
    color: 'var(--text-primary)',
    fontSize: '0.78rem',
    fontFamily: 'inherit',
    outline: 'none',
  },
  addError:    { fontSize: '0.75rem', color: 'var(--error)', margin: 0 },
  parsedList:  { background: 'var(--bg)', borderRadius: 6, padding: '0.5rem', display: 'flex', flexDirection: 'column', gap: 4 },
  parsedHeader:{ fontSize: '0.72rem', fontWeight: 700, color: 'var(--text-secondary)', margin: 0 },
  parsedItem:  { display: 'flex', alignItems: 'center', gap: 5 },
  confirmBtn: {
    marginTop: 4,
    padding: '0.4rem',
    borderRadius: 6,
    border: 'none',
    background: 'var(--accent)',
    color: '#fff',
    fontSize: '0.78rem',
    fontWeight: 700,
    cursor: 'pointer',
    fontFamily: 'inherit',
  },
};
