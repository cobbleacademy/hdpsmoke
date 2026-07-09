import { useState, useEffect, useRef } from 'react';

const BASE = import.meta.env.BASE_URL;

// mermaid is lazy-loaded (dynamic import) so its ~500KB+ isn't part of any
// bundle that doesn't visit this page — including the just-shipped Access
// Control slim app, which never imports this file at all. First Mermaid
// render also calls mermaid.initialize() once.
// 'base' + explicit themeVariables gives every Mermaid diagram the app's own
// accent palette instead of Mermaid's generic default/dark theme — a colored
// lane per node type, closer (though never identical) to a hand-authored
// diagram like the Sensec HSM Demo's architecture SVG. Two palettes so both
// light and dark app themes get a coherent look, not just one theme's colors
// forced onto the other.
const LIGHT_THEME_VARS = {
  primaryColor: '#ede9fe', primaryBorderColor: '#7c6df2', primaryTextColor: '#1e1b4b',
  lineColor: '#7c6df2', secondaryColor: '#e0e7ff', tertiaryColor: '#fef3c7',
  noteBkgColor: '#fef3c7', noteBorderColor: '#f59e0b',
  actorBkg: '#ede9fe', actorBorder: '#7c6df2', actorTextColor: '#1e1b4b',
};
const DARK_THEME_VARS = {
  primaryColor: '#22263a', primaryBorderColor: '#a78bfa', primaryTextColor: '#cdd2f0',
  lineColor: '#a78bfa', secondaryColor: '#1a1d27', tertiaryColor: '#2d1b47',
  noteBkgColor: '#2d1b47', noteBorderColor: '#e879f9', mainBkg: '#1a1d27',
  actorBkg: '#22263a', actorBorder: '#a78bfa', actorTextColor: '#cdd2f0',
};

let mermaidModulePromise = null;
function loadMermaid() {
  if (!mermaidModulePromise) {
    mermaidModulePromise = import('mermaid').then((mod) => {
      const mermaid = mod.default;
      const theme = document.documentElement.dataset.theme;
      const isDark = Boolean(theme && theme !== 'light');
      mermaid.initialize({
        startOnLoad: false,
        theme: 'base',
        themeVariables: isDark ? DARK_THEME_VARS : LIGHT_THEME_VARS,
        securityLevel: 'strict',
      });
      return mermaid;
    });
  }
  return mermaidModulePromise;
}

let renderCounter = 0;

export default function PatternTemplates() {
  const [templates, setTemplates] = useState([]);
  const [writeAuthRequired, setWriteAuthRequired] = useState(false);
  const [adminToken, setAdminToken] = useState('');
  const [listStatus, setListStatus] = useState('loading'); // loading | ready | error
  const [listError, setListError] = useState('');

  const [selectedId, setSelectedId] = useState(null);
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [mermaidText, setMermaidText] = useState('');
  const [type, setType] = useState('mermaid'); // 'mermaid' | 'svg'
  const [dirty, setDirty] = useState(false);

  const [renderedSvg, setRenderedSvg] = useState('');
  const [renderError, setRenderError] = useState('');

  const [saveState, setSaveState] = useState('idle'); // idle | saving | saved | error
  const [saveError, setSaveError] = useState('');
  const [validationErrors, setValidationErrors] = useState([]);

  const [isCreating, setIsCreating] = useState(false);
  const debounceRef = useRef(null);

  // ── Layout controls — free up room for real-world diagrams ──────────────────
  const [listCollapsed, setListCollapsed] = useState(false);
  const [editorLayout, setEditorLayout] = useState('preview'); // split | code | preview

  function loadList() {
    setListStatus('loading');
    fetch(`${BASE}pattern-templates`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((data) => {
        setTemplates(data.templates || []);
        setWriteAuthRequired(Boolean(data.writeAuthRequired));
        setListStatus('ready');
      })
      .catch((err) => {
        setListError(err.message || 'Could not load Pattern Templates — is the backend running?');
        setListStatus('error');
      });
  }

  useEffect(() => { loadList(); }, []);

  // Auto-select the first template once the list arrives, if nothing is selected yet.
  useEffect(() => {
    if (listStatus === 'ready' && templates.length > 0 && selectedId === null && !isCreating) {
      handleSelect(templates[0].id);
    }
  }, [listStatus, templates]); // eslint-disable-line react-hooks/exhaustive-deps

  function handleSelect(id) {
    setIsCreating(false);
    setSelectedId(id);
    setSaveState('idle');
    setSaveError('');
    setValidationErrors([]);
    fetch(`${BASE}pattern-templates/${encodeURIComponent(id)}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((data) => {
        setName(data.name);
        setDescription(data.description || '');
        setMermaidText(data.mermaidText);
        setType(data.type || 'mermaid');
        setDirty(false);
      })
      .catch((err) => {
        setSaveError(err.message || 'Could not load this template');
      });
  }

  function handleNewTemplate() {
    setIsCreating(true);
    setSelectedId(null);
    setName('');
    setDescription('');
    setMermaidText('flowchart TD\n  A[Start] --> B[End]\n');
    setType('mermaid');
    setDirty(true);
    setSaveState('idle');
    setSaveError('');
    setValidationErrors([]);
  }

  function handleTypeChange(nextType) {
    setType(nextType);
    setDirty(true);
    setSaveState('idle');
    // Swap in a sensible starter so the textarea isn't left holding content
    // in the wrong syntax (Mermaid text under 'svg', or vice versa).
    if (nextType === 'svg' && !mermaidText.trim().startsWith('<svg')) {
      setMermaidText('<svg viewBox="0 0 200 100" xmlns="http://www.w3.org/2000/svg">\n  <rect x="10" y="10" width="80" height="40" rx="6" fill="#22263a" stroke="#3b82f6" />\n</svg>\n');
    } else if (nextType === 'mermaid' && mermaidText.trim().startsWith('<svg')) {
      setMermaidText('flowchart TD\n  A[Start] --> B[End]\n');
    }
  }

  // ── Live diagram preview — debounced re-render on every edit ────────────────
  // 'mermaid' templates go through Mermaid's layout engine; 'svg' templates
  // are hand-authored markup rendered directly, no Mermaid involved.
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      const trimmed = mermaidText.trim();
      if (!trimmed) {
        setRenderedSvg('');
        setRenderError('');
        return;
      }

      if (type === 'svg') {
        if (!trimmed.startsWith('<svg')) {
          setRenderedSvg('');
          setRenderError('SVG content must start with <svg — check the markup.');
          return;
        }
        setRenderedSvg(trimmed);
        setRenderError('');
        return;
      }

      loadMermaid()
        .then((mermaid) => {
          const id = `pattern-template-diagram-${++renderCounter}`;
          return mermaid.render(id, mermaidText);
        })
        .then(({ svg }) => {
          setRenderedSvg(svg);
          setRenderError('');
        })
        .catch((err) => {
          setRenderedSvg('');
          setRenderError(err.message || 'Could not render this diagram — check the Mermaid syntax.');
        });
    }, 400);
    return () => clearTimeout(debounceRef.current);
  }, [mermaidText, type]);

  function handleFieldChange(setter) {
    return (val) => {
      setter(val);
      setDirty(true);
      setSaveState('idle');
    };
  }

  async function handleSave() {
    setSaveState('saving');
    setSaveError('');
    setValidationErrors([]);

    const headers = { 'Content-Type': 'application/json' };
    if (writeAuthRequired && adminToken) headers['Authorization'] = `Bearer ${adminToken}`;
    const body = JSON.stringify({ name, description, mermaidText, type });

    try {
      const url = isCreating
        ? `${BASE}pattern-templates`
        : `${BASE}pattern-templates/${encodeURIComponent(selectedId)}`;
      const method = isCreating ? 'POST' : 'PUT';
      const resp = await fetch(url, { method, headers, body });
      const data = await resp.json();
      if (!resp.ok) {
        setSaveState('error');
        setSaveError(data.error || `HTTP ${resp.status}`);
        if (data.validationErrors) setValidationErrors(data.validationErrors);
        return;
      }
      setSaveState('saved');
      setDirty(false);
      setIsCreating(false);
      setSelectedId(data.id);
      loadList(); // refresh the list panel — name/updatedAt/new entry all reflected immediately
    } catch (err) {
      setSaveState('error');
      setSaveError(err.message || 'Network error');
    }
  }

  async function handleDelete(id) {
    try {
      const headers = {};
      if (writeAuthRequired && adminToken) headers['Authorization'] = `Bearer ${adminToken}`;
      const resp = await fetch(`${BASE}pattern-templates/${encodeURIComponent(id)}`, { method: 'DELETE', headers });
      if (!resp.ok) {
        const data = await resp.json().catch(() => ({}));
        setSaveError(data.error || `HTTP ${resp.status}`);
        return;
      }
      if (selectedId === id) {
        setSelectedId(null);
        setName(''); setDescription(''); setMermaidText(''); setRenderedSvg('');
      }
      loadList();
    } catch (err) {
      setSaveError(err.message || 'Network error');
    }
  }

  if (listStatus === 'loading') {
    return (
      <div style={s.page}>
        <div style={s.container}>
          <header style={s.header}><h1 style={s.title}>Pattern Templates</h1></header>
          <div style={s.screen}>
            <div style={s.spinner} />
            <p style={s.mutedText}>Loading templates…</p>
          </div>
        </div>
      </div>
    );
  }

  if (listStatus === 'error') {
    return (
      <div style={s.page}>
        <div style={s.container}>
          <header style={s.header}><h1 style={s.title}>Pattern Templates</h1></header>
          <div style={s.screen}><p style={s.errorText}>{listError}</p></div>
        </div>
      </div>
    );
  }

  return (
    <div style={s.page}>
      <div style={s.container}>
        <header style={s.header}>
          <h1 style={s.title}>Pattern Templates</h1>
          <p style={s.subtitle}>{templates.length} Mermaid diagram template{templates.length === 1 ? '' : 's'} — select, edit, and preview.</p>
        </header>

        <div style={s.body}>
          {/* ── Left: template list (mirrors Payload Library) ── */}
          <div style={{ ...s.listPanel, ...(listCollapsed ? s.listPanelCollapsed : {}) }}>
            <div style={s.listHeaderRow}>
              {!listCollapsed && <span style={s.sectionLabel}>Templates</span>}
              <button
                style={s.collapseBtn}
                onClick={() => setListCollapsed((v) => !v)}
                title={listCollapsed ? 'Expand template list' : 'Collapse template list'}
              >
                {listCollapsed ? '»' : '«'}
              </button>
              {!listCollapsed && <button style={s.newBtn} onClick={handleNewTemplate}>+ New</button>}
            </div>
            {!listCollapsed && (
              <div style={s.listScroll}>
                {templates.map((t) => (
                  <div
                    key={t.id}
                    onClick={() => handleSelect(t.id)}
                    style={{ ...s.listItem, ...(selectedId === t.id && !isCreating ? s.listItemActive : {}) }}
                  >
                    <div style={s.listItemName}>
                      {t.name}
                      {t.type === 'svg' && <span style={s.typeBadge}>SVG</span>}
                    </div>
                    {t.description && <div style={s.listItemDesc}>{t.description}</div>}
                    <button
                      style={s.deleteBtn}
                      onClick={(e) => { e.stopPropagation(); handleDelete(t.id); }}
                      title="Delete template"
                    >
                      🗑
                    </button>
                  </div>
                ))}
                {templates.length === 0 && <p style={s.mutedText}>No templates yet — click + New to create one.</p>}
              </div>
            )}
          </div>

          {/* ── Right: editor + live preview ── */}
          <div style={s.detailPanel}>
            {(selectedId || isCreating) ? (
              <>
                <div style={s.fieldRow}>
                  <input
                    style={s.nameInput}
                    value={name}
                    placeholder="Template name"
                    onChange={(e) => handleFieldChange(setName)(e.target.value)}
                  />
                  {dirty && <span style={s.modifiedBadge}>Modified</span>}
                </div>
                <textarea
                  style={s.descInput}
                  value={description}
                  placeholder="Description (optional)"
                  onChange={(e) => handleFieldChange(setDescription)(e.target.value)}
                />

                <div style={s.typeRow}>
                  <span style={s.sectionLabel}>Content Type</span>
                  <div style={s.typeToggleGroup}>
                    {['mermaid', 'svg'].map((t) => (
                      <button
                        key={t}
                        onClick={() => handleTypeChange(t)}
                        style={{ ...s.typeToggleBtn, ...(type === t ? s.typeToggleBtnActive : {}) }}
                      >
                        {t === 'mermaid' ? 'Mermaid' : 'Raw SVG'}
                      </button>
                    ))}
                  </div>
                </div>

                <div style={s.editorTabRow}>
                  {['split', 'code', 'preview'].map((mode) => (
                    <button
                      key={mode}
                      onClick={() => setEditorLayout(mode)}
                      style={{ ...s.editorTabBtn, ...(editorLayout === mode ? s.editorTabBtnActive : {}) }}
                    >
                      {mode === 'split' ? 'Split' : mode === 'code' ? 'Code' : 'Preview'}
                    </button>
                  ))}
                </div>

                <div style={{ ...s.editorRow, gridTemplateColumns: editorLayout === 'split' ? '1fr 1fr' : '1fr' }}>
                  {editorLayout !== 'preview' && (
                    <div style={s.editorCol}>
                      <span style={s.sectionLabel}>{type === 'svg' ? 'SVG Markup' : 'Mermaid Text'}</span>
                      <textarea
                        style={s.mermaidInput}
                        value={mermaidText}
                        spellCheck={false}
                        onChange={(e) => handleFieldChange(setMermaidText)(e.target.value)}
                      />
                    </div>
                  )}
                  {editorLayout !== 'code' && (
                    <div style={s.editorCol}>
                      <span style={s.sectionLabel}>Live Preview</span>
                      <div style={s.previewBox}>
                        {renderError && <p style={s.errorTextSmall}>{renderError}</p>}
                        {!renderError && renderedSvg && (
                          <div style={s.svgWrap} dangerouslySetInnerHTML={{ __html: renderedSvg }} />
                        )}
                        {!renderError && !renderedSvg && <p style={s.mutedText}>Nothing to preview yet.</p>}
                      </div>
                    </div>
                  )}
                </div>

                {writeAuthRequired && (
                  <input
                    type="password"
                    placeholder="Admin token"
                    value={adminToken}
                    onChange={(e) => setAdminToken(e.target.value)}
                    style={s.adminTokenInput}
                  />
                )}

                <div style={s.saveRow}>
                  <button
                    onClick={handleSave}
                    disabled={saveState === 'saving' || !name.trim() || !mermaidText.trim()}
                    style={{ ...s.saveBtn, ...(saveState === 'saving' || !name.trim() || !mermaidText.trim() ? s.saveBtnDisabled : {}) }}
                  >
                    {saveState === 'saving' ? 'Saving…' : '💾 Save'}
                  </button>
                  {saveState === 'saved' && <span style={s.savedLabel}>Saved</span>}
                </div>

                {validationErrors.length > 0 && (
                  <div style={s.errorBanner}>{validationErrors.map((e, i) => <div key={i}>• {e}</div>)}</div>
                )}
                {saveState === 'error' && !validationErrors.length && (
                  <div style={s.errorBanner}>{saveError}</div>
                )}
              </>
            ) : (
              <p style={s.mutedText}>Select a template on the left, or click + New to create one.</p>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

const s = {
  page: { display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden', background: 'var(--bg)' },
  container: { display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' },
  header: { padding: '1rem 1.25rem 0.75rem', borderBottom: '1px solid var(--border)', background: 'var(--surface)', flexShrink: 0 },
  title: { fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.02em', margin: 0 },
  subtitle: { fontSize: '0.8rem', color: 'var(--text-secondary)', margin: '4px 0 0' },

  screen: { flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 12 },
  spinner: { width: 32, height: 32, borderRadius: '50%', border: '3px solid var(--border)', borderTopColor: 'var(--accent)', animation: 'spin 0.8s linear infinite' },
  mutedText: { fontSize: '0.85rem', color: 'var(--text-secondary)' },
  errorText: { fontSize: '0.85rem', color: 'var(--error)' },
  errorTextSmall: { fontSize: '0.78rem', color: 'var(--error)', margin: 0 },

  body: { flex: 1, overflow: 'hidden', display: 'flex', gap: '1rem', padding: '1.25rem' },

  listPanel: {
    width: 300, flexShrink: 0, display: 'flex', flexDirection: 'column',
    border: '1px solid var(--border)', borderRadius: 'var(--radius)', background: 'var(--surface)', overflow: 'hidden',
    transition: 'width 0.15s',
  },
  listPanelCollapsed: { width: 44 },
  listHeaderRow: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0.85rem 1rem', borderBottom: '1px solid var(--border)' },
  collapseBtn: {
    padding: '0.3rem 0.5rem', borderRadius: 7, border: '1px solid var(--border)', background: 'transparent',
    color: 'var(--text-secondary)', fontSize: '0.75rem', fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
  },
  sectionLabel: { fontSize: '0.7rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em', color: 'var(--text-secondary)' },
  newBtn: {
    padding: '0.3rem 0.65rem', borderRadius: 7, border: '1px solid var(--border)', background: 'transparent',
    color: 'var(--accent)', fontSize: '0.75rem', fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
  },
  listScroll: { flex: 1, overflowY: 'auto', padding: '0.5rem' },
  listItem: {
    position: 'relative', padding: '0.6rem 2rem 0.6rem 0.75rem', borderRadius: 8, cursor: 'pointer',
    marginBottom: 4, transition: 'background 0.15s',
  },
  listItemActive: { background: 'var(--accent-light)' },
  listItemName: { fontSize: '0.85rem', fontWeight: 600, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: 6 },
  listItemDesc: { fontSize: '0.72rem', color: 'var(--text-secondary)', marginTop: 2 },
  typeBadge: {
    fontSize: '0.6rem', fontWeight: 700, letterSpacing: '0.03em', color: 'var(--accent)',
    border: '1px solid var(--accent)', borderRadius: 4, padding: '0.05rem 0.3rem',
  },
  deleteBtn: {
    position: 'absolute', right: 6, top: '50%', transform: 'translateY(-50%)', border: 'none',
    background: 'transparent', cursor: 'pointer', fontSize: '0.8rem', opacity: 0.5, padding: 4,
  },

  detailPanel: {
    flex: 1, overflowY: 'auto', border: '1px solid var(--border)', borderRadius: 'var(--radius)',
    background: 'var(--surface)', padding: '1.25rem', display: 'flex', flexDirection: 'column', gap: 12,
  },
  fieldRow: { display: 'flex', alignItems: 'center', gap: 10 },
  nameInput: {
    flex: 1, padding: '0.5rem 0.75rem', borderRadius: 8, border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)', fontSize: '0.95rem', fontWeight: 700,
    outline: 'none', fontFamily: 'inherit',
  },
  modifiedBadge: {
    display: 'inline-block', borderRadius: 999, background: 'var(--warning)', color: '#fff',
    fontSize: '0.68rem', fontWeight: 700, padding: '0.2rem 0.6rem', flexShrink: 0,
  },
  descInput: {
    width: '100%', minHeight: 40, padding: '0.5rem 0.75rem', borderRadius: 8, border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)', fontSize: '0.82rem', outline: 'none',
    resize: 'vertical', fontFamily: 'inherit', boxSizing: 'border-box',
  },
  typeRow: { display: 'flex', alignItems: 'center', gap: 10 },
  typeToggleGroup: { display: 'flex', gap: 4 },
  typeToggleBtn: {
    padding: '0.25rem 0.6rem', borderRadius: 7, border: '1px solid var(--border)', background: 'transparent',
    color: 'var(--text-secondary)', fontSize: '0.72rem', fontWeight: 600, cursor: 'pointer', fontFamily: 'inherit',
  },
  typeToggleBtnActive: { background: 'var(--accent-light)', color: 'var(--accent)', borderColor: 'var(--accent)' },

  editorTabRow: { display: 'flex', gap: 6 },
  editorTabBtn: {
    padding: '0.3rem 0.75rem', borderRadius: 7, border: '1px solid var(--border)', background: 'transparent',
    color: 'var(--text-secondary)', fontSize: '0.75rem', fontWeight: 600, cursor: 'pointer', fontFamily: 'inherit',
  },
  editorTabBtnActive: { background: 'var(--accent-light)', color: 'var(--accent)', borderColor: 'var(--accent)' },
  editorRow: { flex: 1, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14, minHeight: 260 },
  editorCol: { display: 'flex', flexDirection: 'column', gap: 6 },
  mermaidInput: {
    flex: 1, minHeight: 220, padding: '0.75rem', borderRadius: 8, border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)', fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    fontSize: '0.8rem', outline: 'none', resize: 'vertical', boxSizing: 'border-box',
  },
  previewBox: {
    // var(--bg), not a hardcoded white — mermaid's theme is switched to
    // 'dark' for dim/slate/mocha (see loadMermaid()), whose light-colored
    // strokes/text need a dark surface behind them to stay legible.
    flex: 1, minHeight: 220, borderRadius: 8, border: '1.5px solid var(--border)', background: 'var(--bg)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '0.75rem', overflow: 'auto',
  },
  svgWrap: { width: '100%', display: 'flex', justifyContent: 'center' },

  adminTokenInput: {
    padding: '0.5rem 0.7rem', borderRadius: 8, border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)', fontSize: '0.8rem', outline: 'none', fontFamily: 'inherit',
  },
  saveRow: { display: 'flex', alignItems: 'center', gap: 10 },
  saveBtn: {
    padding: '0.55rem 1.1rem', borderRadius: 8, border: 'none', background: 'var(--accent)',
    color: '#fff', fontSize: '0.85rem', fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
  },
  saveBtnDisabled: { opacity: 0.5, cursor: 'not-allowed' },
  savedLabel: { fontSize: '0.8rem', color: 'var(--success)', fontWeight: 600 },

  errorBanner: {
    padding: '0.6rem 0.75rem', borderRadius: 8, background: 'rgba(220,38,38,0.08)',
    border: '1px solid var(--error)', color: 'var(--error)', fontSize: '0.78rem', lineHeight: 1.6,
  },
};
