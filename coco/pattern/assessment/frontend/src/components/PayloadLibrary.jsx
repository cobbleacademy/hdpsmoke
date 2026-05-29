import { useState, useEffect, useRef } from 'react';
import jsYaml from 'js-yaml';

const MAX_HISTORY = 20;

// ── Status-badge colour helper ────────────────────────────────────────────────
function statusColor(httpStatus) {
  if (!httpStatus) return { bg: '#f3f4f6', text: '#6b7280' };
  if (httpStatus < 300) return { bg: '#f0fdf4', text: '#15803d' };
  if (httpStatus < 400) return { bg: '#eff6ff', text: '#1d4ed8' };
  if (httpStatus < 500) return { bg: '#fff7ed', text: '#c2410c' };
  return { bg: '#fef2f2', text: '#b91c1c' };
}

function StatusBadge({ status }) {
  const { bg, text } = statusColor(status);
  return (
    <span style={{ ...s.chip, background: bg, color: text, fontWeight: 700, flexShrink: 0 }}>
      HTTP {status}
    </span>
  );
}

function truncateUrl(url, max = 52) {
  if (!url) return '';
  try {
    const u = new URL(url);
    const short = u.hostname + u.pathname;
    return short.length > max ? short.slice(0, max) + '…' : short;
  } catch {
    return url.length > max ? url.slice(0, max) + '…' : url;
  }
}

// ─────────────────────────────────────────────────────────────────────────────

export default function PayloadLibrary() {
  // ── Payload list ──────────────────────────────────────────────────────────
  const [payloads, setPayloads]           = useState([]);
  const [fetchStatus, setFetchStatus]     = useState('loading');
  const [errorMsg, setErrorMsg]           = useState('');
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [copied, setCopied]               = useState(false);

  // ── Search & grouping ─────────────────────────────────────────────────────
  const [searchQuery, setSearchQuery]             = useState('');
  const [collapsedCategories, setCollapsedCategories] = useState(new Set());
  const searchInputRef                            = useRef(null);

  // ── Provider config (from backend) ───────────────────────────────────────
  const [providerConfig, setProviderConfig] = useState({
    urls: [],
    authType: 'none',
    timeoutMs: 15000,
  });

  // ── URL selection ─────────────────────────────────────────────────────────
  // urlMode: 'preset' = one of the backend-configured URLs; 'custom' = user-typed
  const [urlMode, setUrlMode]             = useState('preset');
  const [selectedUrlIdx, setSelectedUrlIdx] = useState(0);
  const [customUrl, setCustomUrl]         = useState('');

  // ── Auth type (per-request dropdown) ─────────────────────────────────────
  // Defaults to 'none'; pre-populated from provider-config.defaultAuthType on mount
  const [authType, setAuthType]           = useState('none');

  // ── Payload editing ───────────────────────────────────────────────────────
  // editedPayload: null = use original from YAML; string = user has modified it
  const [isEditingPayload, setIsEditingPayload]   = useState(false);
  const [editedPayload, setEditedPayload]         = useState(null);
  const [payloadParseError, setPayloadParseError] = useState('');

  // ── Run state ─────────────────────────────────────────────────────────────
  const [runState, setRunState]   = useState('idle'); // 'idle' | 'running'
  const [runResult, setRunResult] = useState(null);   // latest run's { status, body, durationMs } or { error, code, durationMs }
  const [responseCopied, setResponseCopied] = useState(false);

  // ── History ───────────────────────────────────────────────────────────────
  const [history, setHistory]           = useState([]);
  const [historyOpen, setHistoryOpen]   = useState(true);
  const [viewingEntry, setViewingEntry] = useState(null); // null = show current runResult

  // ── Load payloads ─────────────────────────────────────────────────────────
  useEffect(() => {
    fetch(`${import.meta.env.BASE_URL}payloads.yaml`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.text();
      })
      .then((text) => {
        const parsed = jsYaml.load(text);
        if (!parsed?.payloads?.length) throw new Error('payloads key missing or empty');
        setPayloads(parsed.payloads);
        setFetchStatus('ready');
      })
      .catch((err) => {
        setErrorMsg(`Could not load payloads: ${err.message}`);
        setFetchStatus('error');
      });
  }, []);

  // ── Load provider config ──────────────────────────────────────────────────
  useEffect(() => {
    fetch(`${import.meta.env.BASE_URL}provider-config`)
      .then((res) => (res.ok ? res.json() : null))
      .then((cfg) => {
        if (!cfg) return;
        setProviderConfig(cfg);
        if (!cfg.urls || cfg.urls.length === 0) setUrlMode('custom');
        // Pre-select auth type from backend default (operator-configured via env var)
        if (cfg.defaultAuthType && cfg.defaultAuthType !== 'none') {
          setAuthType(cfg.defaultAuthType);
        }
      })
      .catch(() => setUrlMode('custom'));
  }, []);

  // ── Cmd/Ctrl+K focuses search input ──────────────────────────────────────
  useEffect(() => {
    function onKeyDown(e) {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        searchInputRef.current?.focus();
      }
    }
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  // ── Search filter + category grouping helpers ─────────────────────────────
  function getFilteredPayloads() {
    const q = searchQuery.trim().toLowerCase();
    if (!q) return payloads.map((p, i) => ({ ...p, originalIndex: i }));
    return payloads.reduce((acc, p, i) => {
      if (
        p.name.toLowerCase().includes(q) ||
        p.category?.toLowerCase().includes(q) ||
        p.description?.toLowerCase().includes(q) ||
        p.tags?.some((t) => t.toLowerCase().includes(q))
      ) {
        acc.push({ ...p, originalIndex: i });
      }
      return acc;
    }, []);
  }

  function getGrouped(items) {
    return items.reduce((acc, p) => {
      const cat = p.category || 'Uncategorised';
      if (!acc[cat]) acc[cat] = [];
      acc[cat].push(p);
      return acc;
    }, {});
  }

  function toggleCategory(cat) {
    setCollapsedCategories((prev) => {
      const next = new Set(prev);
      next.has(cat) ? next.delete(cat) : next.add(cat);
      return next;
    });
  }

  // ── Helpers ───────────────────────────────────────────────────────────────
  function getActiveUrl() {
    if (urlMode === 'custom') return customUrl.trim();
    const entry = providerConfig.urls[selectedUrlIdx];
    return entry ? entry.url : '';
  }

  function getPrettyJson(entry) {
    try { return JSON.stringify(JSON.parse(entry.payload), null, 2); }
    catch { return entry.payload; }
  }

  // Returns the payload text that will actually be sent — edited string if
  // the user has modified it, otherwise the pretty-printed original.
  function getEffectivePayloadStr() {
    return editedPayload !== null ? editedPayload : getPrettyJson(payloads[selectedIndex]);
  }

  function handleCopyPayload() {
    navigator.clipboard.writeText(getEffectivePayloadStr()).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1800);
    });
  }

  function handleEditPayload() {
    // Pre-populate textarea with the current effective payload on first open
    if (editedPayload === null) setEditedPayload(getPrettyJson(payloads[selectedIndex]));
    setIsEditingPayload(true);
  }

  function handleResetPayload() {
    setEditedPayload(null);
    setIsEditingPayload(false);
    setPayloadParseError('');
  }

  function handlePayloadChange(val) {
    setEditedPayload(val);
    try { JSON.parse(val); setPayloadParseError(''); }
    catch (err) { setPayloadParseError(err.message); }
  }

  function handleCopyResponse() {
    const displayed = viewingEntry || runResult;
    if (!displayed) return;
    const text = displayed.error
      ? displayed.error
      : typeof displayed.body === 'object'
        ? JSON.stringify(displayed.body, null, 2)
        : String(displayed.body ?? '');
    navigator.clipboard.writeText(text).then(() => {
      setResponseCopied(true);
      setTimeout(() => setResponseCopied(false), 1800);
    });
  }

  // ── Run ───────────────────────────────────────────────────────────────────
  async function handleRun() {
    const url = getActiveUrl();
    if (!url || runState === 'running') return;

    setViewingEntry(null);
    setRunResult(null);
    setRunState('running');

    const payload = payloads[selectedIndex];
    let payloadObj;
    // Use the edited payload string if the user modified it, otherwise the original
    const rawStr = editedPayload !== null ? editedPayload : payload.payload;
    try { payloadObj = JSON.parse(rawStr); }
    catch { payloadObj = rawStr; }

    const requestedAt = new Date().toLocaleTimeString();
    const payloadName = payload.name;

    try {
      const resp = await fetch(`${import.meta.env.BASE_URL}run-payload`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ payload: payloadObj, url, authType }),
      });

      const data = await resp.json();

      // resp.ok → backend proxy succeeded; data.status is the provider's HTTP code
      // !resp.ok → backend itself failed (timeout/network); data has .error, .code
      const result = resp.ok
        ? data
        : { error: data.error, code: data.code, durationMs: data.durationMs };

      setRunResult(result);
      setRunState('idle');
      addToHistory({ payloadName, url, requestedAt, ...result });
      setHistoryOpen(true);
    } catch (err) {
      const result = { error: err.message, code: 'NETWORK' };
      setRunResult(result);
      setRunState('idle');
      addToHistory({ payloadName, url, requestedAt, ...result });
      setHistoryOpen(true);
    }
  }

  function addToHistory(entry) {
    setHistory((prev) => [{ id: Date.now(), ...entry }, ...prev].slice(0, MAX_HISTORY));
  }

  function handleSelectPayload(i) {
    setSelectedIndex(i);
    // Clear response and edit state when switching payloads
    setViewingEntry(null);
    setRunResult(null);
    setEditedPayload(null);
    setIsEditingPayload(false);
    setPayloadParseError('');
  }

  function handleClearHistory() {
    setHistory([]);
    setViewingEntry(null);
    setHistoryOpen(false);
  }

  // ─────────────────────────────────────────────────────────────────────────
  if (fetchStatus === 'loading') {
    return (
      <div style={s.screen}>
        <div style={s.spinner} />
        <p style={s.mutedText}>Loading payloads…</p>
      </div>
    );
  }

  if (fetchStatus === 'error') {
    return (
      <div style={s.screen}>
        <p style={s.errorText}>{errorMsg}</p>
      </div>
    );
  }

  const selected        = payloads[selectedIndex];
  const activeUrl       = getActiveUrl();
  const payloadIsValid  = editedPayload === null || payloadParseError === '';
  const canRun          = runState === 'idle' && activeUrl.length > 0 && payloadIsValid;
  const isModified      = editedPayload !== null;
  const displayedResult = viewingEntry || runResult;

  // Search / grouping derived values
  const isSearching      = searchQuery.trim().length > 0;
  const filteredPayloads = getFilteredPayloads();
  const grouped          = isSearching ? null : getGrouped(filteredPayloads);

  return (
    <div style={s.page}>
      <div style={s.container}>
        <header style={s.header}>
          <h1 style={s.title}>Payload Library</h1>
          <p style={s.subtitle}>
            {payloads.length} sample API payloads — select, inspect, and run against a provider.
          </p>
        </header>

        <div style={s.columns}>
          {/* ── Left: search + payload list ─────────────────────────────── */}
          <div style={s.listCol}>

            {/* Search bar */}
            <div style={s.searchBar}>
              <span style={s.searchIcon}>⌕</span>
              <input
                ref={searchInputRef}
                type="text"
                placeholder="Search… (⌘K)"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Escape') { setSearchQuery(''); e.target.blur(); }
                }}
                style={s.searchInput}
                spellCheck={false}
              />
              {searchQuery && (
                <button onClick={() => setSearchQuery('')} style={s.searchClear} title="Clear">×</button>
              )}
            </div>

            {/* Match count shown only while searching */}
            {isSearching && (
              <p style={s.searchCount}>
                {filteredPayloads.length} of {payloads.length}
              </p>
            )}

            {/* List body */}
            <div style={s.list}>
              {/* ── Flat list when searching ── */}
              {isSearching && filteredPayloads.map((p) => (
                <button
                  key={p.originalIndex}
                  onClick={() => handleSelectPayload(p.originalIndex)}
                  style={{ ...s.listItem, ...(p.originalIndex === selectedIndex ? s.listItemActive : {}) }}
                >
                  <span style={s.listIndex}>{String(p.originalIndex + 1).padStart(2, '0')}</span>
                  <div style={s.listItemText}>
                    <span style={s.listName}>{p.name}</span>
                    {p.description && <span style={s.listDescription}>{p.description}</span>}
                  </div>
                  {p.category && (
                    <span style={s.listCatPill}>{p.category}</span>
                  )}
                  {p.originalIndex === selectedIndex && <span style={s.listArrow}>›</span>}
                </button>
              ))}

              {isSearching && filteredPayloads.length === 0 && (
                <p style={s.searchEmpty}>No payloads match "{searchQuery}"</p>
              )}

              {/* ── Grouped list when not searching ── */}
              {!isSearching && grouped && Object.entries(grouped).map(([cat, items]) => (
                <div key={cat}>
                  <button
                    onClick={() => toggleCategory(cat)}
                    style={s.categoryHeader}
                  >
                    <span style={s.categoryChevron}>
                      {collapsedCategories.has(cat) ? '▸' : '▾'}
                    </span>
                    <span style={s.categoryName}>{cat}</span>
                    <span style={s.categoryCount}>{items.length}</span>
                  </button>

                  {!collapsedCategories.has(cat) && items.map((p) => (
                    <button
                      key={p.originalIndex}
                      onClick={() => handleSelectPayload(p.originalIndex)}
                      style={{ ...s.listItem, ...s.listItemIndented, ...(p.originalIndex === selectedIndex ? s.listItemActive : {}) }}
                    >
                      <span style={s.listIndex}>{String(p.originalIndex + 1).padStart(2, '0')}</span>
                      <div style={s.listItemText}>
                        <span style={s.listName}>{p.name}</span>
                        {p.description && <span style={s.listDescription}>{p.description}</span>}
                      </div>
                      {p.originalIndex === selectedIndex && <span style={s.listArrow}>›</span>}
                    </button>
                  ))}
                </div>
              ))}
            </div>
          </div>

          {/* ── Right: stacked detail ───────────────────────────────────── */}
          <div style={s.detailCol}>

            {/* ── URL / Auth card ──────────────────────────────────────── */}
            <div style={s.card}>
              <div style={s.cardTopRow}>
                <span style={s.sectionLabel}>Provider URL</span>
                <select
                  value={authType}
                  onChange={(e) => setAuthType(e.target.value)}
                  style={s.authSelect}
                  title="Authentication mode for this run"
                >
                  <option value="none">No Auth</option>
                  <option value="api-key">API Key</option>
                  <option value="entraid-apigee">EntraID + APIGEE</option>
                </select>
              </div>

              <div style={s.urlRow}>
                {/* Preset dropdown — only shown when backend has configured URLs */}
                {providerConfig.urls.length > 0 && (
                  <select
                    value={urlMode === 'preset' ? `p:${selectedUrlIdx}` : 'custom'}
                    onChange={(e) => {
                      const v = e.target.value;
                      if (v === 'custom') {
                        setUrlMode('custom');
                      } else {
                        setUrlMode('preset');
                        setSelectedUrlIdx(parseInt(v.slice(2), 10));
                      }
                    }}
                    style={s.urlSelect}
                  >
                    {providerConfig.urls.map((u, i) => (
                      <option key={i} value={`p:${i}`}>{u.label}</option>
                    ))}
                    <option value="custom">Custom URL…</option>
                  </select>
                )}

                {/* Custom URL input — shown when custom mode or no presets */}
                {(urlMode === 'custom' || providerConfig.urls.length === 0) && (
                  <input
                    type="url"
                    placeholder="https://provider.example.com/api/endpoint"
                    value={customUrl}
                    onChange={(e) => setCustomUrl(e.target.value)}
                    style={s.urlInput}
                    spellCheck={false}
                  />
                )}
              </div>

              {/* Show the full URL when preset is selected */}
              {urlMode === 'preset' && providerConfig.urls[selectedUrlIdx] && (
                <p style={s.urlHint}>{providerConfig.urls[selectedUrlIdx].url}</p>
              )}
            </div>

            {/* ── Payload card ─────────────────────────────────────────── */}
            <div style={s.card}>
              <div style={s.payloadHeaderRow}>
                <div>
                  <h2 style={s.detailTitle}>{selected.name}</h2>
                  <span style={s.detailBadge}>JSON Payload</span>
                  {isModified && (
                    <span style={s.modifiedBadge}>Modified</span>
                  )}
                </div>
                <div style={s.btnRow}>
                  <button
                    onClick={handleCopyPayload}
                    style={{ ...s.outlineBtn, ...(copied ? s.outlineBtnOk : {}) }}
                  >
                    {copied ? '✓ Copied' : 'Copy'}
                  </button>
                  {/* Edit / Reset toggle */}
                  {!isEditingPayload ? (
                    <button
                      onClick={handleEditPayload}
                      style={s.outlineBtn}
                      title="Edit payload before running"
                    >
                      Edit
                    </button>
                  ) : (
                    <button
                      onClick={handleResetPayload}
                      style={s.resetBtn}
                      title="Discard edits and restore original"
                    >
                      Reset
                    </button>
                  )}
                  <button
                    onClick={handleRun}
                    disabled={!canRun}
                    style={{ ...s.runBtn, ...(!canRun ? s.runBtnDisabled : {}) }}
                    title={
                      !activeUrl ? 'Enter a provider URL above to run' :
                      payloadParseError ? 'Fix JSON errors before running' : ''
                    }
                  >
                    {runState === 'running'
                      ? <><span style={s.btnSpinner} />{'Running…'}</>
                      : '▶ Run'
                    }
                  </button>
                </div>
              </div>

              {/* Payload body — textarea in edit mode, pre otherwise */}
              {isEditingPayload ? (
                <>
                  <textarea
                    value={getEffectivePayloadStr()}
                    onChange={(e) => handlePayloadChange(e.target.value)}
                    style={s.editTextarea}
                    spellCheck={false}
                    autoFocus
                  />
                  {payloadParseError && (
                    <p style={s.parseError}>⚠ Invalid JSON: {payloadParseError}</p>
                  )}
                </>
              ) : (
                <pre style={s.pre}>{getEffectivePayloadStr()}</pre>
              )}
            </div>

            {/* ── Response card ────────────────────────────────────────── */}
            {(runState === 'running' || displayedResult) && (
              <div style={s.card}>
                <div style={s.cardTopRow}>
                  <span style={s.sectionLabel}>
                    {viewingEntry
                      ? `History · ${viewingEntry.payloadName} · ${viewingEntry.requestedAt}`
                      : 'Provider Response'}
                  </span>
                  <div style={s.responseMetaRow}>
                    {viewingEntry && (
                      <button onClick={() => setViewingEntry(null)} style={s.ghostBtn}>
                        ← Current
                      </button>
                    )}
                    {displayedResult && !displayedResult.error && (
                      <>
                        <StatusBadge status={displayedResult.status} />
                        {displayedResult.durationMs != null && (
                          <span style={s.chip}>{displayedResult.durationMs} ms</span>
                        )}
                        <button
                          onClick={handleCopyResponse}
                          style={{ ...s.outlineBtn, ...(responseCopied ? s.outlineBtnOk : {}) }}
                        >
                          {responseCopied ? '✓' : 'Copy'}
                        </button>
                      </>
                    )}
                    {displayedResult?.error && (
                      <span style={{ ...s.chip, background: '#fef2f2', color: '#b91c1c' }}>
                        {displayedResult.code === 'TIMEOUT' ? 'Timeout' : 'Error'}
                      </span>
                    )}
                  </div>
                </div>

                {runState === 'running' && !displayedResult ? (
                  <div style={s.responseLoading}>
                    <div style={s.spinner} />
                    <p style={s.mutedText}>Calling provider…</p>
                  </div>
                ) : displayedResult?.error ? (
                  <div style={s.responseErrorBox}>
                    <p style={s.errorText}>{displayedResult.error}</p>
                    {displayedResult.durationMs != null && (
                      <p style={{ ...s.mutedText, marginTop: '0.25rem' }}>
                        {displayedResult.durationMs} ms
                      </p>
                    )}
                  </div>
                ) : (
                  <pre style={s.pre}>
                    {typeof displayedResult.body === 'object'
                      ? JSON.stringify(displayedResult.body, null, 2)
                      : String(displayedResult.body ?? '')}
                  </pre>
                )}
              </div>
            )}

            {/* ── History card ─────────────────────────────────────────── */}
            {history.length > 0 && (
              <div style={s.card}>
                <div style={s.cardTopRow}>
                  <button
                    onClick={() => setHistoryOpen((v) => !v)}
                    style={s.ghostBtn}
                  >
                    <span style={s.sectionLabel}>
                      {historyOpen ? '▾' : '▸'} Run History ({history.length})
                    </span>
                  </button>
                  <button onClick={handleClearHistory} style={s.ghostBtn}>
                    Clear all
                  </button>
                </div>

                {historyOpen && (
                  <div style={s.historyList}>
                    {history.map((entry) => (
                      <button
                        key={entry.id}
                        onClick={() => setViewingEntry(entry)}
                        style={{
                          ...s.historyItem,
                          ...(viewingEntry?.id === entry.id ? s.historyItemActive : {}),
                        }}
                      >
                        <span style={s.historyName}>{entry.payloadName}</span>
                        <div style={s.historyMeta}>
                          {entry.error ? (
                            <span style={{ ...s.chip, background: '#fef2f2', color: '#b91c1c', fontSize: '0.7rem' }}>
                              {entry.code === 'TIMEOUT' ? 'Timeout' : 'Error'}
                            </span>
                          ) : (
                            <StatusBadge status={entry.status} />
                          )}
                          {entry.durationMs != null && (
                            <span style={{ ...s.chip, fontSize: '0.7rem' }}>{entry.durationMs} ms</span>
                          )}
                          <span style={s.historyTime}>{entry.requestedAt}</span>
                          <span style={s.historyUrl}>{truncateUrl(entry.url)}</span>
                        </div>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            )}

          </div>{/* end detailCol */}
        </div>
      </div>
    </div>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────
const s = {
  page: {
    padding: '2.5rem 2rem',
    animation: 'fadeIn 0.3s ease',
  },
  container: {
    maxWidth: 960,
    margin: '0 auto',
  },
  header: {
    marginBottom: '1.75rem',
  },
  title: {
    fontSize: '1.6rem',
    fontWeight: 800,
    color: 'var(--text-primary)',
    margin: 0,
    marginBottom: '0.375rem',
  },
  subtitle: {
    fontSize: '0.9rem',
    color: 'var(--text-secondary)',
    margin: 0,
  },
  columns: {
    display: 'grid',
    gridTemplateColumns: '260px 1fr',
    gap: '1.25rem',
    alignItems: 'start',
  },

  // ── Left column (search + list) ───────────────────────────────────────────
  listCol: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0',
    background: 'var(--surface)',
    border: '1px solid var(--border)',
    borderRadius: 'var(--radius)',
    boxShadow: 'var(--shadow)',
    overflow: 'hidden',
  },

  // ── Search bar ────────────────────────────────────────────────────────────
  searchBar: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.375rem',
    padding: '0.5rem 0.625rem',
    borderBottom: '1px solid var(--border)',
    background: 'var(--bg)',
  },
  searchIcon: {
    fontSize: '0.95rem',
    color: 'var(--text-secondary)',
    flexShrink: 0,
    lineHeight: 1,
  },
  searchInput: {
    flex: 1,
    border: 'none',
    background: 'transparent',
    outline: 'none',
    fontSize: '0.8rem',
    color: 'var(--text-primary)',
    fontFamily: 'inherit',
    minWidth: 0,
  },
  searchClear: {
    background: 'none',
    border: 'none',
    cursor: 'pointer',
    fontSize: '1rem',
    color: 'var(--text-secondary)',
    padding: '0 0.125rem',
    lineHeight: 1,
    flexShrink: 0,
  },
  searchCount: {
    margin: 0,
    padding: '0.25rem 0.75rem',
    fontSize: '0.68rem',
    color: 'var(--text-secondary)',
    borderBottom: '1px solid var(--border)',
    background: 'var(--bg)',
  },
  searchEmpty: {
    margin: 0,
    padding: '1rem 0.75rem',
    fontSize: '0.78rem',
    color: 'var(--text-secondary)',
    textAlign: 'center',
  },

  // ── List body ─────────────────────────────────────────────────────────────
  list: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0',
    maxHeight: '70vh',
    overflowY: 'auto',
    padding: '0.375rem',
  },

  // ── Category group header ─────────────────────────────────────────────────
  categoryHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.375rem',
    width: '100%',
    padding: '0.4rem 0.5rem',
    marginTop: '0.25rem',
    background: 'none',
    border: 'none',
    borderRadius: '6px',
    cursor: 'pointer',
    fontFamily: 'inherit',
    textAlign: 'left',
  },
  categoryChevron: {
    fontSize: '0.65rem',
    color: 'var(--text-secondary)',
    flexShrink: 0,
    width: '0.75rem',
  },
  categoryName: {
    fontSize: '0.68rem',
    fontWeight: 700,
    letterSpacing: '0.06em',
    textTransform: 'uppercase',
    color: 'var(--text-secondary)',
    flex: 1,
  },
  categoryCount: {
    fontSize: '0.65rem',
    fontWeight: 600,
    color: 'var(--text-secondary)',
    background: 'var(--border)',
    borderRadius: '999px',
    padding: '1px 6px',
    flexShrink: 0,
  },

  // ── List items ────────────────────────────────────────────────────────────
  listItem: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
    padding: '0.4rem 0.5rem',
    borderRadius: '7px',
    border: '1px solid transparent',
    background: 'transparent',
    cursor: 'pointer',
    width: '100%',
    textAlign: 'left',
    fontFamily: 'inherit',
    color: 'var(--text-primary)',
    transition: 'all 0.15s ease',
  },
  listItemIndented: {
    paddingLeft: '1.125rem',
  },
  listItemActive: {
    background: 'var(--accent-light)',
    borderColor: 'var(--accent)',
  },
  listIndex: {
    fontSize: '0.62rem',
    fontWeight: 700,
    color: 'var(--text-secondary)',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    flexShrink: 0,
    width: '1.4rem',
  },
  listItemText: {
    flex: 1,
    minWidth: 0,
    display: 'flex',
    flexDirection: 'column',
    gap: '1px',
  },
  listName: {
    fontSize: '0.78rem',
    fontWeight: 600,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  listDescription: {
    fontSize: '0.67rem',
    color: 'var(--text-secondary)',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  listCatPill: {
    fontSize: '0.6rem',
    fontWeight: 600,
    color: 'var(--accent-dark)',
    background: 'var(--accent-light)',
    borderRadius: '4px',
    padding: '1px 5px',
    flexShrink: 0,
    whiteSpace: 'nowrap',
  },
  listArrow: {
    color: 'var(--accent)',
    fontWeight: 700,
    fontSize: '1rem',
    flexShrink: 0,
  },

  // ── Detail column ─────────────────────────────────────────────────────────
  detailCol: {
    display: 'flex',
    flexDirection: 'column',
    gap: '1rem',
  },

  // ── Card shell ────────────────────────────────────────────────────────────
  card: {
    background: 'var(--surface)',
    border: '1px solid var(--border)',
    borderRadius: 'var(--radius)',
    boxShadow: 'var(--shadow)',
    overflow: 'hidden',
  },
  cardTopRow: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '0.625rem 1rem',
    borderBottom: '1px solid var(--border)',
    gap: '0.75rem',
    flexWrap: 'wrap',
    background: 'var(--bg)',
  },
  sectionLabel: {
    fontSize: '0.72rem',
    fontWeight: 700,
    letterSpacing: '0.07em',
    textTransform: 'uppercase',
    color: 'var(--text-secondary)',
  },

  // ── Auth type dropdown ────────────────────────────────────────────────────
  authSelect: {
    padding: '0.25rem 0.5rem',
    borderRadius: '6px',
    border: '1.5px solid var(--border)',
    background: 'var(--bg)',
    color: 'var(--text-primary)',
    fontFamily: 'inherit',
    fontSize: '0.75rem',
    fontWeight: 600,
    cursor: 'pointer',
  },

  // ── URL section ───────────────────────────────────────────────────────────
  urlRow: {
    display: 'flex',
    gap: '0.5rem',
    padding: '0.75rem 1rem',
    flexWrap: 'wrap',
  },
  urlSelect: {
    flex: '0 0 auto',
    padding: '0.4rem 0.65rem',
    borderRadius: '8px',
    border: '1.5px solid var(--border)',
    background: 'var(--bg)',
    color: 'var(--text-primary)',
    fontFamily: 'inherit',
    fontSize: '0.825rem',
    fontWeight: 600,
    cursor: 'pointer',
    minWidth: '9rem',
  },
  urlInput: {
    flex: 1,
    minWidth: '16rem',
    padding: '0.4rem 0.65rem',
    borderRadius: '8px',
    border: '1.5px solid var(--border)',
    background: 'var(--bg)',
    color: 'var(--text-primary)',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    fontSize: '0.78rem',
    outline: 'none',
  },
  urlHint: {
    margin: '0 1rem 0.75rem',
    fontSize: '0.72rem',
    color: 'var(--text-secondary)',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    wordBreak: 'break-all',
  },

  // ── Payload section ───────────────────────────────────────────────────────
  payloadHeaderRow: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '0.875rem 1.125rem',
    borderBottom: '1px solid var(--border)',
    gap: '1rem',
    flexWrap: 'wrap',
  },
  detailTitle: {
    fontSize: '0.95rem',
    fontWeight: 700,
    color: 'var(--text-primary)',
    margin: 0,
    marginBottom: '0.2rem',
  },
  detailBadge: {
    display: 'inline-block',
    fontSize: '0.65rem',
    fontWeight: 700,
    letterSpacing: '0.06em',
    color: 'var(--accent-dark)',
    background: 'var(--accent-light)',
    borderRadius: '5px',
    padding: '2px 7px',
    marginRight: '0.375rem',
  },
  modifiedBadge: {
    display: 'inline-block',
    fontSize: '0.65rem',
    fontWeight: 700,
    letterSpacing: '0.06em',
    color: '#92400e',
    background: '#fef3c7',
    borderRadius: '5px',
    padding: '2px 7px',
  },
  btnRow: {
    display: 'flex',
    gap: '0.5rem',
    flexShrink: 0,
  },

  // ── Pre ───────────────────────────────────────────────────────────────────
  pre: {
    margin: 0,
    padding: '1rem 1.125rem',
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    fontSize: '0.78rem',
    lineHeight: 1.6,
    color: 'var(--text-primary)',
    background: 'var(--bg)',
    overflowX: 'auto',
    whiteSpace: 'pre',
    maxHeight: '26rem',
    overflowY: 'auto',
  },

  // ── Editable payload textarea ─────────────────────────────────────────────
  editTextarea: {
    display: 'block',
    width: '100%',
    minHeight: '16rem',
    maxHeight: '26rem',
    margin: 0,
    padding: '1rem 1.125rem',
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    fontSize: '0.78rem',
    lineHeight: 1.6,
    color: 'var(--text-primary)',
    background: 'var(--bg)',
    border: 'none',
    borderTop: '1px solid var(--border)',
    outline: 'none',
    resize: 'vertical',
    boxSizing: 'border-box',
    overflowY: 'auto',
  },
  parseError: {
    margin: 0,
    padding: '0.5rem 1.125rem',
    fontSize: '0.72rem',
    color: '#b91c1c',
    background: '#fef2f2',
    borderTop: '1px solid #fecaca',
  },

  // ── Buttons ───────────────────────────────────────────────────────────────
  outlineBtn: {
    padding: '0.4rem 0.875rem',
    borderRadius: '8px',
    border: '1.5px solid var(--accent)',
    background: 'transparent',
    color: 'var(--accent)',
    fontFamily: 'inherit',
    fontSize: '0.78rem',
    fontWeight: 600,
    cursor: 'pointer',
    transition: 'all 0.15s ease',
    flexShrink: 0,
  },
  outlineBtnOk: {
    background: '#f0fdf4',
    borderColor: '#22c55e',
    color: '#15803d',
  },
  resetBtn: {
    padding: '0.4rem 0.875rem',
    borderRadius: '8px',
    border: '1.5px solid #f59e0b',
    background: 'transparent',
    color: '#92400e',
    fontFamily: 'inherit',
    fontSize: '0.78rem',
    fontWeight: 600,
    cursor: 'pointer',
    transition: 'all 0.15s ease',
    flexShrink: 0,
  },
  runBtn: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.375rem',
    padding: '0.4rem 1rem',
    borderRadius: '8px',
    border: 'none',
    background: 'var(--accent)',
    color: '#fff',
    fontFamily: 'inherit',
    fontSize: '0.78rem',
    fontWeight: 700,
    cursor: 'pointer',
    transition: 'opacity 0.15s ease',
    flexShrink: 0,
  },
  runBtnDisabled: {
    opacity: 0.45,
    cursor: 'not-allowed',
  },
  btnSpinner: {
    display: 'inline-block',
    width: 12,
    height: 12,
    borderRadius: '50%',
    border: '2px solid rgba(255,255,255,0.35)',
    borderTopColor: '#fff',
    animation: 'spin 0.8s linear infinite',
    flexShrink: 0,
  },
  ghostBtn: {
    background: 'none',
    border: 'none',
    padding: '0.2rem 0.4rem',
    cursor: 'pointer',
    fontFamily: 'inherit',
    fontSize: '0.78rem',
    color: 'var(--text-secondary)',
    fontWeight: 600,
    borderRadius: '6px',
    transition: 'color 0.15s',
  },

  // ── Chips ─────────────────────────────────────────────────────────────────
  chip: {
    display: 'inline-flex',
    alignItems: 'center',
    fontSize: '0.72rem',
    fontWeight: 600,
    padding: '2px 8px',
    borderRadius: '999px',
    background: '#f3f4f6',
    color: '#374151',
    whiteSpace: 'nowrap',
  },

  // ── Response section ──────────────────────────────────────────────────────
  responseMetaRow: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
    flexWrap: 'wrap',
  },
  responseLoading: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    padding: '2rem',
    gap: '0.75rem',
  },
  responseErrorBox: {
    padding: '1rem 1.125rem',
    background: 'var(--bg)',
  },

  // ── History ───────────────────────────────────────────────────────────────
  historyList: {
    display: 'flex',
    flexDirection: 'column',
    gap: '2px',
    padding: '0.5rem',
    maxHeight: '18rem',
    overflowY: 'auto',
  },
  historyItem: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.25rem',
    padding: '0.5rem 0.75rem',
    borderRadius: '8px',
    border: '1px solid transparent',
    background: 'transparent',
    cursor: 'pointer',
    width: '100%',
    textAlign: 'left',
    fontFamily: 'inherit',
    transition: 'all 0.15s ease',
  },
  historyItemActive: {
    background: 'var(--accent-light)',
    borderColor: 'var(--accent)',
  },
  historyName: {
    fontSize: '0.8rem',
    fontWeight: 600,
    color: 'var(--text-primary)',
  },
  historyMeta: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.4rem',
    flexWrap: 'wrap',
  },
  historyTime: {
    fontSize: '0.68rem',
    color: 'var(--text-secondary)',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace',
  },
  historyUrl: {
    fontSize: '0.68rem',
    color: 'var(--text-secondary)',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    maxWidth: '28ch',
  },

  // ── Screens (loading / error) ─────────────────────────────────────────────
  screen: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    minHeight: '60vh',
    gap: '1rem',
  },
  spinner: {
    width: 32,
    height: 32,
    borderRadius: '50%',
    border: '3px solid var(--accent-light)',
    borderTopColor: 'var(--accent)',
    animation: 'spin 0.8s linear infinite',
  },
  mutedText: {
    color: 'var(--text-secondary)',
    fontSize: '0.9rem',
    margin: 0,
  },
  errorText: {
    color: '#dc2626',
    fontSize: '0.875rem',
    margin: 0,
    textAlign: 'center',
    maxWidth: 400,
  },
};
