import { useState, useEffect, useRef } from 'react';
import jsYaml from 'js-yaml';

const MAX_HISTORY = 20;
const MAX_HISTORY_BODY_CHARS = 50_000; // cap stored response body size per history entry

// ── Truncate a run-history entry's response body ──────────────────────────────
// `entry.body` may be a large object (provider JSON response) or string.
// History only needs enough to preview/debug — full response stays available
// in `runResult` for the live view. Caps memory growth across MAX_HISTORY
// entries × multiple env tabs.
function truncateHistoryEntry(entry) {
  if (entry?.body == null) return entry;
  const str = typeof entry.body === 'string' ? entry.body : JSON.stringify(entry.body);
  if (str.length <= MAX_HISTORY_BODY_CHARS) return entry;
  return {
    ...entry,
    body: str.slice(0, MAX_HISTORY_BODY_CHARS) + `\n…[truncated ${str.length - MAX_HISTORY_BODY_CHARS} more chars]`,
    bodyTruncated: true,
  };
}

// ── Troubleshooting hints for failed provider calls ───────────────────────────
// `code` is set by the backend (TIMEOUT | NETWORK); `message` is the enriched
// error string (may include the underlying cause, e.g. TLS/DNS failure detail).
function errorHint(code, message = '') {
  if (code === 'TIMEOUT') {
    return 'The provider did not respond in time. Check that the URL is reachable from the backend container, or increase PROVIDER_TIMEOUT_MS.';
  }
  const msg = message.toLowerCase();
  if (msg.includes('certificate') || msg.includes('tls') || msg.includes('ssl')) {
    return 'TLS/certificate error. If this endpoint uses a self-signed or internally-issued certificate, set this URL\'s "Skip TLS verify" option, or install the CA in the backend container.';
  }
  if (msg.includes('enotfound') || msg.includes('getaddrinfo')) {
    return 'DNS lookup failed. Check that the hostname is correct and resolvable from the backend container.';
  }
  if (msg.includes('econnrefused')) {
    return 'Connection refused. The host is reachable but nothing is listening on that port/path — check the URL and that the service is up.';
  }
  if (msg.includes('401') || msg.includes('403') || msg.includes('unauthorized') || msg.includes('forbidden')) {
    return 'Authorization failed. Check the selected Auth Type and that the corresponding credentials are configured for this environment.';
  }
  return 'Check the URL, Auth Type, and credentials for this environment, and confirm the backend container can reach this host.';
}

// ── Per-tab default state factory ─────────────────────────────────────────────
// Each environment tab gets its own independent state slice.
// `env` is the environment object from /provider-config (tab mode)
// or { urls: [...] } (legacy mode).  Pass null for the very first render.
function makeTabState(env) {
  const firstUrl = env?.urls?.[0];
  return {
    fetchStatus: 'idle',       // 'idle' | 'loading' | 'ready' | 'error'
    payloads: [],
    errorMsg: '',
    selectedIndex: 0,
    copied: false,
    searchQuery: '',
    collapsedCategories: new Set(),
    urlMode: (env?.urls?.length > 0) ? 'preset' : 'custom',
    selectedUrlIdx: 0,
    customUrl: '',
    authType:       firstUrl?.authType       || 'none',
    skipTlsVerify:  firstUrl?.skipTlsVerify || false,  // auto-set per URL; custom toggle for ad-hoc URLs
    isEditingPayload: false,
    editedPayload: null,
    payloadParseError: '',
    runState: 'idle',
    runResult: null,
    responseCopied: false,
    history: [],
    historyOpen: true,
    viewingEntry: null,
    // ── Library YAML editor state ─────────────────────────────────────────────
    rawYaml: '',               // raw YAML from last API fetch (pre-populates editor)
    encryptedSource: null,     // true = served from .enc; false = plain fallback
    editorOpen: false,         // YAML editor panel visible?
    editorYaml: '',            // current text in editor textarea
    editorDirty: false,        // user has typed changes not yet saved
    editorError: '',           // YAML validation error while typing
    saveState: 'idle',         // 'idle' | 'saving' | 'saved' | 'error'
    saveError: '',
    // ── Per-tab batch selection ───────────────────────────────────────────────
    batchChecked: new Set(),   // Set of originalIndex values checked in this tab
    batchResults: [],          // [{index, name, status, httpStatus, durationMs, body, error, code}]
  };
}

// ── Status badge ──────────────────────────────────────────────────────────────
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
  // ── Environment config ────────────────────────────────────────────────────
  // environments: [] = legacy mode (no tabs); non-empty = tab mode
  const [environments, setEnvironments] = useState([]);
  const [legacyUrls, setLegacyUrls]     = useState([]);
  const [activeEnvIdx, setActiveEnvIdx] = useState(0);
  const [configLoaded, setConfigLoaded] = useState(false);
  // listCollapsed is component-level (not per-tab) — a layout preference for
  // the current session. Collapses the payload list to give more room to
  // the detail panels.
  const [listCollapsed, setListCollapsed] = useState(false);
  // writeAuthRequired: true when server has PAYLOAD_WRITE_AUTH_ENABLED=true
  const [writeAuthRequired, setWriteAuthRequired] = useState(false);
  // adminToken: entered by operator at runtime when writeAuthRequired is true
  const [adminToken, setAdminToken] = useState('');

  // ── Per-tab state map ─────────────────────────────────────────────────────
  // Key: env.id in tab mode, '__legacy__' in legacy mode
  const [tabStates, setTabStates] = useState({});

  // ── Batch run state (component-level — not per-tab) ───────────────────────
  // batchMode=false by default → full rollback: nothing in the existing UI
  // changes until the user explicitly toggles Batch mode on.
  const [batchMode, setBatchMode]       = useState(false);
  const [batchRunning, setBatchRunning] = useState(false);
  // non-null → user drilled into this payload index from batch results
  const [batchDrillIdx, setBatchDrillIdx] = useState(null);

  // ── Refs ──────────────────────────────────────────────────────────────────
  const searchInputRef = useRef(null);
  const responseCardRef = useRef(null);
  // Tracks which tabs have already had their payload fetch initiated
  const loadedTabsRef = useRef(new Set());
  // Component-lifetime AbortController for in-flight payload-content fetches.
  // Aborted only on full unmount (see effect below) — NOT on tab switches,
  // since loadedTabsRef marks a tab as "loaded" the moment its fetch starts;
  // aborting on switch would strand that tab in fetchStatus:'loading' forever.
  const fetchAbortRef = useRef(null);
  if (fetchAbortRef.current === null) fetchAbortRef.current = new AbortController();
  useEffect(() => () => fetchAbortRef.current.abort(), []);

  // ── Derived values ────────────────────────────────────────────────────────
  const tabMode     = environments.length > 0;
  const activeEnv   = tabMode ? environments[activeEnvIdx] : null;
  const activeEnvId = tabMode ? activeEnv.id : '__legacy__';
  const activeUrls  = tabMode ? (activeEnv?.urls || []) : legacyUrls;

  // ts = active tab's state (snapshot for this render)
  const ts = tabStates[activeEnvId] ?? makeTabState(activeEnv ?? { urls: legacyUrls });

  // upd: update the active tab's state (safe for synchronous event handlers only —
  // use the setTabStates snapshot pattern inside async callbacks to avoid stale closures)
  function upd(patch) {
    const envId = activeEnvId;
    const envRef = activeEnv;
    const urlsRef = legacyUrls;
    setTabStates(prev => ({
      ...prev,
      [envId]: { ...(prev[envId] ?? makeTabState(envRef ?? { urls: urlsRef })), ...patch },
    }));
  }

  // ── Load provider config ──────────────────────────────────────────────────
  useEffect(() => {
    fetch(`${import.meta.env.BASE_URL}provider-config`)
      .then(r => r.ok ? r.json() : null)
      .then(cfg => {
        if (!cfg) { setConfigLoaded(true); return; }

        if (cfg.writeAuthRequired) setWriteAuthRequired(true);

        if (cfg.environments?.length > 0) {
          // ── Tab mode ──────────────────────────────────────────────────────
          setEnvironments(cfg.environments);
          // Initialise all tab states as 'idle' so tab switches work immediately
          setTabStates(
            cfg.environments.reduce((acc, env) => {
              acc[env.id] = makeTabState(env);
              return acc;
            }, {})
          );
        } else {
          // ── Legacy mode (backward compat) ─────────────────────────────────
          const urls = cfg.urls || [];
          setLegacyUrls(urls);
          setTabStates({
            '__legacy__': {
              ...makeTabState({ urls }),
              authType: (cfg.defaultAuthType || 'none'),
              urlMode: urls.length > 0 ? 'preset' : 'custom',
            },
          });
        }
        setConfigLoaded(true);
      })
      .catch(() => {
        setTabStates({ '__legacy__': makeTabState({ urls: [] }) });
        setConfigLoaded(true);
      });
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Load payloads when active tab first becomes visible ───────────────────
  // Runs when activeEnvId changes or when configLoaded flips to true.
  // loadedTabsRef guards against re-fetching a tab that's already been loaded.
  useEffect(() => {
    if (!configLoaded) return;
    if (loadedTabsRef.current.has(activeEnvId)) return;
    loadedTabsRef.current.add(activeEnvId);

    // Snapshot everything needed for the async path — activeEnvId may change
    // before the fetch resolves (user switches tabs mid-flight).
    const envIdSnap = activeEnvId;

    // Mark as loading (using setTabStates directly to avoid stale upd closure)
    setTabStates(prev => ({
      ...prev,
      [envIdSnap]: {
        ...(prev[envIdSnap] ?? makeTabState(activeEnv ?? { urls: legacyUrls })),
        fetchStatus: 'loading',
      },
    }));

    // Per-invocation AbortController so React StrictMode's double-invoke
    // cleanup doesn't cancel the real fetch. The cleanup cancels only the
    // controller created in THIS effect invocation; on remount a fresh one
    // is used. loadedTabsRef is cleared below so a retry fires correctly.
    const ac = new AbortController();

    (async () => {
      try {
        const resp = await fetch(
          `${import.meta.env.BASE_URL}payload-content/${encodeURIComponent(envIdSnap)}`,
          { signal: ac.signal }
        );
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({}));
          throw new Error(err.error || `HTTP ${resp.status}`);
        }
        const data = await resp.json();
        // Collapse all categories by default so the list starts compact.
        // User can click any header or use Expand All to open groups.
        const allCats = new Set(
          (data.payloads || []).map(p => p.category || 'Uncategorised')
        );
        setTabStates(prev => ({
          ...prev,
          [envIdSnap]: {
            ...(prev[envIdSnap] ?? {}),
            fetchStatus: 'ready',
            payloads: data.payloads,
            rawYaml: data.yaml,
            encryptedSource: data.encrypted,
            collapsedCategories: allCats,
          },
        }));
      } catch (e) {
        if (e.name === 'AbortError') return;
        setTabStates(prev => ({
          ...prev,
          [envIdSnap]: {
            ...(prev[envIdSnap] ?? {}),
            fetchStatus: 'error',
            errorMsg: `Could not load payloads for "${envIdSnap}": ${e.message}`,
          },
        }));
      }
    })();

    // Cleanup: abort the in-flight fetch and remove envId from the loaded-set
    // so the effect can retry correctly on StrictMode's second mount.
    return () => {
      ac.abort();
      loadedTabsRef.current.delete(envIdSnap);
    };
  }, [activeEnvId, configLoaded]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Scroll response card into view on run complete ────────────────────────
  useEffect(() => {
    if (ts.runResult) {
      responseCardRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
  }, [ts.runResult]);

  // ── ⌘K / Ctrl+K focuses search ────────────────────────────────────────────
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

  // ── Helpers ───────────────────────────────────────────────────────────────
  function getActiveUrl() {
    if (ts.urlMode === 'custom') return ts.customUrl.trim();
    const entry = activeUrls[ts.selectedUrlIdx];
    return entry ? entry.url : '';
  }

  function getPrettyJson(entry) {
    try { return JSON.stringify(JSON.parse(entry.payload), null, 2); }
    catch { return entry.payload; }
  }

  function getEffectivePayloadStr() {
    return ts.editedPayload !== null
      ? ts.editedPayload
      : getPrettyJson(ts.payloads[ts.selectedIndex]);
  }

  // ── Search & grouping ─────────────────────────────────────────────────────
  function getFilteredPayloads() {
    const q = ts.searchQuery.trim().toLowerCase();
    if (!q) return ts.payloads.map((p, i) => ({ ...p, originalIndex: i }));
    return ts.payloads.reduce((acc, p, i) => {
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
    const next = new Set(ts.collapsedCategories);
    next.has(cat) ? next.delete(cat) : next.add(cat);
    upd({ collapsedCategories: next });
  }

  function handleExpandAll() {
    upd({ collapsedCategories: new Set() });
  }

  function handleCollapseAll() {
    const allCats = new Set(ts.payloads.map(p => p.category || 'Uncategorised'));
    upd({ collapsedCategories: allCats });
  }

  // ── Event handlers ────────────────────────────────────────────────────────
  function handleCopyPayload() {
    navigator.clipboard.writeText(getEffectivePayloadStr()).then(() => {
      upd({ copied: true });
      setTimeout(() => upd({ copied: false }), 1800);
    });
  }

  function handleEditPayload() {
    const editedPayload = ts.editedPayload === null
      ? getPrettyJson(ts.payloads[ts.selectedIndex])
      : ts.editedPayload;
    upd({ isEditingPayload: true, editedPayload });
  }

  function handleResetPayload() {
    upd({ editedPayload: null, isEditingPayload: false, payloadParseError: '' });
  }

  function handlePayloadChange(val) {
    let payloadParseError = '';
    try { JSON.parse(val); }
    catch (err) { payloadParseError = err.message; }
    upd({ editedPayload: val, payloadParseError });
  }

  function handleCopyResponse() {
    const displayed = ts.viewingEntry || ts.runResult;
    if (!displayed) return;
    const text = displayed.error
      ? displayed.error
      : typeof displayed.body === 'object'
        ? JSON.stringify(displayed.body, null, 2)
        : String(displayed.body ?? '');
    navigator.clipboard.writeText(text).then(() => {
      upd({ responseCopied: true });
      setTimeout(() => upd({ responseCopied: false }), 1800);
    });
  }

  function handleSelectPayload(i) {
    // Auto-expand the selected item's category so it is always visible in the list.
    const cat  = ts.payloads[i]?.category || 'Uncategorised';
    const next = new Set(ts.collapsedCategories);
    next.delete(cat);
    upd({
      selectedIndex: i,
      collapsedCategories: next,
      viewingEntry: null,
      runResult: null,
      editedPayload: null,
      isEditingPayload: false,
      payloadParseError: '',
    });
  }

  // When a URL is selected from the dropdown, auto-set auth type from that
  // URL's configured authType (user can still override before running).
  function handleSelectUrl(value) {
    if (value === 'custom') {
      // Reset to safe defaults for custom URLs — user explicitly controls TLS toggle
      upd({ urlMode: 'custom', skipTlsVerify: false });
    } else {
      const idx   = parseInt(value.slice(2), 10);
      const entry = activeUrls[idx];
      upd({
        urlMode:       'preset',
        selectedUrlIdx: idx,
        // Auto-set both authType and skipTlsVerify from the URL's configured values
        ...(entry?.authType       !== undefined ? { authType:      entry.authType      } : {}),
        ...(entry?.skipTlsVerify  !== undefined ? { skipTlsVerify: entry.skipTlsVerify } : {}),
      });
    }
  }

  // ── Run handler ───────────────────────────────────────────────────────────
  // All state updates after the first await use setTabStates with a captured
  // envIdSnapshot so results always land in the correct tab even if the user
  // switches tabs while the request is in-flight.
  async function handleRun() {
    const url = getActiveUrl();
    if (!url || ts.runState === 'running') return;

    // Snapshot everything needed before the first await
    const envIdSnapshot    = activeEnvId;
    const authType         = ts.authType;
    const skipTlsSnapshot  = ts.skipTlsVerify || false;
    const payload       = ts.payloads[ts.selectedIndex];
    const rawStr        = ts.editedPayload !== null ? ts.editedPayload : payload.payload;
    const requestedAt   = new Date().toLocaleTimeString();
    const payloadName   = payload.name;

    // Synchronous — upd is fine here
    upd({ viewingEntry: null, runResult: null, runState: 'running' });

    let payloadObj;
    try { payloadObj = JSON.parse(rawStr); }
    catch { payloadObj = rawStr; }

    // Async-safe updater: always writes to envIdSnapshot, not current active tab
    const updSnap = (patch) =>
      setTabStates(prev => ({
        ...prev,
        [envIdSnapshot]: { ...(prev[envIdSnapshot] ?? {}), ...patch },
      }));

    const addHistorySnap = (entry) =>
      setTabStates(prev => {
        const current = prev[envIdSnapshot] ?? {};
        const newHistory = [
          { id: Date.now(), ...truncateHistoryEntry(entry) },
          ...(current.history ?? []),
        ].slice(0, MAX_HISTORY);
        return {
          ...prev,
          [envIdSnapshot]: { ...current, history: newHistory, historyOpen: true },
        };
      });

    try {
      const resp = await fetch(`${import.meta.env.BASE_URL}run-payload`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          payload:       payloadObj,
          url,
          authType,
          envId:         tabMode ? envIdSnapshot : null,
          skipTlsVerify: skipTlsSnapshot,
        }),
      });
      const data = await resp.json();
      const result = resp.ok
        ? data
        : { error: data.error, code: data.code, durationMs: data.durationMs };

      updSnap({ runResult: result, runState: 'idle' });
      addHistorySnap({ payloadName, url, requestedAt, ...result });
    } catch (err) {
      const result = { error: err.message, code: 'NETWORK' };
      updSnap({ runResult: result, runState: 'idle' });
      addHistorySnap({ payloadName, url, requestedAt, ...result });
    }
  }

  function handleClearHistory() {
    upd({ history: [], viewingEntry: null, historyOpen: false });
  }

  function handleTabSwitch(idx) {
    if (idx === activeEnvIdx) return;
    setActiveEnvIdx(idx);
    setBatchDrillIdx(null);
    // Tab state is already in tabStates (initialized on config load); payload
    // loading is triggered lazily by the useEffect above via loadedTabsRef.
  }

  // ── Library YAML editor handlers ──────────────────────────────────────────

  function handleOpenEditor() {
    upd({
      editorOpen: true,
      editorYaml: ts.rawYaml,
      editorDirty: false,
      editorError: '',
      saveState: 'idle',
      saveError: '',
    });
  }

  function handleCloseEditor() {
    upd({ editorOpen: false, editorDirty: false, editorError: '' });
  }

  function handleEditorChange(val) {
    // Validate YAML structure on every keystroke
    let editorError = '';
    try {
      const parsed = jsYaml.load(val);
      if (!parsed?.payloads || !Array.isArray(parsed.payloads)) {
        editorError = 'Must contain a top-level "payloads:" array';
      }
    } catch (e) {
      editorError = e.message;
    }
    upd({ editorYaml: val, editorDirty: true, editorError, saveState: 'idle' });
  }

  async function handleSaveEditor() {
    if (ts.editorError || ts.saveState === 'saving') return;

    const envIdSnapshot = activeEnvId;
    const yamlToSave    = ts.editorYaml;

    const updSnap = (patch) =>
      setTabStates(prev => ({
        ...prev,
        [envIdSnapshot]: { ...(prev[envIdSnapshot] ?? {}), ...patch },
      }));

    updSnap({ saveState: 'saving', saveError: '' });

    try {
      const headers = { 'Content-Type': 'application/json' };
      if (writeAuthRequired && adminToken) {
        headers['Authorization'] = `Bearer ${adminToken}`;
      }
      const resp = await fetch(
        `${import.meta.env.BASE_URL}payload-content/${encodeURIComponent(envIdSnapshot)}`,
        { method: 'PUT', headers, body: JSON.stringify({ yaml: yamlToSave }) }
      );
      const data = await resp.json();
      if (!resp.ok) throw new Error(data.error || `HTTP ${resp.status}`);

      // Mark as saved and show loading state while we re-fetch the payload list.
      // We cannot rely on the payload useEffect re-running here because its deps
      // [activeEnvId, configLoaded] don't change on save — so we fetch inline.
      updSnap({ saveState: 'saved', editorDirty: false, rawYaml: yamlToSave, fetchStatus: 'loading', payloads: [] });

      // Re-fetch the updated payload list directly (async-safe — uses envIdSnapshot)
      try {
        const reloadResp = await fetch(
          `${import.meta.env.BASE_URL}payload-content/${encodeURIComponent(envIdSnapshot)}`
        );
        if (reloadResp.ok) {
          const reloadData = await reloadResp.json();
          const reloadAllCats = new Set(
            (reloadData.payloads || []).map(p => p.category || 'Uncategorised')
          );
          setTabStates(prev => ({
            ...prev,
            [envIdSnapshot]: {
              ...(prev[envIdSnapshot] ?? {}),
              fetchStatus: 'ready',
              payloads:    reloadData.payloads,
              rawYaml:     reloadData.yaml,
              encryptedSource: reloadData.encrypted,
              collapsedCategories: reloadAllCats,
              editorOpen:  false,
            },
          }));
        }
      } catch {
        // Re-fetch failed — payload list may be stale, but save succeeded;
        // user can switch tabs and back to force a fresh load.
        setTabStates(prev => ({
          ...prev,
          [envIdSnapshot]: { ...(prev[envIdSnapshot] ?? {}), fetchStatus: 'ready' },
        }));
      }
    } catch (err) {
      updSnap({ saveState: 'error', saveError: err.message });
    }
  }

  // ── Batch run handlers ────────────────────────────────────────────────────
  function toggleBatchMode() {
    setBatchMode(m => !m);
    upd({ batchChecked: new Set(), batchResults: [] });
    setBatchDrillIdx(null);
  }

  function toggleBatchCheck(idx, e) {
    e.stopPropagation();
    const next = new Set(ts.batchChecked);
    next.has(idx) ? next.delete(idx) : next.add(idx);
    upd({ batchChecked: next });
  }

  function batchSelectAll() {
    upd({ batchChecked: new Set(getFilteredPayloads().map(p => p.originalIndex)) });
  }

  function batchClearAll() {
    upd({ batchChecked: new Set() });
  }

  async function handleBatchRun() {
    const url = getActiveUrl();
    if (batchRunning || ts.batchChecked.size === 0 || !url) return;

    const indices      = [...ts.batchChecked];
    const CONCURRENCY  = 5;
    const envIdSnap    = activeEnvId;
    const authTypeSnap = ts.authType;
    const skipTlsSnap  = ts.skipTlsVerify || false;

    setTabStates(prev => ({
      ...prev,
      [envIdSnap]: { ...(prev[envIdSnap] ?? {}), batchResults: indices.map(idx => ({
        index: idx,
        name: prev[envIdSnap]?.payloads[idx]?.name || `Payload ${idx + 1}`,
        status: 'pending',
      })) },
    }));
    setBatchRunning(true);
    setBatchDrillIdx(null);

    async function runOne(idx) {
      setTabStates(prev => ({ ...prev, [envIdSnap]: { ...(prev[envIdSnap] ?? {}),
        batchResults: (prev[envIdSnap]?.batchResults ?? []).map(r => r.index === idx ? { ...r, status: 'running' } : r),
      }}));
      const payload = ts.payloads[idx];
      let payloadObj;
      try { payloadObj = JSON.parse(payload.payload); } catch { payloadObj = payload.payload; }
      const requestedAt = new Date().toLocaleTimeString();
      try {
        const resp = await fetch(`${import.meta.env.BASE_URL}run-payload`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ payload: payloadObj, url, authType: authTypeSnap, envId: tabMode ? envIdSnap : null, skipTlsVerify: skipTlsSnap }),
        });
        const data = await resp.json();
        const ok   = resp.ok && !data.error;
        setTabStates(prev => {
          const cur = prev[envIdSnap] ?? {};
          const entry = truncateHistoryEntry(ok ? data : { error: data.error, code: data.code, durationMs: data.durationMs });
          return { ...prev, [envIdSnap]: { ...cur,
            batchResults: (cur.batchResults ?? []).map(r => r.index === idx ? {
              ...r, status: ok ? 'ok' : 'fail',
              httpStatus: data.status, durationMs: data.durationMs,
              body: data.body, error: data.error, code: data.code,
            } : r),
            historyOpen: true,
            history: [{ id: Date.now() + idx, payloadName: payload.name, url, requestedAt, ...entry }, ...(cur.history ?? [])].slice(0, MAX_HISTORY),
          }};
        });
      } catch (err) {
        setTabStates(prev => ({ ...prev, [envIdSnap]: { ...(prev[envIdSnap] ?? {}),
          batchResults: (prev[envIdSnap]?.batchResults ?? []).map(r => r.index === idx ? { ...r, status: 'fail', error: err.message, code: 'NETWORK' } : r),
        }}));
      }
    }

    let i = 0;
    async function worker() {
      while (i < indices.length) { await runOne(indices[i++]); }
    }
    await Promise.all(Array.from({ length: Math.min(CONCURRENCY, indices.length) }, worker));
    setBatchRunning(false);
  }

  function handleBatchRetryFailed() {
    upd({
      batchChecked: new Set(ts.batchResults.filter(r => r.status === 'fail').map(r => r.index)),
      batchResults: [],
    });
  }

  function handleBatchDrilldown(idx) {
    setBatchDrillIdx(idx);
    handleSelectPayload(idx);
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Tab strip — environment tabs only; no extra elements so overflow-x scroll works
  const tabStrip = tabMode ? (
    <div style={s.tabStrip}>
      {environments.map((env, idx) => (
        <button
          key={env.id}
          onClick={() => handleTabSwitch(idx)}
          style={{ ...s.tab, ...(idx === activeEnvIdx ? s.tabActive : {}) }}
        >
          {env.label}
        </button>
      ))}
    </div>
  ) : null;

  // ── Loading / error early returns ─────────────────────────────────────────
  if (ts.fetchStatus === 'idle' || ts.fetchStatus === 'loading') {
    return (
      <div style={s.page}>
        <div style={s.container}>
          <header style={s.header}>
            <h1 style={s.title}>Payload Library</h1>
          </header>
          {tabStrip}
          <div style={s.screen}>
            <div style={s.spinner} />
            <p style={s.mutedText}>Loading payloads…</p>
          </div>
        </div>
      </div>
    );
  }

  if (ts.fetchStatus === 'error') {
    return (
      <div style={s.page}>
        <div style={s.container}>
          <header style={s.header}>
            <h1 style={s.title}>Payload Library</h1>
          </header>
          {tabStrip}
          <div style={s.screen}>
            <p style={s.errorText}>{ts.errorMsg}</p>
          </div>
        </div>
      </div>
    );
  }

  // ── Main render ───────────────────────────────────────────────────────────
  const selected        = ts.payloads[ts.selectedIndex];
  const activeUrl       = getActiveUrl();
  const payloadIsValid  = ts.editedPayload === null || ts.payloadParseError === '';
  const canRun          = ts.runState === 'idle' && activeUrl.length > 0 && payloadIsValid;
  const isModified      = ts.editedPayload !== null;
  const displayedResult = ts.viewingEntry || ts.runResult;
  const isSearching     = ts.searchQuery.trim().length > 0;
  const filteredPayloads = getFilteredPayloads();
  const grouped         = isSearching ? null : getGrouped(filteredPayloads);

  return (
    <div style={s.page}>
      <div style={s.container}>

        <header style={s.header}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '0.5rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem', flexWrap: 'wrap' }}>
              <h1 style={s.title}>Payload Library</h1>
              {ts.encryptedSource === true && (
                <span style={s.encryptedBadge} title="Payloads served from AES-256-GCM encrypted storage">
                  🔒 Encrypted
                </span>
              )}
              {ts.encryptedSource === false && (
                <span style={s.plaintextBadge} title="Payloads served from plain-text file — set PAYLOAD_ENCRYPTION_KEY to encrypt">
                  ⚠ Plain text
                </span>
              )}
            </div>
            {/* Edit Library button — always visible in the header, never in the scrollable tab strip */}
            {tabMode && (
              <button
                onClick={ts.editorOpen ? handleCloseEditor : handleOpenEditor}
                style={{ ...s.editLibraryBtn, ...(ts.editorOpen ? s.editLibraryBtnActive : {}) }}
                title={ts.editorOpen ? 'Close YAML editor' : 'Edit this environment\'s payload library'}
              >
                {ts.editorOpen ? '✕ Close Editor' : '✎ Edit Library'}
              </button>
            )}
          </div>
          <p style={s.subtitle}>
            {ts.payloads.length} sample API payloads — select, inspect, and run against a provider.
          </p>
        </header>

        {/* ── Tab strip (tab mode only — always shown, even with 1 tab) ──── */}
        {tabStrip}

        {/* ── YAML Library Editor ───────────────────────────────────────── */}
        {tabMode && ts.editorOpen && (
          <div style={s.editorCard}>
            <div style={s.editorHeader}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
                <span style={s.sectionLabel}>YAML Editor — {activeEnv?.label}</span>
                {ts.encryptedSource === true && (
                  <span style={s.encryptedBadge}>🔒 Encrypted</span>
                )}
                {ts.editorDirty && (
                  <span style={s.modifiedBadge}>Unsaved changes</span>
                )}
              </div>
              <div style={s.btnRow}>
                {/* Admin token input — only when write auth is required */}
                {writeAuthRequired && (
                  <input
                    type="password"
                    placeholder="Admin token"
                    value={adminToken}
                    onChange={(e) => setAdminToken(e.target.value)}
                    style={s.adminTokenInput}
                    title="Enter the PAYLOAD_ADMIN_TOKEN value to authorise saves"
                  />
                )}
                <button
                  onClick={handleCloseEditor}
                  style={s.outlineBtn}
                  disabled={ts.saveState === 'saving'}
                >
                  Discard
                </button>
                <button
                  onClick={handleSaveEditor}
                  disabled={
                    !!ts.editorError ||
                    !ts.editorDirty ||
                    ts.saveState === 'saving' ||
                    (writeAuthRequired && !adminToken)
                  }
                  style={{
                    ...s.runBtn,
                    ...(ts.saveState === 'saved' ? s.saveOkBtn : {}),
                    ...((!!ts.editorError || !ts.editorDirty || ts.saveState === 'saving' ||
                      (writeAuthRequired && !adminToken)) ? s.runBtnDisabled : {}),
                  }}
                  title={
                    writeAuthRequired && !adminToken
                      ? 'Enter the admin token to save'
                      : ts.editorError
                        ? 'Fix YAML errors before saving'
                        : !ts.editorDirty
                          ? 'No changes to save'
                          : ''
                  }
                >
                  {ts.saveState === 'saving'
                    ? <><span style={s.btnSpinner} />Saving…</>
                    : ts.saveState === 'saved'
                      ? '✓ Saved'
                      : '💾 Save'}
                </button>
              </div>
            </div>

            {/* YAML textarea */}
            <textarea
              value={ts.editorYaml}
              onChange={(e) => handleEditorChange(e.target.value)}
              style={s.yamlTextarea}
              spellCheck={false}
              autoFocus
              placeholder={`payloads:\n  - name: "My Payload"\n    category: "General"\n    payload: |\n      {\n        "entityId": "...",\n        "clientId": "..."\n      }`}
            />

            {/* Validation error */}
            {ts.editorError && (
              <p style={s.parseError}>⚠ YAML error: {ts.editorError}</p>
            )}

            {/* Save error */}
            {ts.saveState === 'error' && ts.saveError && (
              <p style={s.saveErrorMsg}>✕ Save failed: {ts.saveError}</p>
            )}

            {/* Instructions for write auth */}
            {writeAuthRequired && (
              <p style={s.editorHint}>
                Write access is protected. Enter the <code>PAYLOAD_ADMIN_TOKEN</code> value above to enable saving.
              </p>
            )}
          </div>
        )}

        <div style={{ ...s.columns, gridTemplateColumns: listCollapsed ? '36px minmax(0,1fr)' : '260px minmax(0,1fr)' }}>

          {/* ── Left: search + payload list (or collapsed strip) ────────── */}
          {listCollapsed ? (
            <div style={s.listColCollapsed}>
              <button
                onClick={() => setListCollapsed(false)}
                style={s.panelToggleBtn}
                title="Expand payload list"
              >⟩</button>
            </div>
          ) : (
          <div style={s.listCol}>

            {/* Search bar */}
            <div style={s.searchBar}>
              <span style={s.searchIcon}>⌕</span>
              <input
                ref={searchInputRef}
                type="text"
                placeholder="Search… (⌘K)"
                value={ts.searchQuery}
                onChange={(e) => upd({ searchQuery: e.target.value })}
                onKeyDown={(e) => {
                  if (e.key === 'Escape') { upd({ searchQuery: '' }); e.target.blur(); }
                }}
                style={s.searchInput}
                spellCheck={false}
              />
              {ts.searchQuery && (
                <button
                  onClick={() => upd({ searchQuery: '' })}
                  style={s.searchClear}
                  title="Clear"
                >×</button>
              )}
              <button
                onClick={() => setListCollapsed(true)}
                style={s.panelToggleBtn}
                title="Collapse panel"
              >⟨</button>
            </div>

            {isSearching && (
              <p style={s.searchCount}>
                {filteredPayloads.length} of {ts.payloads.length}
              </p>
            )}

            {/* Expand All / Collapse All / Batch toggle — only shown in grouped mode */}
            {!isSearching && ts.payloads.length > 0 && (
              <div style={s.collapseToolbar}>
                <button onClick={handleExpandAll} style={s.collapseToolbarBtn}>
                  Expand all
                </button>
                <span style={s.collapseToolbarDivider}>·</span>
                <button onClick={handleCollapseAll} style={s.collapseToolbarBtn}>
                  Collapse all
                </button>
                <span style={s.collapseToolbarDivider}>·</span>
                <button
                  onClick={toggleBatchMode}
                  style={{ ...s.collapseToolbarBtn, ...(batchMode ? s.collapseToolbarBtnActive : {}) }}
                  title={batchMode ? 'Exit batch mode' : 'Select multiple payloads and run together'}
                >
                  {batchMode ? '✕ Batch off' : '⊞ Batch'}
                </button>
              </div>
            )}

            {/* Batch selection toolbar — only when batch mode is on */}
            {batchMode && (
              <div style={s.batchToolbar}>
                <span style={s.batchCount}>{ts.batchChecked.size} selected</span>
                <button onClick={batchSelectAll} style={s.collapseToolbarBtn}>All</button>
                <span style={s.collapseToolbarDivider}>·</span>
                <button onClick={batchClearAll} style={s.collapseToolbarBtn}>Clear</button>
                <button
                  onClick={handleBatchRun}
                  disabled={ts.batchChecked.size === 0 || batchRunning || !activeUrl}
                  style={{
                    ...s.batchRunBtn,
                    ...(ts.batchChecked.size === 0 || batchRunning || !activeUrl ? s.runBtnDisabled : {}),
                  }}
                  title={!activeUrl ? 'Set a provider URL above to run' : ''}
                >
                  {batchRunning
                    ? <><span style={s.btnSpinner} />Running…</>
                    : `▶ Run${ts.batchChecked.size > 0 ? ` (${ts.batchChecked.size})` : ''}`
                  }
                </button>
              </div>
            )}

            <div style={s.list}>
              {/* Flat list while searching */}
              {isSearching && filteredPayloads.map((p) => (
                <button
                  key={p.originalIndex}
                  onClick={() => handleSelectPayload(p.originalIndex)}
                  style={{
                    ...s.listItem,
                    ...(p.originalIndex === ts.selectedIndex ? s.listItemActive : {}),
                    ...(batchMode && ts.batchChecked.has(p.originalIndex) ? s.listItemBatchChecked : {}),
                  }}
                >
                  {batchMode && (
                    <input
                      type="checkbox"
                      checked={ts.batchChecked.has(p.originalIndex)}
                      onChange={(e) => toggleBatchCheck(p.originalIndex, e)}
                      onClick={(e) => e.stopPropagation()}
                      style={s.batchCheckbox}
                    />
                  )}
                  <span style={s.listIndex}>{String(p.originalIndex + 1).padStart(2, '0')}</span>
                  <div style={s.listItemText}>
                    <span style={s.listName}>{p.name}</span>
                    {p.description && <span style={s.listDescription}>{p.description}</span>}
                  </div>
                  {p.category && <span style={s.listCatPill}>{p.category}</span>}
                  {p.originalIndex === ts.selectedIndex && <span style={s.listArrow}>›</span>}
                </button>
              ))}

              {isSearching && filteredPayloads.length === 0 && (
                <p style={s.searchEmpty}>No payloads match &ldquo;{ts.searchQuery}&rdquo;</p>
              )}

              {/* Grouped list when not searching */}
              {!isSearching && grouped && Object.entries(grouped).map(([cat, items]) => {
                const isCollapsed   = ts.collapsedCategories.has(cat);
                const hasSelected   = items.some(p => p.originalIndex === ts.selectedIndex);
                return (
                <div key={cat}>
                  <button
                    onClick={() => toggleCategory(cat)}
                    style={{ ...s.categoryHeader, ...(hasSelected && isCollapsed ? s.categoryHeaderHasSelected : {}) }}
                    title={isCollapsed ? `Expand ${cat}` : `Collapse ${cat}`}
                  >
                    <span style={s.categoryChevron}>
                      {isCollapsed ? '▸' : '▾'}
                    </span>
                    <span style={s.categoryName}>{cat}</span>
                    <span style={s.categoryCount}>{items.length}</span>
                    {/* Dot shown when category is collapsed and contains the selected item */}
                    {hasSelected && isCollapsed && (
                      <span style={s.selectedDot} title="Selected item is in this group" />
                    )}
                  </button>
                  {!isCollapsed && items.map((p) => (
                    <button
                      key={p.originalIndex}
                      onClick={() => handleSelectPayload(p.originalIndex)}
                      style={{
                        ...s.listItem,
                        ...s.listItemIndented,
                        ...(p.originalIndex === ts.selectedIndex ? s.listItemActive : {}),
                        ...(batchMode && ts.batchChecked.has(p.originalIndex) ? s.listItemBatchChecked : {}),
                      }}
                    >
                      {batchMode && (
                        <input
                          type="checkbox"
                          checked={ts.batchChecked.has(p.originalIndex)}
                          onChange={(e) => toggleBatchCheck(p.originalIndex, e)}
                          onClick={(e) => e.stopPropagation()}
                          style={s.batchCheckbox}
                        />
                      )}
                      <span style={s.listIndex}>{String(p.originalIndex + 1).padStart(2, '0')}</span>
                      <div style={s.listItemText}>
                        <span style={s.listName}>{p.name}</span>
                        {p.description && <span style={s.listDescription}>{p.description}</span>}
                      </div>
                      {p.originalIndex === ts.selectedIndex && <span style={s.listArrow}>›</span>}
                    </button>
                  ))}
                </div>
                );
              })}
            </div>
          </div>
          )} {/* end listCollapsed conditional */}

          {/* ── Right: stacked detail ────────────────────────────────────── */}
          <div style={s.detailCol}>

            {/* ── Batch drilldown back-link ─────────────────────────────── */}
            {batchMode && batchDrillIdx !== null && (
              <button
                onClick={() => setBatchDrillIdx(null)}
                style={s.batchBackBtn}
              >
                ← Back to batch results
              </button>
            )}

            {/* ── URL / Auth card ──────────────────────────────────────── */}
            <div style={s.card}>
              <div style={s.cardTopRow}>
                <span style={s.sectionLabel}>Provider URL</span>
                <select
                  value={ts.authType}
                  onChange={(e) => upd({ authType: e.target.value })}
                  style={s.authSelect}
                  title="Authentication mode for this run"
                >
                  <option value="none">No Auth</option>
                  <option value="api-key">API Key</option>
                  <option value="entraid-apigee">EntraID + APIGEE</option>
                </select>
              </div>

              <div style={s.urlRow}>
                {/* Preset dropdown — shown when this environment has configured URLs */}
                {activeUrls.length > 0 && (
                  <select
                    value={ts.urlMode === 'preset' ? `p:${ts.selectedUrlIdx}` : 'custom'}
                    onChange={(e) => handleSelectUrl(e.target.value)}
                    style={s.urlSelect}
                  >
                    {activeUrls.map((u, i) => (
                      <option key={i} value={`p:${i}`}>{u.label}</option>
                    ))}
                    <option value="custom">Custom URL…</option>
                  </select>
                )}

                {/* Custom URL input — shown when custom mode or no presets */}
                {(ts.urlMode === 'custom' || activeUrls.length === 0) && (
                  <input
                    type="url"
                    placeholder="https://provider.example.com/api/endpoint"
                    value={ts.customUrl}
                    onChange={(e) => upd({ customUrl: e.target.value })}
                    style={s.urlInput}
                    spellCheck={false}
                  />
                )}
              </div>

              {/* Full URL hint when preset is active */}
              {ts.urlMode === 'preset' && activeUrls[ts.selectedUrlIdx] && (
                <p style={s.urlHint}>
                  {activeUrls[ts.selectedUrlIdx].url}
                </p>
              )}

              {/* TLS toggle — always visible so operators can see and override TLS settings.
                  Preset URL selected: auto-set from PROVIDER_{ENV}_URL_TLS_VERIFY config
                  (user can still override before running, same as Auth Type dropdown).
                  Custom URL: defaults to verified; user manually enables skip if needed. */}
              <div style={s.tlsRow}>
                <label style={s.tlsLabel}>
                  <input
                    type="checkbox"
                    checked={ts.skipTlsVerify}
                    onChange={(e) => upd({ skipTlsVerify: e.target.checked })}
                    style={{ cursor: 'pointer' }}
                  />
                  <span style={{ ...s.tlsText, ...(ts.skipTlsVerify ? s.tlsTextActive : {}) }}>
                    {ts.skipTlsVerify
                      ? '⚠ Skip TLS verification (self-signed cert accepted)'
                      : 'TLS verification on'}
                  </span>
                </label>
              </div>
            </div>

            {/* ── Batch results card (shown in batch mode when not drilling down) ── */}
            {batchMode && batchDrillIdx === null && (
              <div style={s.card}>
                <div style={s.cardTopRow}>
                  <span style={s.sectionLabel}>
                    {batchRunning ? `Running batch… (${ts.batchResults.filter(r => r.status === 'ok' || r.status === 'fail').length}/${ts.batchResults.length})` : ts.batchResults.length > 0 ? 'Batch results' : 'Batch mode — select payloads and run'}
                  </span>
                  {!batchRunning && ts.batchResults.length > 0 && (
                    <div style={s.responseMetaRow}>
                      <span style={{ ...s.chip, background: '#f0fdf4', color: '#15803d' }}>
                        ✓ {ts.batchResults.filter(r => r.status === 'ok').length} passed
                      </span>
                      <span style={{ ...s.chip, background: '#fef2f2', color: '#b91c1c' }}>
                        ✕ {ts.batchResults.filter(r => r.status === 'fail').length} failed
                      </span>
                      {ts.batchResults.some(r => r.status === 'fail') && (
                        <button onClick={handleBatchRetryFailed} style={s.outlineBtn}>
                          Retry failed
                        </button>
                      )}
                    </div>
                  )}
                </div>

                {ts.batchResults.length === 0 ? (
                  <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--text-secondary)', fontSize: '0.85rem' }}>
                    Check payloads on the left, then click <strong>▶ Run</strong>
                  </div>
                ) : (
                  <div style={s.batchResultsList}>
                    {ts.batchResults.map((r) => {
                      const isRunning = r.status === 'running';
                      const isPending = r.status === 'pending';
                      const isFail    = r.status === 'fail';
                      const isOk      = r.status === 'ok';
                      return (
                        <div key={r.index} style={s.batchResultRow}>
                          <div style={s.batchResultHead}>
                            <span style={s.batchResultName}>{r.name}</span>
                            {isRunning && <span style={{ ...s.chip, background: '#fefce8', color: '#a16207' }}>running…</span>}
                            {isPending && <span style={{ ...s.chip }}>pending</span>}
                            {isOk      && <span style={{ ...s.chip, background: '#f0fdf4', color: '#15803d', fontWeight: 700 }}>✓ passed</span>}
                            {isFail    && <span style={{ ...s.chip, background: '#fef2f2', color: '#b91c1c', fontWeight: 700 }}>✕ failed</span>}
                            {r.durationMs != null && <span style={{ ...s.chip, fontSize: '0.68rem' }}>{r.durationMs} ms</span>}
                            {(isOk || isFail) && (
                              <button
                                onClick={() => handleBatchDrilldown(r.index)}
                                style={s.batchInspectBtn}
                                title="Inspect and re-run this payload"
                              >
                                Inspect →
                              </button>
                            )}
                          </div>
                          {(isOk || isFail) && (
                            <div style={s.batchResultSnippet}>
                              {r.error
                                ? <span style={{ color: '#b91c1c' }}>{r.error}</span>
                                : typeof r.body === 'object'
                                  ? JSON.stringify(r.body).slice(0, 120) + (JSON.stringify(r.body).length > 120 ? '…' : '')
                                  : String(r.body ?? '').slice(0, 120)
                              }
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            )}

            {/* ── Payload / Response / History cards (hidden in batch mode unless drilldown) ── */}
            {(!batchMode || batchDrillIdx !== null) && (<>
            <div style={s.card}>
              <div style={s.payloadHeaderRow}>
                <div>
                  <h2 style={s.detailTitle}>{selected.name}</h2>
                  <span style={s.detailBadge}>JSON Payload</span>
                  {isModified && <span style={s.modifiedBadge}>Modified</span>}
                </div>
                <div style={s.btnRow}>
                  <button
                    onClick={handleCopyPayload}
                    style={{ ...s.outlineBtn, ...(ts.copied ? s.outlineBtnOk : {}) }}
                  >
                    {ts.copied ? '✓ Copied' : 'Copy'}
                  </button>
                  {!ts.isEditingPayload ? (
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
                      ts.payloadParseError ? 'Fix JSON errors before running' : ''
                    }
                  >
                    {ts.runState === 'running'
                      ? <><span style={s.btnSpinner} />{'Running…'}</>
                      : '▶ Run'
                    }
                  </button>
                </div>
              </div>

              {ts.isEditingPayload ? (
                <>
                  <textarea
                    value={getEffectivePayloadStr()}
                    onChange={(e) => handlePayloadChange(e.target.value)}
                    style={s.editTextarea}
                    spellCheck={false}
                    autoFocus
                  />
                  {ts.payloadParseError && (
                    <p style={s.parseError}>⚠ Invalid JSON: {ts.payloadParseError}</p>
                  )}
                </>
              ) : (
                <pre style={s.pre}>{getEffectivePayloadStr()}</pre>
              )}
            </div>

            {/* ── Response card ────────────────────────────────────────── */}
            {(ts.runState === 'running' || displayedResult) && (
              <div style={s.card} ref={responseCardRef}>
                <div style={s.cardTopRow}>
                  <span style={s.sectionLabel}>
                    {ts.viewingEntry
                      ? `History · ${ts.viewingEntry.payloadName} · ${ts.viewingEntry.requestedAt}`
                      : 'Provider Response'}
                  </span>
                  <div style={s.responseMetaRow}>
                    {ts.viewingEntry && (
                      <button onClick={() => upd({ viewingEntry: null })} style={s.ghostBtn}>
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
                          style={{ ...s.outlineBtn, ...(ts.responseCopied ? s.outlineBtnOk : {}) }}
                        >
                          {ts.responseCopied ? '✓' : 'Copy'}
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

                {ts.runState === 'running' && !displayedResult ? (
                  <div style={s.responseLoading}>
                    <div style={s.spinner} />
                    <p style={s.mutedText}>Calling provider…</p>
                  </div>
                ) : displayedResult?.error ? (
                  <div style={s.responseErrorBox}>
                    <pre style={s.responseErrorText}>{displayedResult.error}</pre>
                    <p style={{ ...s.mutedText, marginTop: '0.5rem' }}>
                      {displayedResult.code === 'TIMEOUT' ? 'Timeout' : 'Network error'}
                      {displayedResult.durationMs != null && ` · ${displayedResult.durationMs} ms`}
                      {(displayedResult.url || activeUrl) && ` · ${displayedResult.url || activeUrl}`}
                    </p>
                    <p style={{ ...s.mutedText, marginTop: '0.5rem' }}>
                      {errorHint(displayedResult.code, displayedResult.error)}
                    </p>
                  </div>
                ) : (
                  <pre style={s.pre}>
                    {(() => {
                      const body = displayedResult.body;
                      if (body === '' || body == null) {
                        return `HTTP ${displayedResult.status} — (empty response body)`;
                      }
                      return typeof body === 'object'
                        ? JSON.stringify(body, null, 2)
                        : String(body);
                    })()}
                  </pre>
                )}
              </div>
            )}

            {/* ── History card ─────────────────────────────────────────── */}
            {ts.history.length > 0 && (
              <div style={s.card}>
                <div style={s.cardTopRow}>
                  <button
                    onClick={() => upd({ historyOpen: !ts.historyOpen })}
                    style={s.ghostBtn}
                  >
                    <span style={s.sectionLabel}>
                      {ts.historyOpen ? '▾' : '▸'} Run History ({ts.history.length})
                    </span>
                  </button>
                  <button onClick={handleClearHistory} style={s.ghostBtn}>
                    Clear all
                  </button>
                </div>

                {ts.historyOpen && (
                  <div style={s.historyList}>
                    {ts.history.map((entry) => (
                      <button
                        key={entry.id}
                        onClick={() => upd({ viewingEntry: entry })}
                        style={{
                          ...s.historyItem,
                          ...(ts.viewingEntry?.id === entry.id ? s.historyItemActive : {}),
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

            </>)} {/* end !batchMode || batchDrillIdx !== null */}

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
    marginBottom: '1rem',
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

  // ── Tab strip ─────────────────────────────────────────────────────────────
  // Horizontally scrollable; white-space: nowrap keeps all tabs on one line.
  tabStrip: {
    display: 'flex',
    flexDirection: 'row',
    gap: '2px',
    overflowX: 'auto',
    whiteSpace: 'nowrap',
    borderBottom: '2px solid var(--border)',
    marginBottom: '1.25rem',
    paddingBottom: '0',
    // Hide scrollbar on WebKit while keeping scroll functionality
    scrollbarWidth: 'thin',
    scrollbarColor: 'var(--border) transparent',
  },
  tab: {
    display: 'inline-block',
    padding: '0.45rem 1.1rem',
    border: 'none',
    borderBottomWidth: '2px',
    borderBottomStyle: 'solid',
    borderBottomColor: 'transparent',
    marginBottom: '-2px',   // sits on top of the strip's border-bottom
    background: 'none',
    color: 'var(--text-secondary)',
    fontFamily: 'inherit',
    fontSize: '0.82rem',
    fontWeight: 600,
    letterSpacing: '0.04em',
    cursor: 'pointer',
    borderRadius: '6px 6px 0 0',
    transition: 'color 0.15s, border-color 0.15s',
    flexShrink: 0,
    whiteSpace: 'nowrap',
  },
  tabActive: {
    color: 'var(--accent)',
    borderBottomColor: 'var(--accent)',
    background: 'var(--accent-light)',
  },

  columns: {
    display: 'grid',
    gridTemplateColumns: '260px minmax(0, 1fr)',
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
    borderTop: '3px solid var(--accent)',
    borderRadius: 'var(--radius)',
    boxShadow: 'var(--shadow)',
    overflow: 'hidden',
  },
  // ── Collapsed strip (shows only the expand button) ────────────────────────
  listColCollapsed: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    paddingTop: '0.5rem',
    background: 'var(--surface)',
    border: '1px solid var(--border)',
    borderTop: '3px solid var(--accent)',
    borderRadius: 'var(--radius)',
    boxShadow: 'var(--shadow)',
    minHeight: '3rem',
  },
  // ── Panel collapse / expand toggle button ─────────────────────────────────
  panelToggleBtn: {
    background: 'none',
    border: 'none',
    cursor: 'pointer',
    color: 'var(--text-secondary)',
    fontSize: '0.8rem',
    fontFamily: 'inherit',
    padding: '0.2rem 0.3rem',
    borderRadius: '4px',
    lineHeight: 1,
    flexShrink: 0,
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
  // ── Expand All / Collapse All toolbar ────────────────────────────────────
  collapseToolbar: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.25rem',
    padding: '0.25rem 0.625rem',
    borderBottom: '1px solid var(--border)',
    background: 'var(--bg)',
  },
  collapseToolbarBtn: {
    background: 'none',
    border: 'none',
    cursor: 'pointer',
    fontSize: '0.68rem',
    fontWeight: 600,
    color: 'var(--accent)',
    fontFamily: 'inherit',
    padding: '0.1rem 0.2rem',
    borderRadius: '4px',
    lineHeight: 1,
  },
  collapseToolbarDivider: {
    color: 'var(--text-secondary)',
    fontSize: '0.68rem',
    userSelect: 'none',
  },

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
  // Applied to the header when it's collapsed AND contains the selected item
  categoryHeaderHasSelected: {
    background: 'var(--accent-light)',
  },
  // Small accent dot shown on a collapsed header that contains the selected item
  selectedDot: {
    width: '6px',
    height: '6px',
    borderRadius: '50%',
    background: 'var(--accent)',
    flexShrink: 0,
    marginLeft: 'auto',
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
    minWidth: 0,          // prevents flex/grid cross-axis from expanding to content width
  },

  // ── Card shell ────────────────────────────────────────────────────────────
  card: {
    background: 'var(--surface)',
    border: '1px solid var(--border)',
    borderTop: '3px solid var(--accent)',   // accent strip — adapts to theme/colour
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
    margin: '0 1rem 0.5rem',
    fontSize: '0.72rem',
    color: 'var(--text-secondary)',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    wordBreak: 'break-all',
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
    flexWrap: 'wrap',
  },
  tlsSkipBadge: {
    fontSize: '0.65rem',
    fontWeight: 700,
    color: '#b45309',
    background: '#fffbeb',
    border: '1px solid #fde68a',
    borderRadius: '4px',
    padding: '1px 6px',
    whiteSpace: 'nowrap',
    fontFamily: 'inherit',
  },
  // ── TLS toggle row (custom URL mode only) ────────────────────────────────
  tlsRow: {
    margin: '0 1rem 0.75rem',
  },
  tlsLabel: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.4rem',
    cursor: 'pointer',
  },
  tlsText: {
    fontSize: '0.72rem',
    color: 'var(--text-secondary)',
    fontFamily: 'inherit',
  },
  tlsTextActive: {
    color: '#b45309',
    fontWeight: 600,
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
    whiteSpace: 'pre-wrap',   // wraps long lines; preserves JSON indentation
    wordBreak: 'break-all',   // breaks unspaced tokens (long URLs, base64, etc.)
    overflowWrap: 'anywhere',
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
    overflow: 'hidden',   // gives the box a definite width for the pre's wrap boundary
  },
  responseErrorText: {
    margin: 0,
    padding: 0,
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    fontSize: '0.78rem',
    lineHeight: 1.6,
    color: '#b91c1c',
    whiteSpace: 'pre-wrap',
    wordBreak: 'break-word',
    overflowWrap: 'anywhere', // force-breaks unspaced tokens (URLs, env var names, etc.)
    background: 'transparent',
    border: 'none',
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
    minHeight: '40vh',
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

  // ── Encryption badges ─────────────────────────────────────────────────────
  encryptedBadge: {
    display: 'inline-flex',
    alignItems: 'center',
    fontSize: '0.72rem',
    fontWeight: 700,
    color: '#15803d',
    background: '#f0fdf4',
    border: '1px solid #bbf7d0',
    borderRadius: '6px',
    padding: '2px 8px',
    letterSpacing: '0.02em',
  },
  plaintextBadge: {
    display: 'inline-flex',
    alignItems: 'center',
    fontSize: '0.72rem',
    fontWeight: 700,
    color: '#b45309',
    background: '#fffbeb',
    border: '1px solid #fde68a',
    borderRadius: '6px',
    padding: '2px 8px',
    letterSpacing: '0.02em',
  },

  // ── Edit Library header button ────────────────────────────────────────────
  editLibraryBtn: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: '0.3rem',
    padding: '0.35rem 0.9rem',
    borderRadius: '8px',
    border: '1.5px solid var(--border)',
    background: 'var(--bg)',
    color: 'var(--text-secondary)',
    fontFamily: 'inherit',
    fontSize: '0.8rem',
    fontWeight: 600,
    cursor: 'pointer',
    whiteSpace: 'nowrap',
    transition: 'all 0.15s',
  },
  editLibraryBtnActive: {
    background: 'var(--accent-light)',
    borderColor: 'var(--accent)',
    color: 'var(--accent)',
  },

  // ── YAML library editor card ──────────────────────────────────────────────
  editorCard: {
    background: 'var(--surface)',
    border: '1px solid var(--border)',
    borderRadius: 'var(--radius)',
    boxShadow: 'var(--shadow)',
    overflow: 'hidden',
    marginBottom: '1.25rem',
  },
  editorHeader: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '0.625rem 1rem',
    borderBottom: '1px solid var(--border)',
    gap: '0.75rem',
    flexWrap: 'wrap',
    background: 'var(--bg)',
  },
  yamlTextarea: {
    display: 'block',
    width: '100%',
    minHeight: '28rem',
    padding: '1rem',
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    fontSize: '0.8rem',
    lineHeight: 1.6,
    color: 'var(--text-primary)',
    background: 'var(--bg)',
    border: 'none',
    outline: 'none',
    resize: 'vertical',
    boxSizing: 'border-box',
    tabSize: 2,
  },
  saveOkBtn: {
    background: '#15803d',
    borderColor: '#15803d',
  },
  saveErrorMsg: {
    margin: 0,
    padding: '0.5rem 1rem',
    fontSize: '0.78rem',
    color: '#b91c1c',
    background: '#fef2f2',
    borderTop: '1px solid #fecaca',
  },
  editorHint: {
    margin: 0,
    padding: '0.5rem 1rem',
    fontSize: '0.72rem',
    color: 'var(--text-secondary)',
    background: 'var(--bg)',
    borderTop: '1px solid var(--border)',
  },
  adminTokenInput: {
    padding: '0.25rem 0.5rem',
    borderRadius: '6px',
    border: '1.5px solid var(--border)',
    background: 'var(--bg)',
    color: 'var(--text-primary)',
    fontFamily: 'inherit',
    fontSize: '0.78rem',
    width: '14rem',
    outline: 'none',
  },

  // ── Batch run styles ──────────────────────────────────────────────────────
  collapseToolbarBtnActive: {
    color: 'var(--accent)',
    background: 'var(--accent-light)',
    borderRadius: '4px',
  },
  batchToolbar: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.25rem',
    padding: '0.35rem 0.625rem',
    borderBottom: '1px solid var(--border)',
    background: 'var(--bg)',
  },
  batchCount: {
    fontSize: '0.68rem',
    fontWeight: 700,
    color: 'var(--accent)',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    minWidth: '5.5rem',
  },
  batchRunBtn: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.3rem',
    padding: '0.25rem 0.75rem',
    borderRadius: '6px',
    border: 'none',
    background: 'var(--accent)',
    color: '#fff',
    fontFamily: 'inherit',
    fontSize: '0.72rem',
    fontWeight: 700,
    cursor: 'pointer',
    marginLeft: 'auto',
  },
  batchCheckbox: {
    flexShrink: 0,
    cursor: 'pointer',
    accentColor: 'var(--accent)',
    width: 13,
    height: 13,
  },
  listItemBatchChecked: {
    background: 'var(--accent-light)',
    borderColor: 'var(--accent)',
  },
  batchBackBtn: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: '0.3rem',
    padding: '0.3rem 0.75rem',
    borderRadius: '8px',
    border: '1.5px solid var(--accent)',
    background: 'transparent',
    color: 'var(--accent)',
    fontFamily: 'inherit',
    fontSize: '0.78rem',
    fontWeight: 600,
    cursor: 'pointer',
  },
  batchResultsList: {
    display: 'flex',
    flexDirection: 'column',
    gap: '2px',
    padding: '0.5rem',
    maxHeight: '32rem',
    overflowY: 'auto',
  },
  batchResultRow: {
    border: '1px solid var(--border)',
    borderRadius: '8px',
    overflow: 'hidden',
  },
  batchResultHead: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
    padding: '0.45rem 0.75rem',
    background: 'var(--bg)',
    flexWrap: 'wrap',
  },
  batchResultName: {
    fontSize: '0.78rem',
    fontWeight: 600,
    color: 'var(--text-primary)',
    flex: 1,
    minWidth: 0,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  batchResultSnippet: {
    padding: '0.35rem 0.75rem',
    fontSize: '0.7rem',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    color: 'var(--text-secondary)',
    borderTop: '1px solid var(--border)',
    background: 'var(--surface)',
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
  },
  batchInspectBtn: {
    padding: '0.2rem 0.6rem',
    borderRadius: '6px',
    border: '1.5px solid var(--accent)',
    background: 'transparent',
    color: 'var(--accent)',
    fontFamily: 'inherit',
    fontSize: '0.7rem',
    fontWeight: 700,
    cursor: 'pointer',
    flexShrink: 0,
  },
};
