import { useState, useEffect } from 'react';

const BASE = import.meta.env.BASE_URL;

function approxTokens(text) { return Math.round((text || '').length / 4); }

function SectionHeader({ title, badge, open, onToggle, extra }) {
  return (
    <div style={s.sectionHeader} onClick={onToggle}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
        <span style={{ ...s.chevron, transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}>▶</span>
        <span style={s.sectionTitle}>{title}</span>
        {badge && <span style={s.badge}>{badge}</span>}
      </div>
      {extra}
    </div>
  );
}

// ── Client-side Rego cleanup ──────────────────────────────────────────────────
// Mirrors the server-side normaliseRego() in rangerService.js.
// Returns { cleaned, wasChanged } so UI can show a "cleaned" badge.
function clientCleanRego(raw) {
  if (!raw || !raw.trim()) return { cleaned: raw, wasChanged: false };
  const cleaned = raw
    .replace(/^```rego\s*/im, '')
    .replace(/^```\s*/im, '')
    .replace(/\s*```\s*$/im, '')
    .replace(/\r\n/g, '\n')
    .replace(/\t/g, '    ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
  return { cleaned, wasChanged: cleaned !== raw.trim() };
}

/**
 * RangerPolicyEditor — right panel of the Ranger Library.
 *
 * Props:
 *   policyEntry     { policyKey, name, serviceType, service } | null
 *   envId           string
 *   envConfig       { defaultOwner, defaultRepo, defaultBranch, defaultFilePath, defaultFetchMode }
 *   encryptionEnabled bool
 *   exampleRego     string — pre-built example to load
 *   onPolicySaved   (policyKey, meta) => void
 */
export default function RangerPolicyEditor({
  policyEntry,
  envId,
  envConfig = {},
  encryptionEnabled = false,
  exampleRego = '',
  showPrompt = true,
  onPolicySaved,
}) {
  // ── Source ────────────────────────────────────────────────────────────────
  const [sourceMode, setSourceMode] = useState('direct'); // 'direct'|'github'|'example'
  const [fetchMode, setFetchMode]   = useState(envConfig.defaultFetchMode || 'api');
  const [filePath, setFilePath]     = useState(envConfig.defaultFilePath || '');
  const [branch, setBranch]         = useState(envConfig.defaultBranch || 'main');
  const [regoCode, setRegoCode]     = useState('');
  const [originalRego, setOriginalRego] = useState(''); // GitHub-fetched baseline

  // ── Cleanup indicator ─────────────────────────────────────────────────────
  const [wasCleaned, setWasCleaned] = useState(false);

  // ── Generation ────────────────────────────────────────────────────────────
  // idle | fetching | cleaning | generating | ready | saving | saved | error
  const [status, setStatus]   = useState('idle');
  const [error, setError]     = useState(null);
  const [warning, setWarning] = useState(null);

  // ── Results ───────────────────────────────────────────────────────────────
  const [promptText, setPromptText]         = useState('');
  const [originalPrompt, setOriginalPrompt] = useState('');
  const [promptEdited, setPromptEdited]     = useState(false);
  const [rangerJson, setRangerJson]         = useState('');  // stringified for editor
  const [tokenUsage, setTokenUsage]         = useState(null);
  const [isMock, setIsMock]                 = useState(false);

  // ── Panels ────────────────────────────────────────────────────────────────
  const [promptOpen, setPromptOpen]   = useState(false);
  const [previewOpen, setPreviewOpen] = useState(false);

  // ── Save / copy ───────────────────────────────────────────────────────────
  const [saveError, setSaveError] = useState('');
  const [copied, setCopied]       = useState(false);

  // ── Reset when policyEntry changes ────────────────────────────────────────
  useEffect(() => {
    setError(null); setWarning(null); setSaveError('');
    setPromptText(''); setOriginalPrompt('');
    setPromptEdited(false); setTokenUsage(null); setIsMock(false);
    setPromptOpen(false); setPreviewOpen(false);
    setStatus('idle');
    setRegoCode(''); setOriginalRego(''); setWasCleaned(false);
    setSourceMode('direct');
    setFilePath(envConfig.defaultFilePath || '');
    setBranch(envConfig.defaultBranch || 'main');

    if (policyEntry?.policyKey && envId) loadSavedPolicy();
  }, [policyEntry?.policyKey]); // eslint-disable-line react-hooks/exhaustive-deps

  async function loadSavedPolicy() {
    try {
      const resp = await fetch(
        `${BASE}ranger-policy/${encodeURIComponent(envId)}/${encodeURIComponent(policyEntry.policyKey)}`
      );
      if (resp.ok) {
        const data = await resp.json();
        // policy may be an array (multi-policy) or a legacy single object
        const normalised = Array.isArray(data.policy) ? data.policy : [data.policy];
        setRangerJson(JSON.stringify(normalised, null, 2));
        setStatus('ready');
      } else {
        setRangerJson(''); setStatus('idle');
      }
    } catch {
      setRangerJson(''); setStatus('idle');
    }
  }

  // ── Generate ──────────────────────────────────────────────────────────────
  async function handleGenerate(useCustomPrompt = false) {
    setError(null); setWarning(null); setSaveError('');

    // Detect if GitHub-fetched Rego was edited
    const regoEdited = originalRego && regoCode !== originalRego;
    const effectiveMode = (sourceMode === 'github' && regoEdited) ? 'direct' : sourceMode;

    // Client-side cleanup before sending
    const { cleaned, wasChanged } = clientCleanRego(regoCode);
    setWasCleaned(wasChanged);

    setStatus(effectiveMode === 'github' ? 'fetching' : 'generating');

    let regoToSend = cleaned;

    // If GitHub mode and not edited — fetch from GitHub first
    if (effectiveMode === 'github') {
      try {
        const fetchResp = await fetch(`${BASE}ranger-fetch`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            owner:     envConfig.defaultOwner,
            repo:      envConfig.defaultRepo,
            branch:    branch || envConfig.defaultBranch,
            filePath,
            fetchMode,
          }),
        });
        const fetchData = await fetchResp.json();
        if (!fetchResp.ok) {
          setError(fetchData.error || 'GitHub fetch failed');
          setStatus('error');
          return;
        }
        regoToSend = fetchData.content;
        setRegoCode(regoToSend);
        setOriginalRego(regoToSend);
        if (fetchData.warning) setWarning(fetchData.warning);
        setStatus('generating');
      } catch {
        setError('Network error fetching from GitHub.');
        setStatus('error');
        return;
      }
    }

    const body = { regoCode: regoToSend, envId: envId || undefined };
    if (useCustomPrompt && promptEdited) body.customPrompt = promptText;

    try {
      const resp = await fetch(`${BASE}ranger-generate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await resp.json();
      if (!resp.ok) {
        setError(data.error || 'Generation failed');
        setStatus('error');
        return;
      }
      // rangerPolicies is an array; normalise legacy single-object responses
      const policies = Array.isArray(data.rangerPolicies)
        ? data.rangerPolicies
        : data.rangerPolicies ? [data.rangerPolicies] : [];
      setRangerJson(JSON.stringify(policies, null, 2));
      setPromptText(data.builtPrompt || '');
      setOriginalPrompt(data.builtPrompt || '');
      setPromptEdited(false);
      setTokenUsage(data.tokenUsage || null);
      setIsMock(data.mock || false);
      // Show the normalised Rego that was actually sent
      if (data.normalisedRego) setRegoCode(data.normalisedRego);
      setPromptOpen(true);   // auto-expand so the prompt is immediately visible
      setStatus('ready');
    } catch {
      setError('Network error — make sure the backend is running.');
      setStatus('error');
    }
  }

  // ── Save ──────────────────────────────────────────────────────────────────
  async function handleSave() {
    if (!rangerJson.trim() || !policyEntry?.policyKey || !envId) return;
    setSaveError(''); setStatus('saving');

    let parsed;
    try { parsed = JSON.parse(rangerJson); }
    catch { setSaveError('JSON is invalid — fix before saving.'); setStatus('ready'); return; }

    // Always save as array; normalise legacy single-object edits
    const policyArray = Array.isArray(parsed) ? parsed : [parsed];
    const firstPolicy = policyArray[0] || {};

    try {
      const resp = await fetch(
        `${BASE}ranger-policy/${encodeURIComponent(envId)}/${encodeURIComponent(policyEntry.policyKey)}`,
        {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            policy: policyArray,
            // Pass modal metadata so backend can preserve name/serviceType/service in the
            // manifest even when the generated Ranger JSON omits or renames those fields.
            name:        policyEntry.name        || policyEntry.policyKey,
            serviceType: policyEntry.serviceType || 'hive',
            service:     policyEntry.service     || '',
          }),
        }
      );
      const data = await resp.json();
      if (!resp.ok) { setSaveError(data.error || 'Save failed'); setStatus('ready'); return; }
      setStatus('saved');
      onPolicySaved?.(policyEntry.policyKey, {
        // Modal name takes priority — it is the user's chosen display name.
        // The Ranger JSON's name field is a service-level identifier, not a display name.
        name:        policyEntry.name        || firstPolicy.name        || policyEntry.policyKey,
        serviceType: policyEntry.serviceType || firstPolicy.serviceType || 'hive',
        service:     policyEntry.service     || firstPolicy.service     || '',
      });
    } catch {
      setSaveError('Network error during save');
      setStatus('ready');
    }
  }

  function handleCopy() {
    navigator.clipboard.writeText(rangerJson).then(() => {
      setCopied(true); setTimeout(() => setCopied(false), 2000);
    });
  }

  function handleDownload() {
    const name = policyEntry?.name || policyEntry?.policyKey || 'ranger-policy';
    // Apache Ranger's own Import feature (Access Manager → service → Import)
    // expects a top-level { "policies": [...] } envelope, not a bare array —
    // uploading a bare array (what we store/edit internally, and what the
    // programmatic POST /service/public/v2/api/policy/importPoliciesFromFile
    // body also wraps this same way per docs/RANGER_LIBRARY.md) fails with a
    // generic "Error parsing or processing the JSON file" 400 from Ranger.
    // Only the downloaded file gets wrapped — rangerJson itself (used for
    // Save/editing) stays a bare array, matching this app's own storage
    // format and PUT /ranger-policy's expected request body shape.
    let policies;
    try { policies = JSON.parse(rangerJson); } catch { policies = rangerJson; }
    if (!Array.isArray(policies)) policies = [policies];
    const envelope = JSON.stringify({ policies }, null, 2);
    const blob = new Blob([envelope], { type: 'application/json' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url; a.download = `${name}.json`; a.click();
    URL.revokeObjectURL(url);
  }

  function handleSourceMode(m) {
    setSourceMode(m);
    if (m === 'example') {
      setRegoCode(exampleRego);
      setWasCleaned(false);
    }
  }

  // ── Derived ───────────────────────────────────────────────────────────────
  const isRunning   = status === 'fetching' || status === 'generating';
  const hasResult   = !!rangerJson;
  const isSaving    = status === 'saving';
  const isSaved     = status === 'saved';
  const isDemoMode  = !policyEntry;

  // Count policies in current JSON output
  const policyCount = (() => {
    try { const p = JSON.parse(rangerJson); return Array.isArray(p) ? p.length : 1; } catch { return 0; }
  })();

  const canGenerate = sourceMode === 'github'
    ? !!(filePath.trim())
    : regoCode.trim().length > 0;

  const statusLabel = {
    fetching:   'Fetching from GitHub…',
    generating: 'Generating Ranger Policy…',
    saving:     'Saving…',
  }[status] || '';

  return (
    <div style={s.editor}>
      {/* ── Header ── */}
      <div style={s.editorHeader}>
        <div>
          {isDemoMode ? (
            <>
              <div style={s.breadcrumb}>Demo Mode</div>
              <div style={s.policyTitle}>Try "Load Example" to get started →</div>
            </>
          ) : (
            <>
              <div style={s.breadcrumb}>
                {policyEntry.serviceType && (
                  <span style={{ ...s.serviceChip, ...serviceTypeColor(policyEntry.serviceType) }}>
                    {policyEntry.serviceType.toUpperCase()}
                  </span>
                )}
                {policyEntry.service && ` · ${policyEntry.service}`}
              </div>
              <div style={s.policyTitle}>{policyEntry.name || policyEntry.policyKey}</div>
            </>
          )}
        </div>
        <div style={{ display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap' }}>
          {isMock && <span style={s.mockBadge}>⚠ No LLM key — mock output</span>}
        </div>
      </div>

      {/* ── Source config ── */}
      <div style={s.card}>
        <div style={s.cardHeader}>
          <span style={s.cardLabel}>REGO SOURCE</span>
          <select
            value={sourceMode}
            onChange={(e) => handleSourceMode(e.target.value)}
            style={s.modeSelect}
          >
            <option value="direct">✏️ Direct Input</option>
            <option value="github">🐙 GitHub</option>
            <option value="example">📋 Load Example</option>
          </select>
        </div>

        {/* GitHub path inputs */}
        {sourceMode === 'github' && (
          <div style={s.githubRow}>
            <input
              style={s.input}
              placeholder="File path (e.g. policies/my_policy.rego)"
              value={filePath}
              onChange={(e) => setFilePath(e.target.value)}
            />
            <input
              style={{ ...s.input, maxWidth: 160 }}
              placeholder={`Branch (default: ${envConfig.defaultBranch || 'main'})`}
              value={branch}
              onChange={(e) => setBranch(e.target.value)}
            />
            <div style={s.radioGroup}>
              {[['api', 'API'], ['raw', 'Raw']].map(([val, lbl]) => (
                <label key={val} style={s.radioLabel}>
                  <input
                    type="radio" value={val}
                    checked={fetchMode === val}
                    onChange={() => setFetchMode(val)}
                    style={{ marginRight: 3 }}
                  />
                  {lbl}
                </label>
              ))}
            </div>
          </div>
        )}

        {/* Rego textarea — shown for direct and example modes; also for github after fetch */}
        {(sourceMode === 'direct' || sourceMode === 'example' || (sourceMode === 'github' && regoCode)) && (
          <>
            <div style={{ display: 'flex', gap: 4, marginBottom: '0.4rem', justifyContent: 'space-between', alignItems: 'center' }}>
              <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
                {sourceMode === 'example' && (
                  <>
                    <button style={s.secondaryBtn} onClick={() => { setRegoCode(exampleRego); setWasCleaned(false); }}>
                      ↺ Reload
                    </button>
                    <button style={s.secondaryBtn} onClick={() => { setRegoCode(''); setWasCleaned(false); }}>
                      ✕ Clear
                    </button>
                  </>
                )}
                {sourceMode === 'direct' && (
                  <>
                    <button style={s.secondaryBtn} onClick={() => { setRegoCode(exampleRego); setWasCleaned(false); }} title="Load the built-in example Rego">
                      📋 Load Example
                    </button>
                    <button style={s.secondaryBtn} onClick={() => { setRegoCode(''); setWasCleaned(false); }}>
                      ✕ Clear
                    </button>
                  </>
                )}
              </div>
              {wasCleaned && (
                <span style={s.cleanedBadge} title="Markdown fences and extra whitespace were stripped before sending">
                  ✓ cleaned
                </span>
              )}
            </div>
            <textarea
              style={s.codeArea}
              rows={12}
              value={regoCode}
              onChange={(e) => { setRegoCode(e.target.value); setWasCleaned(false); }}
              placeholder="Paste OPA Rego code here (package declaration required)…"
              spellCheck={false}
            />
          </>
        )}

        {error  && <div style={s.errorBanner}>{error}</div>}
        {warning && <div style={s.warnBanner}>⚠ {warning}</div>}

        <button
          style={{
            ...s.generateBtn,
            opacity: (!canGenerate || isRunning) ? 0.55 : 1,
            cursor:  (!canGenerate || isRunning) ? 'not-allowed' : 'pointer',
          }}
          onClick={() => handleGenerate(false)}
          disabled={!canGenerate || isRunning}
        >
          {isRunning ? (
            <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={s.spinner} />{statusLabel}
            </span>
          ) : (
            sourceMode === 'github'
              ? '🐙 Fetch & Generate Ranger Policy'
              : '▶ Generate Ranger Policy'
          )}
        </button>
      </div>

      {/* ── Prompt panel — hidden when showPrompt=false (per-env config) ── */}
      {hasResult && showPrompt && (
        <div style={s.card}>
          <SectionHeader
            title="Prompt"
            badge={promptEdited ? 'edited' : `~${approxTokens(promptText)} tokens`}
            open={promptOpen}
            onToggle={() => setPromptOpen((o) => !o)}
            extra={
              promptOpen && (
                <div style={{ display: 'flex', gap: 4 }} onClick={(e) => e.stopPropagation()}>
                  {promptEdited && (
                    <button
                      style={s.tinyBtn}
                      onClick={() => { setPromptText(originalPrompt); setPromptEdited(false); }}
                    >
                      Reset
                    </button>
                  )}
                  <button
                    style={{ ...s.tinyBtn, ...s.tinyBtnAccent }}
                    onClick={() => handleGenerate(true)}
                    disabled={isRunning}
                  >
                    ↺ Regen with this prompt
                  </button>
                </div>
              )
            }
          />
          {promptOpen && (
            <textarea
              style={{ ...s.codeArea, minHeight: 220, marginTop: '0.6rem', fontSize: '0.75rem' }}
              value={promptText}
              onChange={(e) => { setPromptText(e.target.value); setPromptEdited(true); }}
              spellCheck={false}
            />
          )}
        </div>
      )}

      {/* ── Rego source preview (collapsible) ── */}
      {hasResult && regoCode && (
        <div style={s.card}>
          <SectionHeader
            title="Rego Input"
            badge={sourceMode}
            open={previewOpen}
            onToggle={() => setPreviewOpen((o) => !o)}
          />
          {previewOpen && <pre style={s.sourcePreview}>{regoCode}</pre>}
        </div>
      )}

      {/* ── Ranger JSON output ── */}
      {(hasResult || status === 'idle') && (
        <div style={s.card}>
          <div style={s.cardHeader}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={s.cardLabel}>RANGER POLICY (JSON)</span>
              {hasResult && policyCount > 0 && (
                <span style={s.countBadge} title="Number of Ranger policy objects in the array">
                  {policyCount} {policyCount === 1 ? 'policy' : 'policies'}
                </span>
              )}
            </div>
            <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
              <span style={{ ...s.badge, ...(encryptionEnabled ? s.badgeGreen : s.badgeWarn) }}>
                {encryptionEnabled ? '🔒 encrypted' : '⚠ plain text'}
              </span>
              {tokenUsage && (tokenUsage.promptTokens > 0 || tokenUsage.completionTokens > 0) && (
                <span style={s.metaText}>
                  {tokenUsage.promptTokens}↑ + {tokenUsage.completionTokens}↓ tokens
                </span>
              )}
            </div>
          </div>

          <textarea
            style={{ ...s.codeArea, minHeight: 300 }}
            value={rangerJson}
            onChange={(e) => { setRangerJson(e.target.value); if (status === 'saved') setStatus('ready'); }}
            placeholder={status === 'idle' ? 'Click "Generate Ranger Policy" to produce output…' : ''}
            spellCheck={false}
          />

          {hasResult && (
            <>
              <div style={s.toolbar}>
                <button style={s.toolBtn} onClick={handleCopy}>
                  {copied ? '✓ Copied' : '📋 Copy'}
                </button>
                <button style={s.toolBtn} onClick={handleDownload}>⬇ Download .json</button>
                <button style={s.toolBtn} onClick={() => handleGenerate(false)} disabled={isRunning}>
                  ↺ Regenerate
                </button>
              </div>

              <div style={s.saveRow}>
                <button
                  style={{
                    ...s.saveBtn,
                    opacity: (!rangerJson.trim() || isSaving || !policyEntry?.policyKey) ? 0.55 : 1,
                    cursor:  (!rangerJson.trim() || isSaving || !policyEntry?.policyKey) ? 'not-allowed' : 'pointer',
                    ...(isSaved ? { background: 'var(--success, #16a34a)' } : {}),
                  }}
                  onClick={handleSave}
                  disabled={!rangerJson.trim() || isSaving || !policyEntry?.policyKey}
                  title={!policyEntry?.policyKey ? 'Select a policy from the tree to enable save' : ''}
                >
                  {isSaving ? 'Saving…' : isSaved ? '✓ Saved' : 'Save 🔒'}
                </button>
                {!policyEntry?.policyKey && (
                  <span style={s.metaText}>Select a policy in the tree to save</span>
                )}
              </div>
              {saveError && <p style={s.errorText}>{saveError}</p>}
            </>
          )}
        </div>
      )}
    </div>
  );
}

// ── Service type badge colors ─────────────────────────────────────────────────
function serviceTypeColor(type) {
  const map = {
    hive:  { background: '#ede9fe', color: '#6d28d9' },
    hdfs:  { background: '#e0f2fe', color: '#0369a1' },
    hbase: { background: '#dcfce7', color: '#15803d' },
    tag:   { background: '#fef9c3', color: '#854d0e' },
  };
  return map[type?.toLowerCase()] || { background: 'var(--accent-light)', color: 'var(--accent)' };
}

// ── Styles ────────────────────────────────────────────────────────────────────
const s = {
  editor: {
    flex: 1, overflowY: 'auto',
    padding: '1.25rem',
    display: 'flex', flexDirection: 'column',
    gap: '0.875rem', minWidth: 0,
  },
  editorHeader: {
    display: 'flex', justifyContent: 'space-between',
    alignItems: 'flex-start', flexWrap: 'wrap', gap: '0.5rem',
  },
  breadcrumb:  { fontSize: '0.72rem', color: 'var(--text-secondary)', marginBottom: 2, display: 'flex', alignItems: 'center', gap: 4 },
  policyTitle: { fontSize: '1.2rem', fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.01em' },
  serviceChip: {
    fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.06em',
    padding: '2px 6px', borderRadius: 4,
  },
  mockBadge: {
    fontSize: '0.68rem', background: '#fff7ed', color: '#c2410c',
    border: '1px solid #fed7aa', borderRadius: 5, padding: '2px 6px', fontWeight: 600,
  },
  cleanedBadge: {
    fontSize: '0.68rem', background: '#f0fdf4', color: '#15803d',
    border: '1px solid #bbf7d0', borderRadius: 5, padding: '2px 6px', fontWeight: 600,
  },
  card: {
    background: 'var(--surface)', border: '1px solid var(--border)',
    borderRadius: 12, padding: '1rem',
  },
  cardHeader: {
    display: 'flex', alignItems: 'center',
    justifyContent: 'space-between', flexWrap: 'wrap', gap: '0.5rem',
    marginBottom: '0.6rem',
  },
  cardLabel: {
    fontSize: '0.72rem', fontWeight: 700, letterSpacing: '0.07em',
    textTransform: 'uppercase', color: 'var(--text-secondary)',
  },
  sectionHeader: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    cursor: 'pointer', userSelect: 'none', flexWrap: 'wrap', gap: '0.5rem',
  },
  sectionTitle: {
    fontSize: '0.72rem', fontWeight: 700, letterSpacing: '0.07em',
    textTransform: 'uppercase', color: 'var(--text-secondary)',
  },
  chevron: {
    fontSize: '0.5rem', color: 'var(--text-secondary)',
    transition: 'transform 0.18s ease', display: 'inline-block',
  },
  modeSelect: {
    marginLeft: 'auto', padding: '0.28rem 0.6rem', borderRadius: 7,
    border: '1.5px solid var(--border)', background: 'var(--bg)',
    color: 'var(--text-secondary)', fontSize: '0.78rem', fontWeight: 600,
    cursor: 'pointer', fontFamily: 'inherit', outline: 'none',
  },
  githubRow: { display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center', marginTop: '0.5rem' },
  radioGroup: { display: 'flex', gap: 8 },
  radioLabel:  { fontSize: '0.78rem', cursor: 'pointer', color: 'var(--text-primary)', display: 'flex', alignItems: 'center' },
  input: {
    flex: 1, minWidth: 120, padding: '0.4rem 0.65rem',
    borderRadius: 7, border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)',
    fontSize: '0.8rem', fontFamily: 'inherit', outline: 'none',
  },
  codeArea: {
    width: '100%', padding: '0.65rem',
    borderRadius: 7, border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)',
    fontSize: '0.75rem',
    fontFamily: '"SF Mono","Fira Code","Cascadia Code",monospace',
    resize: 'vertical', lineHeight: 1.55, outline: 'none', minHeight: 160,
    marginTop: '0.5rem', boxSizing: 'border-box',
  },
  sourcePreview: {
    marginTop: '0.5rem', padding: '0.65rem',
    borderRadius: 7, border: '1px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)',
    fontSize: '0.72rem',
    fontFamily: '"SF Mono","Fira Code",monospace',
    whiteSpace: 'pre-wrap', wordBreak: 'break-all',
    maxHeight: 260, overflowY: 'auto', lineHeight: 1.5,
  },
  generateBtn: {
    marginTop: '0.75rem', width: '100%', padding: '0.75rem',
    borderRadius: 9, border: 'none',
    background: 'linear-gradient(135deg, var(--accent) 0%, var(--accent-dark,var(--accent)) 100%)',
    color: '#fff', fontSize: '0.9rem', fontWeight: 700,
    fontFamily: 'inherit', cursor: 'pointer',
    display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8,
  },
  toolbar: { display: 'flex', gap: 6, marginTop: '0.6rem', flexWrap: 'wrap' },
  toolBtn: {
    padding: '0.4rem 0.8rem', borderRadius: 7,
    border: '1.5px solid var(--border)', background: 'var(--surface)',
    color: 'var(--text-primary)', fontSize: '0.78rem', fontWeight: 600,
    cursor: 'pointer', fontFamily: 'inherit',
  },
  secondaryBtn: {
    padding: '0.3rem 0.6rem', borderRadius: 6,
    border: '1px solid var(--border)', background: 'var(--bg)',
    color: 'var(--text-secondary)', fontSize: '0.75rem', fontWeight: 600,
    cursor: 'pointer', fontFamily: 'inherit',
  },
  tinyBtn: {
    padding: '0.25rem 0.55rem', borderRadius: 5,
    border: '1.5px solid var(--border)', background: 'var(--surface)',
    color: 'var(--text-secondary)', fontSize: '0.72rem', fontWeight: 600,
    cursor: 'pointer', fontFamily: 'inherit',
  },
  tinyBtnAccent: {
    background: 'var(--accent-light)', border: '1.5px solid var(--accent)', color: 'var(--accent)',
  },
  saveRow: {
    display: 'flex', alignItems: 'center', gap: 8,
    marginTop: '0.6rem', paddingTop: '0.6rem',
    borderTop: '1px solid var(--border)', flexWrap: 'wrap',
  },
  saveBtn: {
    padding: '0.42rem 1rem', borderRadius: 7, border: 'none',
    background: 'var(--accent)', color: '#fff',
    fontSize: '0.82rem', fontWeight: 700,
    cursor: 'pointer', fontFamily: 'inherit',
  },
  errorBanner: {
    marginTop: '0.6rem', padding: '0.55rem 0.8rem', borderRadius: 7,
    background: '#fef2f2', border: '1px solid #fecaca',
    color: 'var(--error, #b91c1c)', fontSize: '0.8rem',
  },
  warnBanner: {
    marginTop: '0.6rem', padding: '0.55rem 0.8rem', borderRadius: 7,
    background: '#fffbeb', border: '1px solid #fde68a',
    color: '#92400e', fontSize: '0.78rem',
  },
  errorText: { fontSize: '0.78rem', color: 'var(--error, #b91c1c)', marginTop: '0.3rem' },
  metaText:  { fontSize: '0.68rem', color: 'var(--text-secondary)' },
  badge: {
    fontSize: '0.65rem', fontWeight: 700, padding: '2px 6px',
    borderRadius: 4, background: 'var(--accent-light)', color: 'var(--accent)',
  },
  countBadge: {
    fontSize: '0.65rem', fontWeight: 700, padding: '2px 7px',
    borderRadius: 10, background: '#e0f2fe', color: '#0369a1',
    border: '1px solid #bae6fd',
  },
  badgeGreen: { background: '#f0fdf4', color: '#15803d' },
  badgeWarn:  { background: '#fffbeb', color: '#92400e' },
  spinner: {
    width: 15, height: 15, borderRadius: '50%',
    border: '2px solid rgba(255,255,255,0.3)',
    borderTopColor: '#fff',
    display: 'inline-block', animation: 'spin 0.8s linear infinite', flexShrink: 0,
  },
};
