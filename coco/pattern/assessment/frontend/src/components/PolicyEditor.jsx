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

/**
 * PolicyEditor — right panel of the OPA Policy Library.
 *
 * Props:
 *   node          ManifestNode  — selected policy (catalog/schema/table/policyName/policyKey/…)
 *   envId         string        — active environment
 *   envConfig     object        — { defaultBranch, basePath, defaultOwner, defaultRepo }
 *   writeAuthRequired bool
 *   encryptionEnabled bool
 *   onPolicySaved (policyKey, sha, ruleCount) => void
 */
/**
 * Example ABAC policy — demonstrates three scopes (catalog, schema, table)
 * with UDFs, tags, column mask, and row filters.
 */
const EXAMPLE_ABAC_SQL = `-- Databricks Unity Catalog ABAC Policy — Tutorial Example
-- Source: docs.databricks.com/aws/en/data-governance/unity-catalog/abac/
-- Edit below to use your own policy, then click "▶ Generate Rego"

-- ── Governed tag definitions (metadata for policy attachment) ───────────────
CREATE GOVERNED TAG demo_sensitivity DESCRIPTION 'PII sensitivity level'
  VALUES ('pii', 'confidential', 'public');

CREATE GOVERNED TAG demo_row_scope DESCRIPTION 'Column for row-level access'
  VALUES ('region', 'department');

-- ── Column tagging (marks which columns get which policies) ────────────────
-- These tags are used by the policies below to match columns dynamically
ALTER TABLE demos.uc_governance.customers ALTER COLUMN email
  SET TAGS ('demo_sensitivity' = 'pii');

ALTER TABLE demos.uc_governance.customers ALTER COLUMN ssn
  SET TAGS ('demo_sensitivity' = 'pii');

ALTER TABLE demos.uc_governance.customers ALTER COLUMN salary
  SET TAGS ('demo_sensitivity' = 'confidential');

ALTER TABLE demos.uc_governance.customers ALTER COLUMN region
  SET TAGS ('demo_row_scope' = 'region');

-- ── User-Defined Functions (UDFs) — called by policies ────────────────────
-- These functions check group membership or compute transformations
CREATE OR REPLACE FUNCTION mask_pii_string(value STRING)
  RETURNS STRING
  RETURN '***REDACTED***';

CREATE OR REPLACE FUNCTION mask_salary(value DECIMAL(10,2))
  RETURNS DECIMAL(10,2)
  RETURN 0.00;

-- Row filter UDF — checks user's group + column value
CREATE OR REPLACE FUNCTION region_filter_abac(user_region STRING)
  RETURNS BOOLEAN
  RETURN (is_account_group_member('analysts-east') AND user_region = 'east')
      OR (is_account_group_member('analysts-west') AND user_region = 'west')
      OR is_account_group_member('data-analysts');

-- ── Catalog-level policy (applies everywhere in demos catalog) ────────────
CREATE POLICY mask_all_pii_strings
  ON CATALOG demos
  COMMENT 'Mask STRING columns tagged with pii sensitivity across the catalog.'
  COLUMN MASK mask_pii_string
  TO \`account users\` EXCEPT \`pii-readers\`
  FOR TABLES MATCH COLUMNS has_tag_value('demo_sensitivity', 'pii') AS c
  ON COLUMN c;

-- ── Schema-level policy (applies to all tables in customers schema) ───────
CREATE POLICY mask_salary_in_schema
  ON SCHEMA demos.uc_governance
  COMMENT 'Mask salary column in all tables using a decimal-aware function.'
  COLUMN MASK mask_salary
  TO \`account users\` EXCEPT \`payroll-team\`
  FOR TABLES MATCH COLUMNS has_tag_value('demo_sensitivity', 'confidential') AS c
  ON COLUMN c;

-- ── Table-level policy (applies only to customers table) ────────────────
CREATE POLICY region_row_filter
  ON TABLE demos.uc_governance.customers
  COMMENT 'Restrict rows by analyst region. Users see only their region data.'
  ROW FILTER region_filter_abac(region)
  TO \`account users\` EXCEPT \`data-admins\`
  FOR TABLES MATCH COLUMNS has_tag_value('demo_row_scope', 'region') AS region
  USING COLUMNS (region);
`;

export default function PolicyEditor({
  node,
  envId,
  envConfig = {},
  writeAuthRequired = false,
  encryptionEnabled = false,
  onPolicySaved,
  exampleSql = EXAMPLE_ABAC_SQL,
}) {
  // ── Source ────────────────────────────────────────────────────────────────
  const [sourceMode, setSourceMode] = useState('direct'); // 'github'|'direct'
  const [fetchMode, setFetchMode]   = useState('api');
  const [filePath, setFilePath]     = useState('');
  const [branch, setBranch]         = useState('');
  const [directSql, setDirectSql]   = useState(exampleSql);  // Pre-populate with example

  // ── Generation ────────────────────────────────────────────────────────────
  const [status, setStatus]         = useState('idle'); // idle|fetching|generating|ready|saving|saved|error
  const [error, setError]           = useState(null);
  const [warning, setWarning]       = useState(null);

  // ── Results ───────────────────────────────────────────────────────────────
  const [abacPreview, setAbacPreview]     = useState('');
  const [promptText, setPromptText]       = useState('');
  const [originalPrompt, setOriginalPrompt] = useState('');
  const [promptEdited, setPromptEdited]   = useState(false);
  const [regoPolicy, setRegoPolicy]       = useState('');
  const [tokenUsage, setTokenUsage]       = useState(null);
  const [isMock, setIsMock]               = useState(false);
  const [sourceRef, setSourceRef]         = useState(null);

  // ── Panels ────────────────────────────────────────────────────────────────
  const [promptOpen, setPromptOpen]   = useState(false);
  const [previewOpen, setPreviewOpen] = useState(false);

  // ── v2 save ───────────────────────────────────────────────────────────────
  const [adminToken, setAdminToken]   = useState('');
  const [saveError, setSaveError]     = useState('');
  const [copied, setCopied]           = useState(false);

  // ── Reset when node changes ───────────────────────────────────────────────
  useEffect(() => {
    if (!node) return;
    setError(null); setWarning(null); setSaveError('');
    setAbacPreview(''); setPromptText(''); setOriginalPrompt('');
    setPromptEdited(false); setTokenUsage(null); setIsMock(false);
    setSourceRef(null); setPromptOpen(false); setPreviewOpen(false);
    setStatus('idle');

    // Pre-populate source from node metadata
    if (node.filePath) {
      setSourceMode('github');
      setFilePath(node.filePath);
      setBranch(node.branch || envConfig.defaultBranch || '');
    } else {
      setSourceMode('direct');
      setFilePath(''); setBranch('');
    }

    // Load saved Rego if it exists
    loadSavedRego();
  }, [node?.policyKey]); // eslint-disable-line react-hooks/exhaustive-deps

  async function loadSavedRego() {
    if (!node?.policyKey || !envId) { setRegoPolicy(''); return; }
    try {
      const resp = await fetch(`${BASE}opa-policy/${encodeURIComponent(envId)}/${encodeURIComponent(node.policyKey)}`);
      if (resp.ok) {
        const data = await resp.json();
        setRegoPolicy(data.rego || '');
        setStatus('ready');
      } else {
        setRegoPolicy('');
        setStatus('idle');
      }
    } catch {
      setRegoPolicy('');
      setStatus('idle');
    }
  }

  // ── Generate ─────────────────────────────────────────────────────────────
  async function handleGenerate(useCustomPrompt = false) {
    setError(null); setWarning(null); setSaveError('');
    setStatus(sourceMode === 'github' ? 'fetching' : 'generating');

    const body = {
      sourceMode,
      envId:      envId     || undefined,
      policyKey:  node?.policyKey || undefined,
    };

    if (sourceMode === 'direct') {
      body.abacSql = directSql;
    } else {
      body.owner     = envConfig.defaultOwner || undefined;
      body.repo      = envConfig.defaultRepo  || undefined;
      body.branch    = branch || envConfig.defaultBranch;
      body.filePath  = filePath;
      body.fetchMode = fetchMode;
    }

    if (useCustomPrompt && promptEdited) body.customPrompt = promptText;

    if (sourceMode === 'github') {
      await new Promise((r) => setTimeout(r, 500));
      setStatus('generating');
    }

    try {
      const resp = await fetch(`${BASE}opa-generate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await resp.json();

      if (!resp.ok) { setError(data.error || 'Generation failed'); setStatus('error'); return; }

      setRegoPolicy(data.regoPolicy || '');
      setPromptText(data.builtPrompt || '');
      setOriginalPrompt(data.builtPrompt || '');
      setPromptEdited(false);
      setSourceRef(data.sourceRef || null);
      setAbacPreview(sourceMode === 'direct' ? directSql : (data.sourceRef?.content || directSql));
      setTokenUsage(data.tokenUsage || null);
      setIsMock(data.mock || false);
      if (data.warning) setWarning(data.warning);
      setStatus('ready');

      // Notify parent of updated node (ruleCount + sha)
      if (data.ruleCount != null && node?.policyKey) {
        onPolicySaved?.(node.policyKey, data.sourceRef?.sha || null, data.ruleCount);
      }
    } catch {
      setError('Network error — make sure the backend is running.');
      setStatus('error');
    }
  }

  // ── Manual Save ──────────────────────────────────────────────────────────
  async function handleSave() {
    if (!regoPolicy.trim() || !node?.policyKey || !envId) return;
    setSaveError(''); setStatus('saving');

    const headers = { 'Content-Type': 'application/json' };
    if (writeAuthRequired && adminToken) headers['Authorization'] = `Bearer ${adminToken}`;

    try {
      const resp = await fetch(
        `${BASE}opa-policy/${encodeURIComponent(envId)}/${encodeURIComponent(node.policyKey)}`,
        { method: 'PUT', headers, body: JSON.stringify({ rego: regoPolicy }) }
      );
      const data = await resp.json();
      if (!resp.ok) { setSaveError(data.error || 'Save failed'); setStatus('ready'); return; }
      setStatus('saved');
      onPolicySaved?.(node.policyKey, null, data.ruleCount ?? null);
    } catch {
      setSaveError('Network error during save');
      setStatus('ready');
    }
  }

  function handleCopy() {
    navigator.clipboard.writeText(regoPolicy).then(() => {
      setCopied(true); setTimeout(() => setCopied(false), 2000);
    });
  }

  function handleDownload() {
    const blob = new Blob([regoPolicy], { type: 'text/plain' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url;
    a.download = `${node?.policyName || 'policy'}.rego`;
    a.click();
    URL.revokeObjectURL(url);
  }

  // ── Derived ──────────────────────────────────────────────────────────────
  const isRunning  = status === 'fetching' || status === 'generating';
  const hasResult  = !!regoPolicy;
  const isSaving   = status === 'saving';
  const isSaved    = status === 'saved';

  const canGenerate = sourceMode === 'direct'
    ? directSql.trim().length > 0
    : !!(filePath.trim());

  // Breadcrumb
  const breadcrumb = [node?.catalog, node?.schema, node?.table]
    .filter(Boolean).join(' → ');

  const statusLabel = {
    fetching:   'Fetching from GitHub…',
    generating: 'Generating Rego…',
    saving:     'Saving…',
  }[status] || '';

  if (!node) {
    return (
      <div style={s.empty}>
        <span style={s.emptyIcon}>🛡️</span>
        <p style={s.emptyTitle}>Select a policy from the tree</p>
        <p style={s.emptyDesc}>Choose a catalog, schema, or table policy on the left to view, generate, or edit its Rego.</p>
      </div>
    );
  }

  return (
    <div style={s.editor}>
      {/* Header */}
      <div style={s.editorHeader}>
        <div>
          <div style={s.breadcrumb}>{breadcrumb || node.catalog}</div>
          <div style={s.policyTitle}>{node.policyName}</div>
        </div>
        <div style={{ display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap' }}>
          {node.scope && (
            <span style={{ ...s.scopeBadge, ...scopeColors[node.scope] }}>{node.scope}</span>
          )}
          {isMock && <span style={s.mockBadge}>Mock mode</span>}
        </div>
      </div>

      {/* ── Source config ── */}
      <div style={s.card}>
        <div style={s.cardHeader}>
          <span style={s.cardLabel}>SOURCE</span>
          <div style={s.modeToggle}>
            {['direct', 'github'].map((m) => (
              <button
                key={m}
                onClick={() => setSourceMode(m)}
                style={{ ...s.modeBtn, ...(sourceMode === m ? s.modeBtnActive : {}) }}
              >
                {m === 'direct' ? '✏️ Direct Input' : '🐙 GitHub'}
              </button>
            ))}
          </div>
        </div>

        {sourceMode === 'github' && (
          <div style={s.githubRow}>
            <input
              style={s.input}
              placeholder="File path (e.g. policies/dev/demos.sql)"
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
                  <input type="radio" value={val} checked={fetchMode === val} onChange={() => setFetchMode(val)} style={{ marginRight: 3 }} />
                  {lbl}
                </label>
              ))}
            </div>
          </div>
        )}

        {sourceMode === 'direct' && (
          <>
            <div style={{ display: 'flex', gap: 4, marginBottom: '0.4rem', justifyContent: 'flex-end' }}>
              <button
                style={s.secondaryBtn}
                onClick={() => setDirectSql(exampleSql)}
                title="Load the tutorial example (catalog/schema/table policies)"
              >
                📚 Load Example
              </button>
              <button
                style={s.secondaryBtn}
                onClick={() => setDirectSql('')}
                title="Clear the SQL editor"
              >
                ✕ Clear
              </button>
            </div>
            <textarea
              style={s.codeArea}
              rows={12}
              value={directSql}
              onChange={(e) => setDirectSql(e.target.value)}
              placeholder="Paste Databricks ABAC SQL here… (Click 'Load Example' to see a tutorial)"
              spellCheck={false}
            />
          </>
        )}

        {error && <div style={s.errorBanner}>{error}</div>}
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
            sourceMode === 'github' ? '🐙 Fetch & Generate Rego' : '▶ Generate Rego'
          )}
        </button>
      </div>

      {/* ── Prompt panel ── */}
      {hasResult && (
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
                    <button style={s.tinyBtn} onClick={() => { setPromptText(originalPrompt); setPromptEdited(false); }}>Reset</button>
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

      {/* ── ABAC source preview ── */}
      {hasResult && abacPreview && (
        <div style={s.card}>
          <SectionHeader
            title="ABAC Source"
            badge={sourceRef?.extractedFromNotebook ? `notebook · ${sourceRef.sqlBlockCount} blocks` : sourceMode}
            open={previewOpen}
            onToggle={() => setPreviewOpen((o) => !o)}
          />
          {previewOpen && <pre style={s.sourcePreview}>{abacPreview}</pre>}
        </div>
      )}

      {/* ── OPA output ── */}
      {(hasResult || status === 'idle') && (
        <div style={s.card}>
          <div style={s.cardHeader}>
            <span style={s.cardLabel}>OPA POLICY (REGO)</span>
            <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
              {encryptionEnabled !== undefined && (
                <span style={{ ...s.badge, ...(encryptionEnabled ? s.badgeGreen : s.badgeWarn) }}>
                  {encryptionEnabled ? '🔒 encrypted' : '⚠ plain text'}
                </span>
              )}
              {tokenUsage && (tokenUsage.promptTokens > 0 || tokenUsage.completionTokens > 0) && (
                <span style={s.metaText}>{tokenUsage.promptTokens}↑ + {tokenUsage.completionTokens}↓ tokens</span>
              )}
            </div>
          </div>

          <textarea
            style={{ ...s.codeArea, minHeight: 280 }}
            value={regoPolicy}
            onChange={(e) => { setRegoPolicy(e.target.value); setStatus('ready'); }}
            placeholder={status === 'idle' ? 'Click Generate Rego to produce output…' : ''}
            spellCheck={false}
          />

          {hasResult && (
            <>
              <div style={s.toolbar}>
                <button style={s.toolBtn} onClick={handleCopy}>{copied ? '✓ Copied' : '📋 Copy'}</button>
                <button style={s.toolBtn} onClick={handleDownload}>⬇ Download .rego</button>
                <button style={s.toolBtn} onClick={() => handleGenerate(false)} disabled={isRunning}>↺ Regenerate</button>
              </div>

              <div style={s.saveRow}>
                {writeAuthRequired && (
                  <input
                    style={{ ...s.input, maxWidth: 180 }}
                    type="password"
                    placeholder="Admin token"
                    value={adminToken}
                    onChange={(e) => setAdminToken(e.target.value)}
                  />
                )}
                <button
                  style={{
                    ...s.saveBtn,
                    opacity: (!regoPolicy.trim() || isSaving) ? 0.55 : 1,
                    cursor:  (!regoPolicy.trim() || isSaving) ? 'not-allowed' : 'pointer',
                    ...(isSaved ? { background: 'var(--success)' } : {}),
                  }}
                  onClick={handleSave}
                  disabled={!regoPolicy.trim() || isSaving}
                >
                  {isSaving ? 'Saving…' : isSaved ? '✓ Saved' : 'Save 🔒'}
                </button>
              </div>
              {saveError && <p style={s.errorText}>{saveError}</p>}
            </>
          )}
        </div>
      )}
    </div>
  );
}

// ── Scope badge colors ────────────────────────────────────────────────────────
const scopeColors = {
  catalog: { background: '#ede9fe', color: '#6d28d9' },
  schema:  { background: '#e0f2fe', color: '#0369a1' },
  table:   { background: '#dcfce7', color: '#15803d' },
};

// ── Styles ────────────────────────────────────────────────────────────────────
const s = {
  empty: {
    flex: 1, display: 'flex', flexDirection: 'column',
    alignItems: 'center', justifyContent: 'center',
    gap: '0.75rem', padding: '3rem 2rem', color: 'var(--text-secondary)',
  },
  emptyIcon:  { fontSize: '2.5rem' },
  emptyTitle: { fontSize: '1rem', fontWeight: 700, color: 'var(--text-primary)', margin: 0 },
  emptyDesc:  { fontSize: '0.85rem', textAlign: 'center', maxWidth: 320, margin: 0 },

  editor: {
    flex: 1,
    overflowY: 'auto',
    padding: '1.25rem',
    display: 'flex',
    flexDirection: 'column',
    gap: '0.875rem',
    minWidth: 0,
  },
  editorHeader: {
    display: 'flex', justifyContent: 'space-between',
    alignItems: 'flex-start', flexWrap: 'wrap', gap: '0.5rem',
  },
  breadcrumb:   { fontSize: '0.72rem', color: 'var(--text-secondary)', marginBottom: 2 },
  policyTitle:  { fontSize: '1.2rem', fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.01em' },
  scopeBadge: {
    fontSize: '0.68rem', fontWeight: 700, letterSpacing: '0.03em',
    padding: '2px 7px', borderRadius: 4,
  },
  mockBadge: {
    fontSize: '0.68rem', background: '#fff7ed', color: '#c2410c',
    border: '1px solid #fed7aa', borderRadius: 5, padding: '2px 6px', fontWeight: 600,
  },

  card: {
    background: 'var(--surface)',
    border: '1px solid var(--border)',
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

  modeToggle: { display: 'flex', gap: 4 },
  modeBtn: {
    padding: '0.3rem 0.7rem', borderRadius: 7,
    border: '1.5px solid var(--border)', background: 'transparent',
    color: 'var(--text-secondary)', fontSize: '0.78rem', fontWeight: 600,
    cursor: 'pointer', fontFamily: 'inherit',
  },
  modeBtnActive: { background: 'var(--accent-light)', border: '1.5px solid var(--accent)', color: 'var(--accent)' },

  githubRow: { display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center', marginTop: '0.5rem' },
  radioGroup: { display: 'flex', gap: 8 },
  radioLabel: { fontSize: '0.78rem', cursor: 'pointer', color: 'var(--text-primary)', display: 'flex', alignItems: 'center' },
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
    marginTop: '0.5rem',
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
    background: 'linear-gradient(135deg, var(--accent) 0%, var(--accent-dark) 100%)',
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
    transition: 'all 0.15s',
  },
  tinyBtn: {
    padding: '0.25rem 0.55rem', borderRadius: 5,
    border: '1.5px solid var(--border)', background: 'var(--surface)',
    color: 'var(--text-secondary)', fontSize: '0.72rem', fontWeight: 600,
    cursor: 'pointer', fontFamily: 'inherit',
  },
  tinyBtnAccent: { background: 'var(--accent-light)', border: '1.5px solid var(--accent)', color: 'var(--accent)' },

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
    color: 'var(--error)', fontSize: '0.8rem',
  },
  warnBanner: {
    marginTop: '0.6rem', padding: '0.55rem 0.8rem', borderRadius: 7,
    background: '#fffbeb', border: '1px solid #fde68a',
    color: '#92400e', fontSize: '0.78rem',
  },
  errorText: { fontSize: '0.78rem', color: 'var(--error)', marginTop: '0.3rem' },
  metaText:  { fontSize: '0.68rem', color: 'var(--text-secondary)' },

  badge: {
    fontSize: '0.65rem', fontWeight: 700, padding: '2px 6px',
    borderRadius: 4, background: 'var(--accent-light)', color: 'var(--accent)',
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
