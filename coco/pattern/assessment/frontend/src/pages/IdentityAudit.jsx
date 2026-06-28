import { useState, useEffect } from 'react';

const BASE = import.meta.env.BASE_URL;

const EXAMPLE_PROMPT = "Find groups for alex.smith@company.com containing Sec";

export default function IdentityAudit() {
  const [environments, setEnvironments] = useState([]);
  const [activeEnvIdx, setActiveEnvIdx] = useState(0);
  const [configError, setConfigError]   = useState(null);

  const [prompt, setPrompt]     = useState('');
  const [status, setStatus]     = useState('idle'); // idle | loading | done | error
  const [result, setResult]     = useState(null);
  const [error, setError]       = useState(null);

  // ── Load env config on mount — mirrors GroupPermissionChecker/RangerLibrary ──
  useEffect(() => {
    fetch(`${BASE}identity-audit-config`)
      .then((r) => r.json())
      .then((cfg) => {
        const envs = cfg.environments || [];
        setEnvironments(envs);
        if (envs.length === 0) setConfigError('No Identity Audit environments configured.');
      })
      .catch(() => setConfigError('Could not load Identity Audit config — is the backend running?'));
  }, []);

  const activeEnv = environments[activeEnvIdx] || null;

  async function handleSubmit(e) {
    e.preventDefault();
    if (!prompt.trim() || !activeEnv) return;
    setStatus('loading');
    setError(null);
    setResult(null);
    try {
      const resp = await fetch(`${BASE}identity-audit`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt, envId: activeEnv.id }),
      });
      const data = await resp.json();
      if (!resp.ok) {
        setError(data.error || `Request failed (${resp.status})`);
        setStatus('error');
        return;
      }
      setResult(data);
      setStatus('done');
    } catch {
      setError('Network error — is the backend running?');
      setStatus('error');
    }
  }

  return (
    <div style={s.container}>
      <div style={s.pageHeader}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <span style={s.headerIcon}>🔎</span>
          <div>
            <div style={s.headerTitle}>Identity Audit</div>
            <div style={s.headerSub}>Entra ID transitive group footprint, via natural language</div>
          </div>
        </div>
      </div>

      {/* ── Env tab strip — mirrors GroupPermissionChecker/RangerLibrary ── */}
      {environments.length > 1 && (
        <div style={s.tabStrip}>
          {environments.map((env, idx) => (
            <button
              key={env.id}
              style={{ ...s.tab, ...(idx === activeEnvIdx ? s.tabActive : {}) }}
              onClick={() => { setActiveEnvIdx(idx); setResult(null); setError(null); }}
            >
              {env.label}
              {idx === activeEnvIdx && <span style={s.tabDot} />}
            </button>
          ))}
        </div>
      )}
      {environments.length === 1 && (
        <div style={s.singleEnvLabel}>
          Environment: <strong>{environments[0].label}</strong>
          <span style={s.envHint}> · Set IDENTITY_AUDIT_ENVS in .env to add more environments</span>
        </div>
      )}

      <div style={s.body}>
        {configError && <div style={s.errorBanner}>{configError}</div>}

        <section style={s.panel}>
          <div style={s.panelHead}>
            <h3 style={s.panelTitle}>Audit Prompt</h3>
            <p style={s.panelSub}>
              "Find groups for [UPN] containing [string]" · "starts with [string]" · "ending in [string]" —
              or describe it naturally if the LLM parser is enabled. e.g. "{EXAMPLE_PROMPT}"
            </p>
          </div>

          <form onSubmit={handleSubmit}>
            <textarea
              style={s.textarea}
              placeholder={EXAMPLE_PROMPT}
              value={prompt}
              onChange={(e) => setPrompt(e.target.value)}
            />
            <div style={s.formRow}>
              <button type="button" style={s.secondaryBtn} onClick={() => setPrompt(EXAMPLE_PROMPT)}>
                📋 Load Example
              </button>
              <button
                type="submit"
                style={s.primaryBtn}
                disabled={status === 'loading' || !prompt.trim() || !activeEnv}
              >
                {status === 'loading' ? 'Auditing…' : '▶ Run Audit'}
              </button>
            </div>
          </form>

          {error && <div style={s.errorBanner}>{error}</div>}

          {result && (
            <div style={s.resultWrap}>
              {result.mock && (
                <div style={s.mockBanner}>
                  ⚠ Mock data — no Entra credentials configured for {result.envId}. Set
                  IDENTITY_AUDIT_{result.envId}_TENANT_ID / _CLIENT_ID / _CLIENT_SECRET for a real Graph lookup.
                </div>
              )}

              <div style={s.detailRows}>
                <div style={s.detailRow}>
                  <span style={s.detailLabel}>upn</span>
                  <span style={s.detailValue}>{result.upn}</span>
                </div>
                <div style={s.detailRow}>
                  <span style={s.detailLabel}>environment</span>
                  <span style={s.detailValue}>{result.envId}</span>
                </div>
                <div style={s.detailRow}>
                  <span style={s.detailLabel}>filters applied</span>
                  <span style={s.detailValue}>
                    {result.filters.length === 0
                      ? `none (${result.filterSource})`
                      : result.filters.map((f) => `${f.type}:${f.value}`).join(' OR ') + ` (${result.filterSource})`}
                  </span>
                </div>
                <div style={s.detailRow}>
                  <span style={s.detailLabel}>groups</span>
                  <span style={s.detailValue}>{result.groups.length} of {result.totalBeforeFilter} total</span>
                </div>
                <div style={s.detailRow}>
                  <span style={s.detailLabel}>parsed via</span>
                  <span style={s.detailValue}>{result.mode === 'llm' ? 'LLM structured output' : 'regex fallback'}</span>
                </div>
              </div>

              <table style={s.table}>
                <thead>
                  <tr><th style={s.th}>Group Name</th><th style={s.th}>Object ID</th></tr>
                </thead>
                <tbody>
                  {result.groups.length === 0 ? (
                    <tr><td style={s.td} colSpan={2}>No groups matched.</td></tr>
                  ) : (
                    result.groups.map((g) => (
                      <tr key={g.id}>
                        <td style={s.td}>{g.displayName}</td>
                        <td style={s.tdMono}>{g.id}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}

const s = {
  container: { display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden', background: 'var(--bg)' },
  pageHeader: { padding: '1rem 1.25rem 0.75rem', borderBottom: '1px solid var(--border)', background: 'var(--surface)', flexShrink: 0 },
  headerIcon: { fontSize: '1.75rem', lineHeight: 1, flexShrink: 0 },
  headerTitle: { fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.02em' },
  headerSub: { fontSize: '0.78rem', color: 'var(--text-secondary)', marginTop: 2 },

  tabStrip: {
    display: 'flex', borderBottom: '1px solid var(--border)', background: 'var(--surface)',
    padding: '0 1rem', flexShrink: 0, overflowX: 'auto',
  },
  tab: {
    padding: '0.6rem 1.1rem', border: 'none', borderBottom: '2.5px solid transparent',
    background: 'transparent', color: 'var(--text-secondary)', fontSize: '0.85rem', fontWeight: 600,
    cursor: 'pointer', fontFamily: 'inherit', position: 'relative',
    display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap', transition: 'color 0.15s',
  },
  tabActive: { color: 'var(--accent)', borderBottom: '2.5px solid var(--accent)' },
  tabDot: { width: 6, height: 6, borderRadius: '50%', background: 'var(--accent)' },
  envHint: { color: 'var(--text-secondary)', opacity: 0.7 },
  singleEnvLabel: {
    padding: '0.5rem 1.25rem', fontSize: '0.78rem', color: 'var(--text-secondary)',
    background: 'var(--surface)', borderBottom: '1px solid var(--border)', flexShrink: 0,
  },

  body: { flex: 1, overflowY: 'auto', padding: '1.25rem', display: 'flex', flexDirection: 'column', gap: '1.25rem' },

  panel: {
    background: 'var(--surface)', border: '1px solid var(--border)',
    borderLeft: '4px solid var(--accent)', borderRadius: 14,
    padding: '1.5rem 1.75rem', boxShadow: 'var(--shadow)',
  },
  panelHead: { marginBottom: '1rem' },
  panelTitle: { fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-primary)', margin: 0, letterSpacing: '-0.01em' },
  panelSub: { fontSize: '0.85rem', color: 'var(--text-secondary)', margin: '0.35rem 0 0', lineHeight: 1.5 },

  textarea: {
    width: '100%', minHeight: 80, padding: '0.6rem 0.75rem', borderRadius: 8,
    border: '1.5px solid var(--border)', background: 'var(--bg)', color: 'var(--text-primary)',
    fontFamily: 'inherit', fontSize: '0.85rem', outline: 'none', resize: 'vertical', boxSizing: 'border-box',
  },
  formRow: { display: 'flex', gap: 8, marginTop: '0.75rem' },
  secondaryBtn: {
    padding: '0.5rem 1rem', borderRadius: 8, border: '1.5px solid var(--border)', background: 'transparent',
    color: 'var(--text-secondary)', fontSize: '0.82rem', fontWeight: 600, cursor: 'pointer', fontFamily: 'inherit',
  },
  primaryBtn: {
    padding: '0.5rem 1rem', borderRadius: 8, border: 'none', background: 'var(--accent)',
    color: '#fff', fontSize: '0.82rem', fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
  },

  errorBanner: {
    marginTop: '0.75rem', padding: '0.5rem 0.75rem', borderRadius: 8,
    background: 'rgba(220,38,38,0.08)', border: '1px solid var(--error)', color: 'var(--error)', fontSize: '0.8rem',
  },
  mockBanner: {
    marginBottom: '0.85rem', padding: '0.5rem 0.75rem', borderRadius: 8,
    background: 'rgba(217,119,6,0.08)', border: '1px solid var(--warning, #d97706)', color: 'var(--warning, #d97706)', fontSize: '0.78rem',
  },

  resultWrap: { marginTop: '1.25rem' },
  detailRows: { display: 'flex', flexDirection: 'column', gap: 6, marginBottom: '1rem' },
  detailRow: {
    display: 'flex', justifyContent: 'space-between', gap: 10,
    padding: '0.45rem 0.65rem', borderRadius: 7, background: 'var(--bg)', border: '1px solid var(--border)',
  },
  detailLabel: { fontSize: '0.75rem', fontWeight: 700, color: 'var(--accent)', fontFamily: 'ui-monospace, SFMono-Regular, monospace' },
  detailValue: { fontSize: '0.8rem', color: 'var(--text-primary)', fontFamily: 'ui-monospace, SFMono-Regular, monospace' },

  table: { width: '100%', borderCollapse: 'collapse', fontSize: '0.82rem' },
  th: {
    textAlign: 'left', padding: '0.4rem 0.5rem', borderBottom: '1.5px solid var(--border)',
    color: 'var(--text-secondary)', fontWeight: 700, fontSize: '0.7rem', letterSpacing: '0.03em', textTransform: 'uppercase',
  },
  td: { padding: '0.5rem 0.5rem', borderBottom: '1px solid var(--border)', color: 'var(--text-primary)' },
  tdMono: {
    padding: '0.5rem 0.5rem', borderBottom: '1px solid var(--border)', color: 'var(--text-secondary)',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '0.74rem',
  },
};
