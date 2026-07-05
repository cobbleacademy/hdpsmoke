import { useState, useEffect } from 'react';

const BASE = import.meta.env.BASE_URL;

const EXAMPLE_PROMPTS = [
  "find user's permission on GroupID Alpha12 for test@company.com",
  "does user testuser have access to group Alpha12",
];

export default function GroupPermissionChecker() {
  const [environments, setEnvironments] = useState([]);
  const [activeEnvIdx, setActiveEnvIdx] = useState(0);
  const [configError, setConfigError]   = useState(null);

  const [prompt, setPrompt]     = useState('');
  const [status, setStatus]     = useState('idle'); // idle | loading | done | error
  const [result, setResult]     = useState(null);
  const [error, setError]       = useState(null);

  // ── Load env config on mount — mirrors RangerLibrary/OPAPolicyGenerator ────
  useEffect(() => {
    fetch(`${BASE}permission-config`)
      .then((r) => r.json())
      .then((cfg) => {
        const envs = cfg.environments || [];
        setEnvironments(envs);
        if (envs.length === 0) setConfigError('No permission environments configured.');
      })
      .catch(() => setConfigError('Could not load Group Permission config — is the backend running?'));
  }, []);

  const activeEnv = environments[activeEnvIdx] || null;

  async function handleSubmit(e) {
    e.preventDefault();
    if (!prompt.trim() || !activeEnv) return;
    setStatus('loading');
    setError(null);
    setResult(null);
    try {
      const resp = await fetch(`${BASE}check-permission`, {
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
          <span style={s.headerIcon}>🧭</span>
          <div>
            <div style={s.headerTitle}>Group Permission Checker</div>
            <div style={s.headerSub}>Location-tier hierarchical access evaluation</div>
          </div>
        </div>
      </div>

      {/* ── Env tab strip — separate row, matches Ranger/OPA Library layout ── */}
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
          <span style={s.envHint}> · Set PERMISSION_ENVS in .env to add more environments</span>
        </div>
      )}

      <div style={s.body}>
        {configError && <div style={s.errorBanner}>{configError}</div>}

        <section style={s.panel}>
          <div style={s.panelHead}>
            <h3 style={s.panelTitle}>Access Check Prompt</h3>
          </div>

          <div style={s.tipBox}>
            <div style={s.tipRow}><span style={s.tipLabel}>User</span><span style={s.tipVal}>UPN / email — <code>test@company.com</code></span></div>
            <div style={s.tipRow}><span style={s.tipLabel}>User</span><span style={s.tipVal}>SAM account — <code>testuser</code> (use "user" keyword: "user testuser …")</span></div>
            <div style={s.tipRow}><span style={s.tipLabel}>Group</span><span style={s.tipVal}>Alphanumeric group ID — <code>Alpha12</code> (use "group" keyword: "… group Alpha12")</span></div>
          </div>

          <form onSubmit={handleSubmit}>
            <textarea
              style={s.textarea}
              placeholder={EXAMPLE_PROMPTS[0]}
              value={prompt}
              onChange={(e) => setPrompt(e.target.value)}
            />
            <div style={s.formRow}>
              {EXAMPLE_PROMPTS.map((ex, i) => (
                <button
                  key={i}
                  type="button"
                  style={s.secondaryBtn}
                  onClick={() => setPrompt(ex)}
                >
                  {i === 0 ? '📋 Example (UPN)' : '📋 Example (SAM)'}
                </button>
              ))}
              <button
                type="submit"
                style={s.primaryBtn}
                disabled={status === 'loading' || !prompt.trim() || !activeEnv}
              >
                {status === 'loading' ? 'Checking…' : '▶ Check Permission'}
              </button>
            </div>
          </form>

          {error && <div style={s.errorBanner}>{error}</div>}

          {result && (
            <div style={s.resultWrap}>
              <div
                style={{
                  ...s.resultBadge,
                  ...(result.status === 'PERMIT' ? s.resultPermit : s.resultDeny),
                }}
              >
                {result.status === 'PERMIT' ? '✓ PERMIT' : '✕ DENY'}
              </div>

              <div style={s.detailRows}>
                <div style={s.detailRow}>
                  <span style={s.detailLabel}>user</span>
                  <span style={s.detailValue}>{result.userPrincipalName}</span>
                </div>
                <div style={s.detailRow}>
                  <span style={s.detailLabel}>user location</span>
                  <span style={s.detailValue}>{result.userLocation ?? 'not found'}</span>
                </div>
                <div style={s.detailRow}>
                  <span style={s.detailLabel}>groupId</span>
                  <span style={s.detailValue}>{result.groupId}</span>
                </div>
                <div style={s.detailRow}>
                  <span style={s.detailLabel}>group location</span>
                  <span style={s.detailValue}>{result.groupLocation ?? 'not found'}</span>
                </div>
                <div style={s.detailRow}>
                  <span style={s.detailLabel}>environment</span>
                  <span style={s.detailValue}>{result.envId}</span>
                </div>
                <div style={s.detailRow}>
                  <span style={s.detailLabel}>parsed via</span>
                  <span style={s.detailValue}>{result.mode === 'llm' ? 'LLM structured output' : 'regex fallback'}</span>
                </div>
              </div>
            </div>
          )}
        </section>

        <section style={s.panel}>
          <div style={s.panelHead}>
            <h3 style={s.panelTitle}>Hierarchy Reference</h3>
          </div>
          <table style={s.table}>
            <thead>
              <tr><th style={s.th}>Group's restricted_location</th><th style={s.th}>Weight</th><th style={s.th}>Allowed User Tiers</th></tr>
            </thead>
            <tbody>
              <tr>
                <td style={s.td}>ONSHORE</td><td style={s.td}>3</td>
                <td style={s.td}>ONSHORE only</td>
              </tr>
              <tr>
                <td style={s.td}>NEARSHORE</td><td style={s.td}>2</td>
                <td style={s.td}>ONSHORE, NEARSHORE</td>
              </tr>
              <tr>
                <td style={s.td}>OFFSHORE</td><td style={s.td}>1</td>
                <td style={s.td}>ONSHORE, NEARSHORE, OFFSHORE</td>
              </tr>
              <tr>
                <td style={s.td}>NONE (unrestricted)</td><td style={s.td}>0</td>
                <td style={s.td}>ONSHORE, NEARSHORE, OFFSHORE — anyone</td>
              </tr>
            </tbody>
          </table>
          <p style={s.tableNote}>
            A user is permitted when their tier's weight is ≥ the group's restricted_location weight.
            NONE is only ever a group's restriction state — a user's own location is always
            ONSHORE, NEARSHORE, or OFFSHORE, never NONE. The names above (ONSHORE, restricted_location)
            describe this app's internal model — the selected environment's real table, column, and
            tier-code names may differ; the weight comparison itself is the same everywhere.
          </p>
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

  tipBox: {
    marginBottom: '0.85rem', padding: '0.65rem 0.85rem', borderRadius: 8,
    background: 'rgba(99,102,241,0.06)', border: '1px solid rgba(99,102,241,0.2)',
    display: 'flex', flexDirection: 'column', gap: 4,
  },
  tipRow: { display: 'flex', gap: 8, alignItems: 'baseline', fontSize: '0.78rem' },
  tipLabel: { fontWeight: 700, color: 'var(--accent)', fontFamily: 'ui-monospace, SFMono-Regular, monospace', minWidth: 44 },
  tipVal: { color: 'var(--text-secondary)', lineHeight: 1.45 },

  errorBanner: {
    marginTop: '0.75rem', padding: '0.5rem 0.75rem', borderRadius: 8,
    background: 'rgba(220,38,38,0.08)', border: '1px solid var(--error)', color: 'var(--error)', fontSize: '0.8rem',
  },

  resultWrap: { marginTop: '1.25rem' },
  resultBadge: {
    display: 'inline-block', padding: '0.6rem 1.5rem', borderRadius: 10,
    fontSize: '1.1rem', fontWeight: 800, letterSpacing: '0.04em',
  },
  resultPermit: { background: '#dcfce7', color: '#15803d', border: '2px solid #22c55e' },
  resultDeny:   { background: '#fee2e2', color: '#b91c1c', border: '2px solid #ef4444' },

  detailRows: { marginTop: '0.85rem', display: 'flex', flexDirection: 'column', gap: 6 },
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
  tableNote: { fontSize: '0.78rem', color: 'var(--text-secondary)', marginTop: '0.75rem', lineHeight: 1.5 },
};
