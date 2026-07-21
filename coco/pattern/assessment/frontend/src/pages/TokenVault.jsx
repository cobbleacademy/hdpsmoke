import { useState, useEffect, useCallback } from 'react';

const BASE = import.meta.env.BASE_URL;

// Template inserted by "+ Insert template" — clientSecret is present here
// only because this is a brand-new entry; once saved, subsequent edits of
// this same file will never show a stored clientSecret back in the text.
function credentialTemplate() {
  return {
    id: `cred_${Date.now().toString(36)}${Math.floor(Math.random() * 1000)}`,
    displayName: 'New credential set',
    tokenUrl: 'https://example.com/oauth2/token',
    clientId: '',
    clientSecret: '',
    authMethod: 'post-body', // 'post-body' | 'basic'
    extraParams: { grant_type: 'client_credentials' },
  };
}

export default function TokenVault() {
  const [environments, setEnvironments] = useState([]);
  const [activeEnvIdx, setActiveEnvIdx]  = useState(0);
  const [configError, setConfigError]    = useState(null);
  const [writeAuthRequired, setWriteAuthRequired] = useState(false);
  const [adminToken, setAdminToken]      = useState('');

  const [credentials, setCredentials]    = useState([]);
  const [loadingList, setLoadingList]    = useState(false);

  const [editorOpen, setEditorOpen]      = useState(false);
  const [editorText, setEditorText]      = useState(''); // raw JSON — mirrors Payload Library's YAML editor
  const [editorDirty, setEditorDirty]    = useState(false);
  const [editorParseError, setEditorParseError] = useState('');
  const [saveState, setSaveState]        = useState('idle'); // idle | saving | saved | error
  const [saveError, setSaveError]        = useState('');

  const [tokenState, setTokenState]      = useState({}); // credentialId -> { status, token, error }

  const activeEnv = environments[activeEnvIdx] || null;

  useEffect(() => {
    fetch(`${BASE}token-vault-config`)
      .then((r) => r.json())
      .then((cfg) => {
        const envs = cfg.tokenVaultEnvironments || [];
        setEnvironments(envs);
        setWriteAuthRequired(Boolean(cfg.writeAuthRequired));
        if (envs.length === 0) setConfigError('No Token Vault environments configured.');
      })
      .catch(() => setConfigError('Could not load Token Vault config — is the backend running?'));
  }, []);

  const loadCredentials = useCallback(() => {
    if (!activeEnv) return;
    setLoadingList(true);
    fetch(`${BASE}token-vault-manifest/${encodeURIComponent(activeEnv.id)}`)
      .then((r) => r.json())
      .then((data) => setCredentials(data.credentials || []))
      .finally(() => setLoadingList(false));
  }, [activeEnv]);

  useEffect(() => { loadCredentials(); }, [loadCredentials]);

  function handleOpenEditor() {
    // credentials is already the redacted GET response (clientSecret
    // stripped, clientSecretSet flag present) — that's what gets edited.
    // Adding a "clientSecret" field to any entry here, or leaving it out,
    // is exactly how set-once / rotate-on-renewal works: present = new
    // value to store, absent = keep whatever's already stored server-side.
    setEditorText(JSON.stringify(credentials, null, 2));
    setEditorDirty(false);
    setEditorParseError('');
    setSaveState('idle');
    setSaveError('');
    setEditorOpen(true);
  }
  function handleCloseEditor() {
    setEditorOpen(false);
  }

  function handleEditorChange(text) {
    setEditorText(text);
    setEditorDirty(true);
    setSaveState('idle');
    try {
      const parsed = JSON.parse(text);
      if (!Array.isArray(parsed)) throw new Error('Top level must be a JSON array of credential sets');
      setEditorParseError('');
    } catch (err) {
      setEditorParseError(err.message);
    }
  }

  function handleInsertTemplate() {
    let list;
    try {
      list = JSON.parse(editorText || '[]');
      if (!Array.isArray(list)) list = [];
    } catch {
      list = [];
    }
    list.push(credentialTemplate());
    const text = JSON.stringify(list, null, 2);
    setEditorText(text);
    setEditorDirty(true);
    setEditorParseError('');
  }

  async function handleSave() {
    if (!activeEnv) return;
    let payload;
    try {
      payload = JSON.parse(editorText);
      if (!Array.isArray(payload)) throw new Error('Top level must be a JSON array of credential sets');
    } catch (err) {
      setEditorParseError(err.message);
      return;
    }
    setSaveState('saving');
    setSaveError('');
    const headers = { 'Content-Type': 'application/json' };
    if (writeAuthRequired && adminToken) headers.Authorization = `Bearer ${adminToken}`;
    try {
      const resp = await fetch(`${BASE}token-vault-manifest/${encodeURIComponent(activeEnv.id)}`, {
        method: 'PUT',
        headers,
        body: JSON.stringify({ credentials: payload }),
      });
      const data = await resp.json();
      if (!resp.ok) {
        setSaveState('error');
        setSaveError(data.error || `Save failed (${resp.status})`);
        return;
      }
      setCredentials(data.credentials || []);
      setSaveState('saved');
      setEditorDirty(false);
      setEditorOpen(false);
      setTokenState({});
    } catch {
      setSaveState('error');
      setSaveError('Network error — is the backend running?');
    }
  }

  async function handleGenerate(credentialId) {
    if (!activeEnv) return;
    setTokenState((prev) => ({ ...prev, [credentialId]: { status: 'loading' } }));
    try {
      const resp = await fetch(`${BASE}token-vault-generate/${encodeURIComponent(activeEnv.id)}/${encodeURIComponent(credentialId)}`, {
        method: 'POST',
      });
      const data = await resp.json();
      if (!resp.ok) {
        setTokenState((prev) => ({ ...prev, [credentialId]: { status: 'error', error: data.error || `Failed (${resp.status})` } }));
        return;
      }
      setTokenState((prev) => ({ ...prev, [credentialId]: { status: 'done', token: data } }));
    } catch {
      setTokenState((prev) => ({ ...prev, [credentialId]: { status: 'error', error: 'Network error — is the backend running?' } }));
    }
  }

  return (
    <div style={s.container}>
      <div style={s.pageHeader}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <span style={s.headerIcon}>🔑</span>
          <div>
            <div style={s.headerTitle}>Token Vault</div>
            <div style={s.headerSub}>Multi-set bearer token provisioning — Entra ID, ForgeRock, or any OAuth2 client-credentials vendor</div>
          </div>
        </div>
        <button style={editorOpen ? s.editLibraryBtnActive : s.editLibraryBtn} onClick={editorOpen ? handleCloseEditor : handleOpenEditor}>
          {editorOpen ? '✕ Close Editor' : '✎ Edit Library'}
        </button>
      </div>

      {environments.length > 1 && (
        <div style={s.tabStrip}>
          {environments.map((env, idx) => (
            <button
              key={env.id}
              style={{ ...s.tab, ...(idx === activeEnvIdx ? s.tabActive : {}) }}
              onClick={() => { setActiveEnvIdx(idx); setEditorOpen(false); setTokenState({}); }}
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
          <span style={s.envHint}> · Set TOKEN_VAULT_ENVS in .env to add more environments</span>
        </div>
      )}

      <div style={s.body}>
        {configError && <div style={s.errorBanner}>{configError}</div>}

        {editorOpen ? (
          <section style={s.panel}>
            <div style={s.panelHead}>
              <h3 style={s.panelTitle}>Edit Credential Sets — {activeEnv?.label}</h3>
              <p style={s.panelSub}>
                Raw JSON array. Stored clientSecret values are never shown here — add a <code>"clientSecret"</code> field
                to a credential set only when setting it for the first time or rotating it; omit it (as loaded) to keep
                the value already stored.
              </p>
            </div>

            {writeAuthRequired && (
              <div style={s.formRow}>
                <label style={s.label}>Admin token</label>
                <input
                  type="password"
                  style={s.input}
                  placeholder="PAYLOAD_ADMIN_TOKEN"
                  value={adminToken}
                  onChange={(e) => setAdminToken(e.target.value)}
                />
              </div>
            )}

            <textarea
              style={s.jsonEditor}
              spellCheck={false}
              value={editorText}
              onChange={(e) => handleEditorChange(e.target.value)}
            />
            {editorParseError && <div style={s.errorBanner}>Invalid JSON: {editorParseError}</div>}

            <div style={s.formRow}>
              <button style={s.secondaryBtn} onClick={handleInsertTemplate}>+ Insert template</button>
              {editorDirty && <span style={s.hintText}>Unsaved changes</span>}
            </div>

            {saveError && <div style={s.errorBanner}>{saveError}</div>}

            <div style={s.formRow}>
              <button
                style={s.primaryBtn}
                onClick={handleSave}
                disabled={saveState === 'saving' || Boolean(editorParseError) || (writeAuthRequired && !adminToken)}
              >
                {saveState === 'saving' ? 'Saving…' : 'Save'}
              </button>
              {writeAuthRequired && !adminToken && (
                <span style={s.hintText}>Enter the admin token above to enable saving.</span>
              )}
            </div>
          </section>
        ) : (
          <>
            {loadingList ? (
              <p style={s.muted}>Loading…</p>
            ) : credentials.length === 0 ? (
              <p style={s.muted}>No credential sets configured for {activeEnv?.label}. Click "✎ Edit Library" to add one.</p>
            ) : (
              <div style={s.cardGrid}>
                {credentials.map((c) => {
                  const ts = tokenState[c.id] || { status: 'idle' };
                  return (
                    <section key={c.id} style={s.credCard}>
                      <div style={s.credCardHead}>
                        <span style={s.credName}>{c.displayName}</span>
                        <button
                          style={s.primaryBtnSmall}
                          disabled={ts.status === 'loading'}
                          onClick={() => handleGenerate(c.id)}
                        >
                          {ts.status === 'loading' ? '…' : 'Generate Token'}
                        </button>
                      </div>
                      <div style={s.credMeta}>{c.tokenUrl}</div>
                    </section>
                  );
                })}
              </div>
            )}

            <section style={s.panel}>
              <div style={s.panelHead}>
                <h3 style={s.panelTitle}>Output Bearer Token</h3>
              </div>
              <div style={s.output}>
                {Object.entries(tokenState).map(([id, ts]) => {
                  const cred = credentials.find((c) => c.id === id);
                  if (ts.status === 'loading') {
                    return <div key={id} style={s.outputLine}>Acquiring token for {cred?.displayName || id}…</div>;
                  }
                  if (ts.status === 'error') {
                    return <div key={id} style={{ ...s.outputLine, color: 'var(--error)' }}>{cred?.displayName || id}: {ts.error}</div>;
                  }
                  if (ts.status === 'done') {
                    return (
                      <div key={id} style={s.outputLine}>
                        {cred?.displayName || id} ({ts.token.cached ? 'cached' : 'fresh'}, expires_in={String(ts.token.expires_in)}):
                        <br />{ts.token.access_token}
                      </div>
                    );
                  }
                  return null;
                })}
                {Object.keys(tokenState).length === 0 && <div style={s.outputLine}>No token generated yet.</div>}
              </div>
            </section>
          </>
        )}
      </div>
    </div>
  );
}

const s = {
  container: { display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden', background: 'var(--bg)' },
  pageHeader: {
    padding: '1rem 1.25rem 0.75rem', borderBottom: '1px solid var(--border)', background: 'var(--surface)',
    flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '1rem',
  },
  headerIcon: { fontSize: '1.75rem', lineHeight: 1, flexShrink: 0 },
  headerTitle: { fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.02em' },
  headerSub: { fontSize: '0.78rem', color: 'var(--text-secondary)', marginTop: 2 },

  editLibraryBtn: {
    padding: '0.5rem 1rem', borderRadius: 8, border: '1.5px solid var(--border)', background: 'transparent',
    color: 'var(--text-secondary)', fontSize: '0.82rem', fontWeight: 600, cursor: 'pointer', fontFamily: 'inherit', flexShrink: 0,
  },
  editLibraryBtnActive: {
    padding: '0.5rem 1rem', borderRadius: 8, border: '1.5px solid var(--accent)', background: 'rgba(99,102,241,0.08)',
    color: 'var(--accent)', fontSize: '0.82rem', fontWeight: 600, cursor: 'pointer', fontFamily: 'inherit', flexShrink: 0,
  },

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

  cardGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: '0.75rem' },
  credCard: {
    background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 10,
    padding: '0.85rem 1rem', display: 'flex', flexDirection: 'column', gap: 6,
  },
  credCardHead: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 },
  credName: { fontSize: '0.9rem', fontWeight: 700, color: 'var(--text-primary)' },
  credMeta: {
    fontSize: '0.72rem', color: 'var(--text-secondary)', fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },

  output: {
    background: '#0f1117', border: '1px solid var(--border)', borderRadius: 8,
    padding: '0.85rem 1rem', fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '0.78rem',
    color: '#cdd2f0', display: 'flex', flexDirection: 'column', gap: 8, wordBreak: 'break-all',
  },
  outputLine: { lineHeight: 1.5 },

  formRow: { display: 'flex', gap: 8, alignItems: 'center', marginTop: '0.75rem', flexWrap: 'wrap' },
  label: { fontSize: '0.78rem', fontWeight: 600, color: 'var(--text-secondary)' },
  input: {
    padding: '0.5rem 0.65rem', borderRadius: 7, border: '1.5px solid var(--border)', background: 'var(--bg)',
    color: 'var(--text-primary)', fontFamily: 'inherit', fontSize: '0.82rem', outline: 'none', boxSizing: 'border-box', width: '100%',
  },
  jsonEditor: {
    width: '100%', minHeight: 360, padding: '0.85rem 1rem', borderRadius: 8,
    border: '1.5px solid var(--border)', background: '#0f1117', color: '#cdd2f0',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '0.8rem', lineHeight: 1.5,
    outline: 'none', resize: 'vertical', boxSizing: 'border-box',
  },

  secondaryBtn: {
    padding: '0.5rem 1rem', borderRadius: 8, border: '1.5px solid var(--border)', background: 'transparent',
    color: 'var(--text-secondary)', fontSize: '0.82rem', fontWeight: 600, cursor: 'pointer', fontFamily: 'inherit',
    alignSelf: 'flex-start',
  },
  primaryBtn: {
    padding: '0.5rem 1rem', borderRadius: 8, border: 'none', background: 'var(--accent)',
    color: '#fff', fontSize: '0.82rem', fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
  },
  primaryBtnSmall: {
    padding: '0.35rem 0.75rem', borderRadius: 6, border: 'none', background: 'var(--accent)',
    color: '#fff', fontSize: '0.76rem', fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
  },

  hintText: { fontSize: '0.76rem', color: 'var(--text-secondary)' },
  muted: { fontSize: '0.85rem', color: 'var(--text-secondary)' },
  errorBanner: {
    padding: '0.5rem 0.75rem', borderRadius: 8,
    background: 'rgba(220,38,38,0.08)', border: '1px solid var(--error)', color: 'var(--error)', fontSize: '0.8rem',
  },
};
