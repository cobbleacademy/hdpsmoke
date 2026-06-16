import { useState, useEffect } from 'react';
import RangerPolicyTree   from './RangerPolicyTree';
import RangerPolicyEditor from './RangerPolicyEditor';

const BASE = import.meta.env.BASE_URL;

// ── Example Rego pre-loaded for demo / new policies ───────────────────────────
const EXAMPLE_REGO = `package databricks.abac

import future.keywords.if
import future.keywords.in

# ── Input schema ──────────────────────────────────────────────────────────────
# input.catalog:   string     — catalog name (e.g. "demos")
# input.schema:    string     — "catalog.schema"
# input.principal: string     — user email or service principal
# input.groups:    [string]   — account-level group memberships
# input.row:       {col: val} — row under evaluation

# ── UDF: mask_pii_string ──────────────────────────────────────────────────────
mask_pii_string(_) := "***REDACTED***"

# ── UDF: region_filter_abac ───────────────────────────────────────────────────
region_filter_abac(region) if {
    "analysts-east" in input.groups
    region == "east"
}

region_filter_abac(region) if {
    "analysts-west" in input.groups
    region == "west"
}

# ── Policy: mask_all_pii_strings (COLUMN MASK) ────────────────────────────────
# Masks STRING columns tagged pii — applied to all tables in demos catalog.
column_masked["mask_all_pii_strings"][c] := mask_pii_string(input.row[c]) if {
    input.catalog == "demos"
    not "pii-readers" in input.groups
    has_tag_value(c, "demo_sensitivity", "pii")
}

# ── Policy: region_row_filter (ROW FILTER) ────────────────────────────────────
# Restricts rows by analyst region — east/west analysts see only their region.
row_visible["region_row_filter"] if {
    input.catalog == "demos"
    not "pii-readers" in input.groups
    some region_col
    has_tag_value(region_col, "demo_row_scope", "region")
    region_filter_abac(input.row[region_col])
}

# ── Tag helpers ────────────────────────────────────────────────────────────────
has_tag_value(col, key, val) if { input.column_tags[col][key] == val }
has_tag(col, key)            if { _ := input.column_tags[col][key] }
`;

// ── Add-policy modal ──────────────────────────────────────────────────────────
function AddPolicyModal({ onAdd, onClose }) {
  const [name, setName]               = useState('');
  const [serviceType, setServiceType] = useState('hive');
  const [service, setService]         = useState('');
  const [error, setError]             = useState('');

  function handleSubmit(e) {
    e.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) { setError('Policy name is required'); return; }
    if (!/^[a-zA-Z0-9_-]+$/.test(trimmed)) {
      setError('Name may only contain letters, numbers, hyphens and underscores');
      return;
    }
    onAdd({
      policyKey:   trimmed.toLowerCase().replace(/[^a-z0-9]/g, '_'),
      name:        trimmed,
      serviceType,
      service:     service.trim(),
      hasPolicy:   false,
      lastGenerated: null,
    });
  }

  return (
    <div style={m.overlay} onClick={onClose}>
      <div style={m.modal} onClick={(e) => e.stopPropagation()}>
        <div style={m.header}>
          <span style={m.title}>Add Policy</span>
          <button style={m.closeBtn} onClick={onClose}>✕</button>
        </div>
        <form onSubmit={handleSubmit} style={m.form}>
          <label style={m.label}>Policy name</label>
          <input
            autoFocus
            style={m.input}
            placeholder="e.g. hive-region-row-filter"
            value={name}
            onChange={(e) => { setName(e.target.value); setError(''); }}
          />
          <label style={m.label}>Service type</label>
          <select
            style={m.select}
            value={serviceType}
            onChange={(e) => setServiceType(e.target.value)}
          >
            {['hive', 'hdfs', 'hbase', 'tag'].map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
          <label style={m.label}>Service (optional)</label>
          <input
            style={m.input}
            placeholder="e.g. hive_dev"
            value={service}
            onChange={(e) => setService(e.target.value)}
          />
          {error && <p style={m.error}>{error}</p>}
          <div style={m.actions}>
            <button type="button" style={m.cancelBtn} onClick={onClose}>Cancel</button>
            <button type="submit" style={m.submitBtn}>Add</button>
          </div>
        </form>
      </div>
    </div>
  );
}

// ── Component ─────────────────────────────────────────────────────────────────

export default function RangerLibrary() {
  const [config, setConfig]                   = useState(null);
  const [rangerEnvs, setRangerEnvs]           = useState([]);
  const [activeEnvIdx, setActiveEnvIdx]       = useState(0);
  const [policies, setPolicies]               = useState([]);
  const [selectedKey, setSelectedKey]         = useState(null);
  const [loadingManifest, setLoadingManifest] = useState(false);
  const [showAddModal, setShowAddModal]       = useState(false);
  const [configError, setConfigError]         = useState(null);

  const activeEnv    = rangerEnvs[activeEnvIdx] || null;
  const activeEnvId  = activeEnv?.id || null;
  const selectedEntry = policies.find((p) => p.policyKey === selectedKey) || null;

  // ── Load config on mount ──────────────────────────────────────────────────
  useEffect(() => {
    fetch(`${BASE}ranger-config`)
      .then((r) => r.json())
      .then((cfg) => {
        setConfig(cfg);
        const envs = cfg.rangerEnvironments || [];
        setRangerEnvs(envs);
        if (envs.length === 0) setConfigError('No RANGER_ENVS configured. Add RANGER_ENVS to backend/.env.');
      })
      .catch(() => setConfigError('Could not load Ranger config — is the backend running?'));
  }, []);

  // ── Load manifest when env tab changes ────────────────────────────────────
  useEffect(() => {
    if (!activeEnvId) return;
    setSelectedKey(null);
    loadManifest(activeEnvId);
  }, [activeEnvId]); // eslint-disable-line react-hooks/exhaustive-deps

  async function loadManifest(envId) {
    setLoadingManifest(true);
    try {
      const resp = await fetch(`${BASE}ranger-manifest/${encodeURIComponent(envId)}`);
      if (resp.ok) {
        const data = await resp.json();
        setPolicies(data.policies || []);
      } else {
        setPolicies([]);
      }
    } catch {
      setPolicies([]);
    } finally {
      setLoadingManifest(false);
    }
  }

  function handleAddPolicy(entry) {
    // Optimistically add to list — will be persisted on first Save
    setPolicies((prev) => {
      const exists = prev.find((p) => p.policyKey === entry.policyKey);
      if (exists) return prev;
      return [...prev, entry];
    });
    setSelectedKey(entry.policyKey);
    setShowAddModal(false);
  }

  function handleDelete(policyKey) {
    setPolicies((prev) => prev.filter((p) => p.policyKey !== policyKey));
    if (selectedKey === policyKey) setSelectedKey(null);
  }

  function handlePolicySaved(policyKey, meta) {
    setPolicies((prev) =>
      prev.map((p) =>
        p.policyKey === policyKey
          ? { ...p, hasPolicy: true, ...meta, lastGenerated: new Date().toISOString() }
          : p
      )
    );
  }

  // ── Env config passed to editor ───────────────────────────────────────────
  const envConfig = {
    defaultOwner:     config?.defaultOwner     || '',
    defaultRepo:      config?.defaultRepo      || '',
    defaultBranch:    config?.defaultBranch    || 'main',
    defaultFilePath:  config?.defaultFilePath  || '',
    defaultFetchMode: config?.defaultFetchMode || 'api',
  };

  if (configError) {
    return (
      <div style={s.errorPage}>
        <span style={{ fontSize: '2rem' }}>⚠️</span>
        <p style={s.errorTitle}>Ranger Library unavailable</p>
        <p style={s.errorDesc}>{configError}</p>
      </div>
    );
  }

  return (
    <div style={s.container}>
      {/* ── Header ── */}
      <div style={s.pageHeader}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <span style={s.headerIcon}>🏹</span>
          <div>
            <div style={s.headerTitle}>Ranger Library</div>
            <div style={s.headerSub}>Rego → Apache Ranger Policy</div>
          </div>
        </div>
      </div>

      {/* ── Env tab strip — separate row, matches OPA Library layout ── */}
      {rangerEnvs.length > 1 && (
        <div style={s.tabStrip}>
          {rangerEnvs.map((env, idx) => (
            <button
              key={env.id}
              style={{ ...s.tab, ...(idx === activeEnvIdx ? s.tabActive : {}) }}
              onClick={() => setActiveEnvIdx(idx)}
            >
              {env.label}
              {idx === activeEnvIdx && <span style={s.tabDot} />}
            </button>
          ))}
        </div>
      )}
      {rangerEnvs.length === 1 && (
        <div style={s.singleEnvLabel}>
          Environment: <strong>{rangerEnvs[0].label}</strong>
        </div>
      )}

      {/* ── Body: tree + editor ── */}
      <div style={s.body}>
        {loadingManifest ? (
          <div style={s.loading}>
            <span style={s.spinner} />
            <span>Loading policies…</span>
          </div>
        ) : (
          <>
            <RangerPolicyTree
              envId={activeEnvId}
              policies={policies}
              selectedKey={selectedKey}
              onSelect={setSelectedKey}
              onAdd={() => setShowAddModal(true)}
              onDelete={handleDelete}
              onRefresh={() => loadManifest(activeEnvId)}
            />

            <RangerPolicyEditor
              policyEntry={selectedEntry}
              envId={activeEnvId}
              envConfig={envConfig}
              encryptionEnabled={config?.encryptionEnabled || false}
              exampleRego={EXAMPLE_REGO}
              showPrompt={activeEnv?.showPrompt !== false}
              onPolicySaved={handlePolicySaved}
            />
          </>
        )}
      </div>

      {showAddModal && (
        <AddPolicyModal
          onAdd={handleAddPolicy}
          onClose={() => setShowAddModal(false)}
        />
      )}
    </div>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────
const s = {
  container: {
    display: 'flex', flexDirection: 'column',
    height: '100%', overflow: 'hidden',
    background: 'var(--bg)',
  },
  pageHeader: {
    padding: '1rem 1.25rem 0.75rem',
    borderBottom: '1px solid var(--border)',
    background: 'var(--surface)', flexShrink: 0,
  },
  headerIcon:  { fontSize: '1.75rem', lineHeight: 1, flexShrink: 0 },
  headerTitle: { fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.02em', margin: 0 },
  headerSub:   { fontSize: '0.78rem', color: 'var(--text-secondary)', marginTop: 2 },

  tabStrip: {
    display: 'flex',
    borderBottom: '1px solid var(--border)',
    background: 'var(--surface)',
    padding: '0 1rem',
    flexShrink: 0,
    overflowX: 'auto',
  },
  tab: {
    padding: '0.6rem 1.1rem',
    border: 'none',
    borderBottom: '2.5px solid transparent',
    background: 'transparent',
    color: 'var(--text-secondary)',
    fontSize: '0.85rem', fontWeight: 600,
    cursor: 'pointer', fontFamily: 'inherit',
    position: 'relative',
    display: 'flex', alignItems: 'center', gap: 6,
    whiteSpace: 'nowrap',
    transition: 'color 0.15s',
  },
  tabActive: {
    color: 'var(--accent)',
    borderBottom: '2.5px solid var(--accent)',
  },
  tabDot: {
    width: 6, height: 6, borderRadius: '50%',
    background: 'var(--accent)',
  },
  singleEnvLabel: {
    padding: '0.5rem 1.25rem',
    fontSize: '0.78rem',
    color: 'var(--text-secondary)',
    background: 'var(--surface)',
    borderBottom: '1px solid var(--border)',
    flexShrink: 0,
  },

  body: {
    flex: 1, display: 'flex', overflow: 'hidden', minHeight: 0,
  },

  loading: {
    flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center',
    gap: '0.75rem', color: 'var(--text-secondary)', fontSize: '0.9rem',
  },
  spinner: {
    width: 18, height: 18, borderRadius: '50%',
    border: '2px solid var(--border)', borderTopColor: 'var(--accent)',
    display: 'inline-block', animation: 'spin 0.8s linear infinite',
  },

  errorPage: {
    flex: 1, display: 'flex', flexDirection: 'column',
    alignItems: 'center', justifyContent: 'center',
    gap: '0.75rem', padding: '3rem',
    color: 'var(--text-secondary)', textAlign: 'center',
  },
  errorTitle: { fontSize: '1rem', fontWeight: 700, color: 'var(--text-primary)', margin: 0 },
  errorDesc:  { fontSize: '0.85rem', margin: 0, maxWidth: 420 },
};

// ── Add-policy modal styles ────────────────────────────────────────────────────
const m = {
  overlay: {
    position: 'fixed', inset: 0,
    background: 'rgba(0,0,0,0.45)',
    display: 'flex', alignItems: 'center', justifyContent: 'center',
    zIndex: 1000,
  },
  modal: {
    background: 'var(--surface)', borderRadius: 14,
    border: '1px solid var(--border)',
    width: 360, maxWidth: '90vw',
    padding: '1.25rem',
    boxShadow: '0 8px 30px rgba(0,0,0,0.2)',
  },
  header: {
    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
    marginBottom: '1rem',
  },
  title:    { fontSize: '1rem', fontWeight: 800, color: 'var(--text-primary)' },
  closeBtn: {
    background: 'none', border: 'none', cursor: 'pointer',
    color: 'var(--text-secondary)', fontSize: '1rem', padding: 4,
  },
  form: { display: 'flex', flexDirection: 'column', gap: '0.6rem' },
  label: { fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-secondary)', letterSpacing: '0.04em' },
  input: {
    padding: '0.45rem 0.7rem', borderRadius: 7,
    border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)',
    fontSize: '0.85rem', fontFamily: 'inherit', outline: 'none',
  },
  select: {
    padding: '0.45rem 0.7rem', borderRadius: 7,
    border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)',
    fontSize: '0.85rem', fontFamily: 'inherit', outline: 'none',
  },
  error: { fontSize: '0.78rem', color: 'var(--error, #b91c1c)', margin: 0 },
  actions: { display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '0.5rem' },
  cancelBtn: {
    padding: '0.42rem 1rem', borderRadius: 7,
    border: '1.5px solid var(--border)', background: 'transparent',
    color: 'var(--text-secondary)', fontSize: '0.82rem', fontWeight: 600,
    cursor: 'pointer', fontFamily: 'inherit',
  },
  submitBtn: {
    padding: '0.42rem 1.1rem', borderRadius: 7, border: 'none',
    background: 'var(--accent)', color: '#fff',
    fontSize: '0.82rem', fontWeight: 700,
    cursor: 'pointer', fontFamily: 'inherit',
  },
};
