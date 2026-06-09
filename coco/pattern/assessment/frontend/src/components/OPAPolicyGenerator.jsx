import { useState, useEffect } from 'react';
import PolicyTree   from './PolicyTree';
import PolicyEditor from './PolicyEditor';

const BASE = import.meta.env.BASE_URL;

// ── Example SQL pre-loaded into the direct editor for new/empty policies ─────
// This is passed to PolicyEditor as a prop
const EXAMPLE_SQL = `-- Databricks Unity Catalog ABAC Policy — Tutorial Example
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

// ── Component ─────────────────────────────────────────────────────────────────

export default function OPAPolicyGenerator() {
  const [config, setConfig]             = useState(null);
  const [abacEnvs, setAbacEnvs]         = useState([]);
  const [activeEnvId, setActiveEnvId]   = useState(null);
  const [nodes, setNodes]               = useState([]);          // manifest nodes for active env
  const [selectedKey, setSelectedKey]   = useState(null);
  const [selectedNode, setSelectedNode] = useState(null);
  const [searchQuery, setSearchQuery]   = useState('');
  const [loadingManifest, setLoadingManifest] = useState(false);

  // ── Load config on mount ────────────────────────────────────────────────────
  useEffect(() => {
    fetch(`${BASE}opa-config`)
      .then((r) => r.json())
      .then((cfg) => {
        setConfig(cfg);
        const envs = cfg.abacEnvironments || [];
        setAbacEnvs(envs);
        if (envs.length > 0) setActiveEnvId(envs[0].id);
      })
      .catch(() => setConfig({}));
  }, []);

  // ── Load manifest when env tab changes ──────────────────────────────────────
  useEffect(() => {
    if (!activeEnvId) return;
    setSelectedKey(null);
    setSelectedNode(null);
    setSearchQuery('');
    loadManifest(activeEnvId);
    // Background stale check
    fetch(`${BASE}opa-stale/${encodeURIComponent(activeEnvId)}`)
      .then((r) => r.json())
      .then((data) => {
        if (data.manifest?.nodes) {
          const enriched = enrichNodes(data.manifest.nodes);
          setNodes(enriched);
        }
      })
      .catch(() => {/* non-blocking */});
  }, [activeEnvId]); // eslint-disable-line react-hooks/exhaustive-deps

  async function loadManifest(envId) {
    setLoadingManifest(true);
    try {
      const resp = await fetch(`${BASE}opa-manifest/${encodeURIComponent(envId)}`);
      if (resp.ok) {
        const data = await resp.json();
        setNodes(enrichNodes(data.nodes || []));
      }
    } catch { /* ignore */ }
    finally { setLoadingManifest(false); }
  }

  // Add policyKey to nodes coming from backend (backend already does this but guard anyway)
  function enrichNodes(rawNodes) {
    return rawNodes.map((n) => ({
      ...n,
      policyKey: n.policyKey || buildFrontendKey(n),
    }));
  }

  function buildFrontendKey(n) {
    const norm = (s) => (s || '').toLowerCase().replace(/[^a-z0-9]/g, '_');
    return `${norm(n.catalog)}__${norm(n.schema)}__${norm(n.table)}__${norm(n.policyName)}`;
  }

  // ── Handle node selection ────────────────────────────────────────────────────
  function handleSelectNode(node) {
    setSelectedKey(node.policyKey);
    setSelectedNode(node);
  }

  // ── Handle add nodes from PolicyTree ────────────────────────────────────────
  async function handleAddNodes(newNodes) {
    if (!activeEnvId || !newNodes?.length) return;
    try {
      const resp = await fetch(`${BASE}opa-manifest/${encodeURIComponent(activeEnvId)}/add`, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ nodes: newNodes }),
      });
      if (resp.ok) {
        const data = await resp.json();
        setNodes(enrichNodes(data.nodes || []));
      }
    } catch { /* ignore */ }
  }

  // ── Handle delete node from PolicyTree ──────────────────────────────────────
  async function handleDeleteNode(policyKey) {
    if (!activeEnvId) return;
    try {
      const resp = await fetch(`${BASE}opa-manifest/${encodeURIComponent(activeEnvId)}/node`, {
        method:  'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ policyKey }),
      });
      if (resp.ok) {
        const data = await resp.json();
        setNodes(enrichNodes(data.nodes || []));
        if (selectedKey === policyKey) { setSelectedKey(null); setSelectedNode(null); }
      }
    } catch { /* ignore */ }
  }

  // ── Handle regenerate node from PolicyTree ──────────────────────────────────
  async function handleRegenerateNode(policyKey) {
    if (!activeEnvId) return;
    try {
      const resp = await fetch(`${BASE}opa-manifest/${encodeURIComponent(activeEnvId)}/node/regenerate`, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ policyKey }),
      });
      const data = await resp.json();
      if (resp.ok && data.nodes) {
        setNodes(enrichNodes(data.nodes));
        // If the regenerated policy is currently selected, deselect to force editor refresh
        if (selectedKey === policyKey) {
          setSelectedKey(null);
          setSelectedNode(null);
        }
      }
    } catch { /* ignore */ }
  }

  // ── Handle save callback from PolicyEditor ───────────────────────────────────
  function handlePolicySaved(policyKey, sha, ruleCount) {
    setNodes((prev) =>
      prev.map((n) =>
        n.policyKey === policyKey
          ? { ...n, status: 'current', sha: sha || n.sha, ruleCount: ruleCount ?? n.ruleCount, lastGenerated: new Date().toISOString() }
          : n
      )
    );
  }

  const activeEnvConfig = abacEnvs.find((e) => e.id === activeEnvId) || {};
  const showTabs = abacEnvs.length > 1;

  return (
    <div style={s.page}>
      {/* ── Header ── */}
      <div style={s.pageHeader}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <span style={s.headerIcon}>🛡️</span>
          <div>
            <h1 style={s.title}>OPA Policy Library</h1>
            <p style={s.subtitle}>Generate and manage Rego from Databricks ABAC SQL · catalog / schema / table hierarchy</p>
          </div>
        </div>
      </div>

      {/* ── Env tabs ── */}
      {showTabs && (
        <div style={s.tabStrip}>
          {abacEnvs.map((env) => (
            <button
              key={env.id}
              style={{ ...s.tab, ...(activeEnvId === env.id ? s.tabActive : {}) }}
              onClick={() => setActiveEnvId(env.id)}
            >
              {env.label}
              {activeEnvId === env.id && <span style={s.tabDot} />}
            </button>
          ))}
        </div>
      )}

      {/* ── Single-env label when no tabs ── */}
      {!showTabs && abacEnvs.length === 1 && (
        <div style={s.singleEnvLabel}>
          Environment: <strong>{abacEnvs[0].label}</strong>
          <span style={s.envHint}> · Set ABAC_ENVS in .env to add more environments</span>
        </div>
      )}

      {/* ── Main split layout ── */}
      <div style={s.split}>
        {/* Left panel — policy tree */}
        <PolicyTree
          nodes={nodes}
          selectedKey={selectedKey}
          onSelect={handleSelectNode}
          onAddNodes={handleAddNodes}
          onDeleteNode={handleDeleteNode}
          onRegenerateNode={handleRegenerateNode}
          searchQuery={searchQuery}
          onSearchChange={setSearchQuery}
          config={config}
        />

        {/* Right panel — policy editor */}
        {config && (
          <PolicyEditor
            node={selectedNode}
            envId={activeEnvId}
            envConfig={activeEnvConfig}
            writeAuthRequired={config.writeAuthRequired}
            encryptionEnabled={config.encryptionEnabled}
            onPolicySaved={handlePolicySaved}
            exampleSql={EXAMPLE_SQL}
          />
        )}
      </div>
    </div>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────

const s = {
  page: {
    display: 'flex',
    flexDirection: 'column',
    height: '100vh',
    background: 'var(--bg)',
    overflow: 'hidden',
  },
  pageHeader: {
    padding: '1rem 1.25rem 0.75rem',
    borderBottom: '1px solid var(--border)',
    background: 'var(--surface)',
    flexShrink: 0,
  },
  headerIcon: { fontSize: '1.75rem', lineHeight: 1, flexShrink: 0 },
  title:    { fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.02em', margin: 0 },
  subtitle: { fontSize: '0.78rem', color: 'var(--text-secondary)', marginTop: 2 },

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
    fontSize: '0.85rem',
    fontWeight: 600,
    cursor: 'pointer',
    fontFamily: 'inherit',
    position: 'relative',
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    whiteSpace: 'nowrap',
    transition: 'color 0.15s',
  },
  tabActive: {
    color: 'var(--accent)',
    borderBottom: '2.5px solid var(--accent)',
  },
  tabDot: {
    width: 6, height: 6,
    borderRadius: '50%',
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
  envHint: { color: 'var(--text-secondary)', opacity: 0.7 },

  split: {
    flex: 1,
    display: 'flex',
    overflow: 'hidden',
    minHeight: 0,
  },
};
