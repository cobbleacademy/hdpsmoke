'use strict';

const fs   = require('fs');
const path = require('path');
const { buildPolicyKey, deriveScope } = require('./abacSqlParser');

// ── Storage paths ─────────────────────────────────────────────────────────────

function baseStoragePath() {
  return (
    process.env.OPA_POLICY_STORAGE_PATH ||
    path.join(__dirname, '../../data/opa-policies')
  );
}

function envDir(envId) {
  const dir = path.join(baseStoragePath(), envId.toLowerCase());
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function manifestFilePath(envId) {
  return path.join(envDir(envId), 'manifest.json');
}

// ── Read / Write manifest ─────────────────────────────────────────────────────

/**
 * Read the manifest for an environment.
 * Returns { nodes: [] } if not yet created.
 */
function readManifest(envId) {
  const p = manifestFilePath(envId);
  if (!fs.existsSync(p)) return { nodes: [] };
  try {
    return JSON.parse(fs.readFileSync(p, 'utf8'));
  } catch {
    return { nodes: [] };
  }
}

function writeManifest(envId, manifest) {
  fs.writeFileSync(manifestFilePath(envId), JSON.stringify(manifest, null, 2), 'utf8');
}

// ── Node CRUD ─────────────────────────────────────────────────────────────────

/**
 * Add or update a policy node in the manifest.
 * Identified by policyKey derived from (catalog, schema, table, policyName).
 *
 * @param {string} envId
 * @param {object} nodeData  { catalog, schema, table, policyName, filePath?,
 *                             branch?, sha?, status?, ruleCount?, scope? }
 * @returns updated manifest
 */
function upsertNode(envId, nodeData) {
  const manifest = readManifest(envId);
  const key = buildPolicyKey(
    nodeData.catalog, nodeData.schema, nodeData.table, nodeData.policyName
  );

  const idx = manifest.nodes.findIndex(
    (n) => buildPolicyKey(n.catalog, n.schema, n.table, n.policyName) === key
  );

  const node = {
    catalog:       nodeData.catalog       ?? null,
    schema:        nodeData.schema        ?? null,
    table:         nodeData.table         ?? null,
    policyName:    nodeData.policyName,
    scope:         nodeData.scope         ?? deriveScope(nodeData.catalog, nodeData.schema, nodeData.table),
    filePath:      nodeData.filePath      ?? null,
    branch:        nodeData.branch        ?? null,
    sha:           nodeData.sha           ?? null,
    status:        nodeData.status        ?? 'pending',
    ruleCount:     nodeData.ruleCount     ?? null,
    lastGenerated: nodeData.lastGenerated ?? null,
  };

  if (idx >= 0) {
    manifest.nodes[idx] = { ...manifest.nodes[idx], ...node };
  } else {
    manifest.nodes.push(node);
  }

  writeManifest(envId, manifest);
  return manifest;
}

/**
 * Remove a policy node identified by its policyKey.
 */
function removeNode(envId, policyKey) {
  const manifest = readManifest(envId);
  manifest.nodes = manifest.nodes.filter(
    (n) => buildPolicyKey(n.catalog, n.schema, n.table, n.policyName) !== policyKey
  );
  writeManifest(envId, manifest);
  return manifest;
}

/**
 * Patch specific fields on a node (status, sha, ruleCount, lastGenerated …).
 */
function updateNodeStatus(envId, policyKey, updates) {
  const manifest = readManifest(envId);
  const node = manifest.nodes.find(
    (n) => buildPolicyKey(n.catalog, n.schema, n.table, n.policyName) === policyKey
  );
  if (node) {
    Object.assign(node, updates);
    writeManifest(envId, manifest);
  }
  return manifest;
}

// ── ABAC environment config ───────────────────────────────────────────────────

/**
 * Parse ABAC_ENVS and per-environment vars into an environment array.
 *
 * Env var pattern:
 *   ABAC_ENVS=DEV,NPE,PROD
 *   ABAC_DEV_BRANCH=dev
 *   ABAC_DEV_BASE_PATH=policies/dev/
 *   ABAC_PROD_BRANCH=main
 *   ABAC_PROD_BASE_PATH=policies/
 *
 * Falls back to a single "Default" environment when ABAC_ENVS is unset.
 * Each environment inherits GITHUB_DEFAULT_OWNER / GITHUB_DEFAULT_REPO.
 *
 * @returns {Array<{id, label, defaultBranch, basePath, defaultOwner, defaultRepo}>}
 */
function getAbacEnvironments() {
  const rawEnvs = process.env.ABAC_ENVS || '';
  const envIds  = rawEnvs.split(',').map((s) => s.trim()).filter(Boolean);

  if (envIds.length === 0) {
    // Single default env — still works with the tree UI
    const showPrompt = process.env.ABAC_DEFAULT_SHOW_PROMPT !== 'false';
    return [
      {
        id:            'default',
        label:         'Default',
        defaultBranch: process.env.GITHUB_DEFAULT_BRANCH || 'main',
        basePath:      '',
        defaultOwner:  process.env.GITHUB_DEFAULT_OWNER || '',
        defaultRepo:   process.env.GITHUB_DEFAULT_REPO  || '',
        showPrompt,
      },
    ];
  }

  return envIds.map((id) => {
    const key = id.toUpperCase().replace(/[^A-Z0-9]/g, '_');
    const showPrompt = process.env[`ABAC_${key}_SHOW_PROMPT`] !== 'false';
    return {
      id,
      label:         id,
      defaultBranch: process.env[`ABAC_${key}_BRANCH`]   || process.env.GITHUB_DEFAULT_BRANCH || 'main',
      basePath:      process.env[`ABAC_${key}_BASE_PATH`] || '',
      defaultOwner:  process.env.GITHUB_DEFAULT_OWNER     || '',
      defaultRepo:   process.env.GITHUB_DEFAULT_REPO      || '',
      showPrompt,
    };
  });
}

module.exports = {
  readManifest,
  writeManifest,
  upsertNode,
  removeNode,
  updateNodeStatus,
  getAbacEnvironments,
};
