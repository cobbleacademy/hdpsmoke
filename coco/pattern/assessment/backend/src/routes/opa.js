'use strict';

const crypto  = require('crypto');
const express = require('express');
const router  = express.Router();

const { fetchAbacPolicy }    = require('../services/githubService');
const { generateOpaPolicy, buildOpaPrompt, PROMPT_TEMPLATES } = require('../services/opaPolicyService');
const { readPolicyByKey, writePolicyByKey, readPolicy, writePolicy, listPolicies, getEncryptionKey, validateRego } = require('../services/opaPolicyPersistService');
const { parseAbacPolicies, buildPolicyKey, parsePolicyKey } = require('../services/abacSqlParser');
const { readManifest, upsertNode, removeNode, updateNodeStatus, getAbacEnvironments } = require('../services/policyManifestService');

// ── Write-auth middleware ─────────────────────────────────────────────────────

function checkWriteAuth(req, res, next) {
  if (process.env.PAYLOAD_WRITE_AUTH_ENABLED !== 'true') return next();
  const adminToken = process.env.PAYLOAD_ADMIN_TOKEN || '';
  if (!adminToken) return next();

  const authHeader = req.headers['authorization'] || '';
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';
  let valid = false;
  try {
    valid = token.length === adminToken.length &&
      crypto.timingSafeEqual(Buffer.from(token), Buffer.from(adminToken));
  } catch { valid = false; }

  if (!valid) return res.status(401).json({ error: 'Unauthorized: valid Bearer token required' });
  next();
}

// ── Count rule heads in a Rego string ─────────────────────────────────────────
function countRules(rego) {
  return (rego.match(/\b(allow|row_visible|column_masked|deny)\s*[\[{=]/g) || []).length;
}

// ── GET /opa-config ───────────────────────────────────────────────────────────
router.get('/opa-config', (req, res) => {
  const writeAuthRequired =
    process.env.PAYLOAD_WRITE_AUTH_ENABLED === 'true' &&
    Boolean(process.env.PAYLOAD_ADMIN_TOKEN);

  res.json({
    // legacy single-generate fields
    defaultOwner:          process.env.GITHUB_DEFAULT_OWNER    || 'ffgdeo',
    defaultRepo:           process.env.GITHUB_DEFAULT_REPO     || 'uc-governance-demo',
    defaultBranch:         process.env.GITHUB_DEFAULT_BRANCH   || 'main',
    defaultFilePath:       process.env.GITHUB_DEFAULT_ABAC_PATH || 'notebooks/04_row_filters_abac.py',
    defaultFetchMode:      process.env.GITHUB_DEFAULT_FETCH_MODE || 'api',
    schemaVariants:        Object.keys(PROMPT_TEMPLATES),
    githubTokenConfigured: Boolean(process.env.GITHUB_TOKEN),
    writeAuthRequired,
    encryptionEnabled:     Boolean(getEncryptionKey()),
    // multi-env tree fields
    abacEnvironments:      getAbacEnvironments(),
  });
});

// ── GET /opa-manifest/:envId ──────────────────────────────────────────────────
router.get('/opa-manifest/:envId', (req, res) => {
  const { envId } = req.params;
  if (!isValidEnvId(envId)) return res.status(400).json({ error: 'Invalid envId' });

  try {
    const manifest = readManifest(envId);
    // Annotate each node with its computed policyKey
    const nodes = manifest.nodes.map((n) => ({
      ...n,
      policyKey: buildPolicyKey(n.catalog, n.schema, n.table, n.policyName),
      hasRego:   Boolean(readPolicyByKey(envId, buildPolicyKey(n.catalog, n.schema, n.table, n.policyName))),
    }));
    res.json({ nodes });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /opa-manifest/:envId/add ─────────────────────────────────────────────
/**
 * Add one or more nodes to the manifest.
 * Body: { nodes: [{ catalog, schema, table, policyName, scope, filePath, branch, sha }] }
 */
router.post('/opa-manifest/:envId/add', checkWriteAuth, (req, res) => {
  const { envId } = req.params;
  const { nodes  } = req.body;

  if (!isValidEnvId(envId)) return res.status(400).json({ error: 'Invalid envId' });
  if (!Array.isArray(nodes) || nodes.length === 0)
    return res.status(400).json({ error: 'nodes must be a non-empty array' });

  try {
    let manifest;
    for (const n of nodes) {
      if (!n.policyName) continue;
      manifest = upsertNode(envId, n);
    }
    const enriched = manifest.nodes.map((n) => ({
      ...n,
      policyKey: buildPolicyKey(n.catalog, n.schema, n.table, n.policyName),
    }));
    res.json({ nodes: enriched });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── DELETE /opa-manifest/:envId/node ─────────────────────────────────────────
/**
 * Remove a policy node from the manifest.
 * Body: { policyKey: string }
 */
router.delete('/opa-manifest/:envId/node', checkWriteAuth, (req, res) => {
  const { envId    } = req.params;
  const { policyKey } = req.body;

  if (!isValidEnvId(envId))   return res.status(400).json({ error: 'Invalid envId' });
  if (!policyKey)              return res.status(400).json({ error: 'policyKey is required' });

  try {
    const manifest = removeNode(envId, policyKey);
    res.json({ nodes: manifest.nodes.map((n) => ({ ...n, policyKey: buildPolicyKey(n.catalog, n.schema, n.table, n.policyName) })) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /opa-manifest/:envId/node/regenerate ────────────────────────────────
/**
 * Re-fetch a policy's source file from GitHub, regenerate Rego, auto-save.
 * Node must have filePath stored in the manifest (added via GitHub mode).
 *
 * Body: { policyKey: string, schemaVariant?: string }
 */
router.post('/opa-manifest/:envId/node/regenerate', checkWriteAuth, async (req, res) => {
  const { envId } = req.params;
  const { policyKey, schemaVariant = 'default' } = req.body;

  if (!isValidEnvId(envId)) return res.status(400).json({ error: 'Invalid envId' });
  if (!policyKey)            return res.status(400).json({ error: 'policyKey is required' });

  // Look up node from manifest
  const manifest = readManifest(envId);
  const node = manifest.nodes.find(
    (n) => buildPolicyKey(n.catalog, n.schema, n.table, n.policyName) === policyKey
  );
  if (!node) return res.status(404).json({ error: `Policy "${policyKey}" not found in manifest` });
  if (!node.filePath) return res.status(400).json({ error: 'Policy has no filePath — was not added via GitHub mode' });

  // Resolve env config for owner/repo/branch defaults
  const abacEnvs = getAbacEnvironments();
  const envCfg   = abacEnvs.find((e) => e.id === envId) || {};

  const resolvedOwner  = envCfg.defaultOwner || process.env.GITHUB_DEFAULT_OWNER;
  const resolvedRepo   = envCfg.defaultRepo  || process.env.GITHUB_DEFAULT_REPO;
  const resolvedBranch = node.branch || envCfg.defaultBranch || process.env.GITHUB_DEFAULT_BRANCH || 'main';

  if (!resolvedOwner || !resolvedRepo)
    return res.status(400).json({ error: 'GitHub owner/repo not configured for this environment' });

  // 1. Re-fetch from GitHub
  let fetched;
  try {
    fetched = await fetchAbacPolicy({
      owner: resolvedOwner, repo: resolvedRepo,
      branch: resolvedBranch, filePath: node.filePath,
      fetchMode: process.env.GITHUB_DEFAULT_FETCH_MODE || 'api',
      token: process.env.GITHUB_TOKEN || '',
    });
  } catch (err) {
    const status = err.code === 'FILE_NOT_FOUND' ? 404 :
                   err.code === 'UNAUTHORIZED'   ? 401 :
                   err.code === 'RATE_LIMITED'   ? 429 : 502;
    return res.status(status).json({ error: err.message, code: err.code });
  }

  // 2. Re-generate Rego
  let result;
  try {
    result = await generateOpaPolicy(fetched.content, { schemaVariant });
  } catch (err) {
    return res.status(500).json({ error: `Rego generation failed: ${err.message}` });
  }

  const ruleCount = countRules(result.regoPolicy);

  // 3. Auto-save Rego + update manifest
  try {
    writePolicyByKey(envId, policyKey, result.regoPolicy);
    updateNodeStatus(envId, policyKey, {
      status:        'current',
      sha:           fetched.sha || null,
      ruleCount,
      lastGenerated: new Date().toISOString(),
    });
  } catch (saveErr) {
    console.warn('[regenerate] Save failed:', saveErr.message);
  }

  const updatedManifest = readManifest(envId);
  const nodes = updatedManifest.nodes.map((n) => ({
    ...n,
    policyKey: buildPolicyKey(n.catalog, n.schema, n.table, n.policyName),
  }));

  return res.json({
    regoPolicy: result.regoPolicy,
    ruleCount,
    sha:        fetched.sha,
    mock:       result.mock,
    warning:    fetched.warning || null,
    nodes,
  });
});

// ── POST /opa-parse ───────────────────────────────────────────────────────────
/**
 * Parse CREATE POLICY statements from SQL (fetched or direct) without saving.
 * Returns detected policy descriptors ready to add to a manifest.
 *
 * Body: { sourceMode, abacSql, owner, repo, branch, filePath, fetchMode }
 */
router.post('/opa-parse', async (req, res) => {
  const {
    sourceMode = 'direct',
    abacSql,
    owner, repo, branch, filePath,
    fetchMode = 'api',
  } = req.body;

  let sql = '', sha = null, warning = null;

  if (sourceMode === 'direct') {
    if (!abacSql) return res.status(400).json({ error: 'abacSql is required in direct mode' });
    sql = abacSql;
  } else {
    const resolvedOwner = owner || process.env.GITHUB_DEFAULT_OWNER;
    const resolvedRepo  = repo  || process.env.GITHUB_DEFAULT_REPO;
    if (!resolvedOwner || !resolvedRepo || !filePath)
      return res.status(400).json({ error: 'owner, repo and filePath are required in github mode' });

    try {
      const fetched = await fetchAbacPolicy({
        owner: resolvedOwner, repo: resolvedRepo,
        branch: branch || 'main', filePath, fetchMode,
        token: process.env.GITHUB_TOKEN || '',
      });
      sql     = fetched.content;
      sha     = fetched.sha;
      warning = fetched.warning;
    } catch (err) {
      const status = err.code === 'FILE_NOT_FOUND' ? 404 :
                     err.code === 'UNAUTHORIZED'   ? 401 :
                     err.code === 'RATE_LIMITED'   ? 429 : 502;
      return res.status(status).json({ error: err.message, code: err.code });
    }
  }

  const parsed = parseAbacPolicies(sql);
  const policies = parsed.map((p) => ({
    ...p,
    policyKey: buildPolicyKey(p.catalog, p.schema, p.table, p.policyName),
    filePath:  filePath || null,
    branch:    branch   || null,
    sha:       sha      || null,
    status:    'pending',
  }));

  res.json({ policies, warning });
});

// ── GET /opa-stale/:envId ─────────────────────────────────────────────────────
/**
 * Check GitHub SHAs for all nodes that have a filePath+sha stored.
 * Marks changed nodes as 'stale' in the manifest and returns the stale key list.
 */
router.get('/opa-stale/:envId', async (req, res) => {
  const { envId } = req.params;
  if (!isValidEnvId(envId)) return res.status(400).json({ error: 'Invalid envId' });

  const manifest = readManifest(envId);
  const abacEnvs = getAbacEnvironments();
  const envCfg   = abacEnvs?.find((e) => e.id === envId) || {};

  // Collect unique file paths to check (avoid duplicate API calls)
  const toCheck = new Map();
  for (const n of manifest.nodes) {
    if (!n.filePath || !n.sha) continue;
    const resolvedBranch = n.branch || envCfg.defaultBranch || 'main';
    const fileKey = `${resolvedBranch}:${n.filePath}`;
    if (!toCheck.has(fileKey)) {
      toCheck.set(fileKey, { filePath: n.filePath, branch: resolvedBranch });
    }
  }

  // Fetch current SHAs from GitHub (best-effort — skip on error)
  const currentShas = {};
  const owner = envCfg.defaultOwner || process.env.GITHUB_DEFAULT_OWNER || '';
  const repo  = envCfg.defaultRepo  || process.env.GITHUB_DEFAULT_REPO  || '';
  const token = process.env.GITHUB_TOKEN || '';
  const headers = { 'User-Agent': 'pattern-assessment-opa', Accept: 'application/vnd.github.v3+json' };
  if (token) headers.Authorization = `Bearer ${token}`;

  for (const [fileKey, { filePath, branch }] of toCheck) {
    try {
      const url = `https://api.github.com/repos/${owner}/${repo}/contents/${filePath}?ref=${encodeURIComponent(branch)}`;
      const resp = await fetch(url, { headers });
      if (resp.ok) {
        const data = await resp.json();
        currentShas[fileKey] = data.sha;
      }
    } catch { /* skip */ }
  }

  // Identify stale nodes and update manifest
  const staleKeys = [];
  for (const n of manifest.nodes) {
    if (!n.filePath || !n.sha) continue;
    const resolvedBranch = n.branch || envCfg.defaultBranch || 'main';
    const fileKey = `${resolvedBranch}:${n.filePath}`;
    if (currentShas[fileKey] && currentShas[fileKey] !== n.sha) {
      const policyKey = buildPolicyKey(n.catalog, n.schema, n.table, n.policyName);
      staleKeys.push(policyKey);
      updateNodeStatus(envId, policyKey, { status: 'stale' });
    }
  }

  res.json({ staleKeys, manifest: readManifest(envId) });
});

// ── POST /opa-generate ────────────────────────────────────────────────────────
/**
 * Fetch ABAC SQL (GitHub or direct) → build prompt → call LLM → return Rego.
 * When envId + policyKey are provided the Rego is auto-saved and the manifest updated.
 *
 * Body: {
 *   sourceMode, abacSql, owner, repo, branch, filePath, fetchMode,
 *   schemaVariant, customPrompt,
 *   envId?,      ← NEW: environment for manifest update
 *   policyKey?,  ← NEW: policy key for auto-save
 * }
 */
router.post('/opa-generate', async (req, res) => {
  const {
    sourceMode    = 'direct',
    abacSql,
    owner, repo, branch, filePath, fetchMode = 'api',
    schemaVariant = 'default',
    customPrompt,
    envId,
    policyKey,
  } = req.body;

  let abacContent = '';
  let sourceRef   = { mode: sourceMode };

  if (sourceMode === 'direct') {
    if (!abacSql?.trim()) return res.status(400).json({ error: 'abacSql is required in direct mode' });
    abacContent = abacSql.trim();
  } else if (sourceMode === 'github') {
    const resolvedOwner    = owner    || process.env.GITHUB_DEFAULT_OWNER;
    const resolvedRepo     = repo     || process.env.GITHUB_DEFAULT_REPO;
    const resolvedBranch   = branch   || process.env.GITHUB_DEFAULT_BRANCH   || 'main';
    const resolvedFilePath = filePath || process.env.GITHUB_DEFAULT_ABAC_PATH;
    const resolvedFetch    = fetchMode|| process.env.GITHUB_DEFAULT_FETCH_MODE || 'api';

    if (!resolvedOwner || !resolvedRepo || !resolvedFilePath)
      return res.status(400).json({ error: 'owner, repo and filePath are required in github mode' });

    try {
      const fetched = await fetchAbacPolicy({
        owner: resolvedOwner, repo: resolvedRepo,
        branch: resolvedBranch, filePath: resolvedFilePath, fetchMode: resolvedFetch,
        token: process.env.GITHUB_TOKEN || '',
      });
      abacContent = fetched.content;
      sourceRef   = {
        mode: 'github',
        owner: resolvedOwner, repo: resolvedRepo, branch: resolvedBranch,
        filePath: resolvedFilePath, fetchMode: resolvedFetch,
        content: fetched.content,  // Include fetched SQL for frontend to track edits
        sha: fetched.sha, sizeBytes: fetched.sizeBytes,
        extractedFromNotebook: fetched.extractedFromNotebook,
        sqlBlockCount: fetched.sqlBlockCount,
        warning: fetched.warning,
      };
    } catch (err) {
      const status = err.code === 'FILE_NOT_FOUND' ? 404 :
                     err.code === 'UNAUTHORIZED'   ? 401 :
                     err.code === 'RATE_LIMITED'   ? 429 : 502;
      return res.status(status).json({ error: err.message, code: err.code });
    }
  } else {
    return res.status(400).json({ error: 'sourceMode must be "direct" or "github"' });
  }

  if (!abacContent) return res.status(400).json({ error: 'No ABAC SQL content resolved.' });

  try {
    const result = await generateOpaPolicy(abacContent, { schemaVariant, customPrompt });
    const ruleCount = countRules(result.regoPolicy);

    // Auto-save + manifest update when envId+policyKey provided
    if (envId && policyKey) {
      try {
        writePolicyByKey(envId, policyKey, result.regoPolicy);
        updateNodeStatus(envId, policyKey, {
          status:        'current',
          sha:           sourceRef.sha || null,
          ruleCount,
          lastGenerated: new Date().toISOString(),
        });
      } catch (saveErr) {
        console.warn('[opa-generate] Auto-save failed:', saveErr.message);
      }
    }

    return res.json({
      regoPolicy:  result.regoPolicy,
      builtPrompt: result.builtPrompt,
      sourceRef,
      tokenUsage:  result.tokenUsage,
      warning:     sourceRef.warning || null,
      mock:        result.mock,
      ruleCount,
    });
  } catch (err) {
    console.error('[opa-generate] LLM error:', err.message);
    return res.status(500).json({ error: `Policy generation failed: ${err.message}` });
  }
});

// ── GET /opa-policy/:envId/:policyKey  (env-aware) ───────────────────────────
router.get('/opa-policy/:envId/:policyKey', (req, res) => {
  const { envId, policyKey } = req.params;
  if (!isValidEnvId(envId)) return res.status(400).json({ error: 'Invalid envId' });

  try {
    const result = readPolicyByKey(envId, policyKey);
    if (!result) return res.status(404).json({ error: `No saved Rego found for "${policyKey}" in env "${envId}"` });
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── PUT /opa-policy/:envId/:policyKey  (env-aware) ───────────────────────────
router.put('/opa-policy/:envId/:policyKey', checkWriteAuth, (req, res) => {
  const { envId, policyKey } = req.params;
  const { rego } = req.body;

  if (!isValidEnvId(envId))           return res.status(400).json({ error: 'Invalid envId' });
  if (!rego?.trim())                   return res.status(400).json({ error: 'rego is required' });

  try {
    writePolicyByKey(envId, policyKey, rego);
    const ruleCount = countRules(rego);
    // Update manifest status
    updateNodeStatus(envId, policyKey, {
      status:        'current',
      ruleCount,
      lastGenerated: new Date().toISOString(),
    });
    res.json({ ok: true, ruleCount });
  } catch (err) {
    const status = err.message.includes('package') || err.message.includes('rule') ? 400 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ── Legacy single-policy routes (backward compat) ────────────────────────────

router.get('/opa-policies', (req, res) => {
  try { res.json({ policies: listPolicies() }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/opa-policy/:name', (req, res) => {
  const { name } = req.params;
  if (!name || name.length > 128 || !/^[\w\-]+$/.test(name))
    return res.status(400).json({ error: 'Invalid policy name' });
  try {
    const result = readPolicy(name);
    if (!result) return res.status(404).json({ error: `No policy found for "${name}"` });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.put('/opa-policy/:name', checkWriteAuth, (req, res) => {
  const { name } = req.params;
  const { rego } = req.body;
  if (!name || name.length > 128 || !/^[\w\-]+$/.test(name))
    return res.status(400).json({ error: 'Invalid policy name' });
  if (!rego?.trim()) return res.status(400).json({ error: 'rego is required' });
  try {
    writePolicy(name, rego);
    res.json({ ok: true });
  } catch (err) {
    const status = err.message.includes('package') || err.message.includes('rule') ? 400 : 500;
    res.status(status).json({ error: err.message });
  }
});

// ── Helpers ───────────────────────────────────────────────────────────────────

function isValidEnvId(envId) {
  return envId && envId.length <= 64 && /^[\w\-]+$/.test(envId);
}

module.exports = router;
