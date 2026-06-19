'use strict';

const express = require('express');
const router  = express.Router();

const { fetchRegoFile }      = require('../services/rangerGithubService');
const { generateRangerPolicy, buildRangerPrompt, normaliseRego } = require('../services/rangerService');
const {
  readManifest,
  upsertManifestEntry,
  removeManifestEntry,
  readPolicy,
  writePolicy,
  getRangerEnvironments,
  getEncryptionKey,
} = require('../services/rangerPersistService');

// ── Helpers ───────────────────────────────────────────────────────────────────

function isValidEnvId(id) {
  return typeof id === 'string' && /^[A-Za-z0-9_-]{1,32}$/.test(id);
}

function isValidPolicyKey(key) {
  return typeof key === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(key);
}

// ── GET /ranger-config ────────────────────────────────────────────────────────
router.get('/ranger-config', (req, res) => {
  res.json({
    rangerEnvironments:    getRangerEnvironments(),
    defaultOwner:          process.env.RANGER_GITHUB_OWNER          || process.env.GITHUB_DEFAULT_OWNER    || '',
    defaultRepo:           process.env.RANGER_GITHUB_REPO           || process.env.GITHUB_DEFAULT_REPO     || '',
    defaultBranch:         process.env.RANGER_GITHUB_BRANCH         || process.env.GITHUB_DEFAULT_BRANCH   || 'main',
    defaultFilePath:       process.env.RANGER_GITHUB_REGO_PATH      || '',
    defaultFetchMode:      process.env.RANGER_GITHUB_FETCH_MODE     || process.env.GITHUB_DEFAULT_FETCH_MODE || 'api',
    githubTokenConfigured: Boolean(process.env.GITHUB_TOKEN),
    encryptionEnabled:     Boolean(getEncryptionKey()),
    rangerLlmModel:        process.env.RANGER_LLM_MODEL || process.env.OPA_LLM_MODEL || process.env.OPENAI_MODEL || 'gpt-4o',
  });
});

// ── POST /ranger-fetch ────────────────────────────────────────────────────────
// Fetch a Rego file from GitHub and return its content.
router.post('/ranger-fetch', async (req, res) => {
  const {
    owner, repo, branch, filePath, fetchMode,
  } = req.body;

  if (!owner || !repo || !branch || !filePath) {
    return res.status(400).json({ error: 'owner, repo, branch and filePath are required' });
  }

  try {
    const result = await fetchRegoFile({
      owner,
      repo,
      branch,
      filePath,
      fetchMode: fetchMode || 'api',
      token: process.env.GITHUB_TOKEN || undefined,
    });
    res.json(result);
  } catch (err) {
    const status = err.code === 'FILE_NOT_FOUND' ? 404
                 : err.code === 'UNAUTHORIZED'   ? 401
                 : err.code === 'RATE_LIMITED'   ? 429
                 : 502;
    res.status(status).json({ error: err.message, code: err.code });
  }
});

// ── POST /ranger-generate ─────────────────────────────────────────────────────
// Clean Rego input, call LLM, return Ranger policy JSON + prompt.
router.post('/ranger-generate', async (req, res) => {
  const { regoCode, customPrompt, envId } = req.body;

  if (!regoCode || typeof regoCode !== 'string') {
    return res.status(400).json({ error: 'regoCode is required and must be a string' });
  }

  // Server-side cleanup — also validates package declaration
  let normalised;
  try {
    normalised = normaliseRego(regoCode);
  } catch (err) {
    return res.status(400).json({ error: err.message, code: 'INVALID_REGO' });
  }

  try {
    const result = await generateRangerPolicy(normalised, { customPrompt });
    res.json({
      rangerPolicies: result.rangerPolicies,
      builtPrompt:    result.builtPrompt,
      tokenUsage:     result.tokenUsage,
      mock:           result.mock,
      normalisedRego: normalised,
    });
  } catch (err) {
    console.error(
      `[ranger-generate] Failed: envId=${envId || 'n/a'} code=${err.code || 'LLM_ERROR'}`,
      `\n  message: ${err.message}`
    );
    res.status(502).json({ error: err.message, code: err.code || 'LLM_ERROR' });
  }
});

// ── GET /ranger-manifest/:envId ───────────────────────────────────────────────
router.get('/ranger-manifest/:envId', (req, res) => {
  const { envId } = req.params;
  if (!isValidEnvId(envId)) return res.status(400).json({ error: 'Invalid envId' });

  try {
    const manifest = readManifest(envId);
    // Annotate each entry with whether a stored policy file exists
    const policies = manifest.policies.map((p) => ({
      ...p,
      hasPolicy: Boolean(readPolicy(envId, p.policyKey)),
    }));
    res.json({ policies });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /ranger-policy/:envId/:policyKey ──────────────────────────────────────
router.get('/ranger-policy/:envId/:policyKey', (req, res) => {
  const { envId, policyKey } = req.params;
  if (!isValidEnvId(envId))       return res.status(400).json({ error: 'Invalid envId' });
  if (!isValidPolicyKey(policyKey)) return res.status(400).json({ error: 'Invalid policyKey' });

  try {
    const result = readPolicy(envId, policyKey);
    if (!result) return res.status(404).json({ error: 'Policy not found' });
    res.json({ policy: result.policy, encrypted: result.encrypted });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── PUT /ranger-policy/:envId/:policyKey ──────────────────────────────────────
// Save a generated (or manually edited) Ranger policy JSON.
router.put('/ranger-policy/:envId/:policyKey', (req, res) => {
  const { envId, policyKey } = req.params;
  if (!isValidEnvId(envId))        return res.status(400).json({ error: 'Invalid envId' });
  if (!isValidPolicyKey(policyKey)) return res.status(400).json({ error: 'Invalid policyKey' });

  const { policy, name: bodyName, serviceType: bodyServiceType, service: bodyService } = req.body;
  if (!policy || typeof policy !== 'object') {
    return res.status(400).json({ error: 'policy must be a non-null JSON object or array in request body' });
  }

  // Derive display metadata — policy may be a single object or an array of objects.
  // Fallback chain: LLM-generated field → modal-supplied body field → policyKey.
  const firstPolicy = Array.isArray(policy) ? policy[0] : policy;

  try {
    writePolicy(envId, policyKey, policy);
    upsertManifestEntry(envId, {
      policyKey,
      // Modal-supplied name takes priority over the Ranger JSON's internal name field.
      name:          bodyName          || firstPolicy?.name          || policyKey,
      serviceType:   bodyServiceType   || firstPolicy?.serviceType   || 'hive',
      service:       bodyService       || firstPolicy?.service       || '',
      lastGenerated: new Date().toISOString(),
    });
    res.json({ ok: true, policyKey, encrypted: Boolean(getEncryptionKey()) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── DELETE /ranger-policy/:envId/:policyKey ───────────────────────────────────
router.delete('/ranger-policy/:envId/:policyKey', (req, res) => {
  const { envId, policyKey } = req.params;
  if (!isValidEnvId(envId))        return res.status(400).json({ error: 'Invalid envId' });
  if (!isValidPolicyKey(policyKey)) return res.status(400).json({ error: 'Invalid policyKey' });

  try {
    removeManifestEntry(envId, policyKey);
    res.json({ ok: true, policyKey });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
