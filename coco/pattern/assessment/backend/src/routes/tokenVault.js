'use strict';

const express = require('express');
const crypto  = require('crypto');
const router  = express.Router();

const {
  readCredentials,
  writeCredentials,
  redact,
  findCredential,
  getTokenVaultEnvironments,
  getEncryptionKey,
} = require('../services/tokenVaultPersistService');
const { fetchToken, clearCache } = require('../services/tokenVaultOAuthService');

// ── Helpers ───────────────────────────────────────────────────────────────────

function isValidEnvId(id) {
  return typeof id === 'string' && /^[A-Za-z0-9_-]{1,32}$/.test(id);
}

function isValidCredentialId(id) {
  return typeof id === 'string' && /^[A-Za-z0-9_-]{1,64}$/.test(id);
}

// Same write-auth gate as Payload Library / OPA Policy Library — reuses
// PAYLOAD_WRITE_AUTH_ENABLED / PAYLOAD_ADMIN_TOKEN, no new secret.
function checkWriteAuth(req, res, next) {
  if (process.env.PAYLOAD_WRITE_AUTH_ENABLED !== 'true') return next();
  const adminToken = process.env.PAYLOAD_ADMIN_TOKEN || '';
  if (!adminToken) {
    console.warn(
      '[tokenVault] PAYLOAD_WRITE_AUTH_ENABLED=true but PAYLOAD_ADMIN_TOKEN is not set — ' +
      'write auth is effectively disabled'
    );
    return next();
  }

  const authHeader = req.headers['authorization'] || '';
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';

  let valid = false;
  try {
    valid =
      token.length === adminToken.length &&
      crypto.timingSafeEqual(Buffer.from(token), Buffer.from(adminToken));
  } catch {
    valid = false;
  }

  if (!valid) {
    return res.status(401).json({ error: 'Unauthorized: valid Bearer token required to write Token Vault credentials' });
  }
  next();
}

// ── GET /token-vault-config ───────────────────────────────────────────────────
router.get('/token-vault-config', (req, res) => {
  res.json({
    tokenVaultEnvironments: getTokenVaultEnvironments(),
    writeAuthRequired:      process.env.PAYLOAD_WRITE_AUTH_ENABLED === 'true',
    encryptionEnabled:      Boolean(getEncryptionKey()),
  });
});

// ── GET /token-vault-manifest/:envId ──────────────────────────────────────────
router.get('/token-vault-manifest/:envId', (req, res) => {
  const { envId } = req.params;
  if (!isValidEnvId(envId)) return res.status(400).json({ error: 'Invalid envId' });

  try {
    const credentials = readCredentials(envId).map(redact);
    res.json({ credentials });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── PUT /token-vault-manifest/:envId ──────────────────────────────────────────
// Saves the full credential-set list for an environment (whole-file replace,
// same granularity as Payload Library's YAML editor). A credential set whose
// clientSecret is omitted keeps its previously stored secret — this is how
// the redacted list the UI already has round-trips back without forcing the
// operator to re-paste every secret on every edit.
router.put('/token-vault-manifest/:envId', checkWriteAuth, (req, res) => {
  const { envId } = req.params;
  if (!isValidEnvId(envId)) return res.status(400).json({ error: 'Invalid envId' });

  const { credentials } = req.body;
  if (!Array.isArray(credentials)) {
    return res.status(400).json({ error: 'credentials must be an array in the request body' });
  }
  for (const c of credentials) {
    if (!isValidCredentialId(c.id)) {
      return res.status(400).json({ error: `Invalid credential id: ${c.id}` });
    }
    if (!c.displayName || !c.tokenUrl || !c.clientId) {
      return res.status(400).json({ error: `Credential "${c.id}" is missing displayName, tokenUrl, or clientId` });
    }
  }

  try {
    const existing = readCredentials(envId);
    const merged = credentials.map((c) => {
      if (c.clientSecret) return c;
      const prior = existing.find((e) => e.id === c.id);
      return { ...c, clientSecret: prior?.clientSecret || '' };
    });
    writeCredentials(envId, merged);
    clearCache(); // credentials may have changed — drop all cached tokens for safety
    res.json({ ok: true, credentials: merged.map(redact), encrypted: Boolean(getEncryptionKey()) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /token-vault-generate/:envId/:credentialId ───────────────────────────
router.post('/token-vault-generate/:envId/:credentialId', async (req, res) => {
  const { envId, credentialId } = req.params;
  if (!isValidEnvId(envId))            return res.status(400).json({ error: 'Invalid envId' });
  if (!isValidCredentialId(credentialId)) return res.status(400).json({ error: 'Invalid credentialId' });

  let credential;
  try {
    credential = findCredential(envId, credentialId);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
  if (!credential) return res.status(404).json({ error: 'Credential set not found' });

  try {
    const token = await fetchToken(credentialId, credential);
    res.json(token);
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

module.exports = router;
