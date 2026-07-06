'use strict';

// Governance Lifecycle routes — runtime-editable content for the swimlane/
// RACI/SOP dashboard. See docs/adr/0017-governance-lifecycle.md.

const express = require('express');
const crypto  = require('crypto');
const { readConfig, writeConfig } = require('../services/governanceLifecycleConfigService');

const router = express.Router();

/**
 * Middleware: verify the Bearer token when PAYLOAD_WRITE_AUTH_ENABLED=true.
 * Same gate as Payload Library / OPA / Ranger's editors — one shared secret
 * reused across every content-editor feature in this app, not a new one.
 */
function checkWriteAuth(req, res, next) {
  if (process.env.PAYLOAD_WRITE_AUTH_ENABLED !== 'true') return next();
  const adminToken = process.env.PAYLOAD_ADMIN_TOKEN || '';
  if (!adminToken) {
    console.warn(
      '[governanceLifecycle] PAYLOAD_WRITE_AUTH_ENABLED=true but PAYLOAD_ADMIN_TOKEN is not set — ' +
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
    return res.status(401).json({
      error: 'Unauthorized: valid Bearer token required to write Governance Lifecycle content',
    });
  }
  next();
}

// ── GET /governance-lifecycle-config ──────────────────────────────────────────
// Returns the saved config, or the built-in default if nothing has been
// saved yet. Also reports whether write auth is required, mirroring
// provider-config's writeAuthRequired flag.
router.get('/governance-lifecycle-config', (req, res) => {
  try {
    const { config, usingDefault } = readConfig();
    const writeAuthRequired =
      process.env.PAYLOAD_WRITE_AUTH_ENABLED === 'true' && Boolean(process.env.PAYLOAD_ADMIN_TOKEN);
    res.json({ config, usingDefault, writeAuthRequired });
  } catch (err) {
    console.error('[governanceLifecycle] GET config failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── PUT /governance-lifecycle-config ──────────────────────────────────────────
// Body: { config: {...} }
// Validates cross-references and the "exactly one Accountable per RACI row"
// invariant before persisting — see governanceLifecycleConfigService.validateConfig.
router.put('/governance-lifecycle-config', checkWriteAuth, (req, res) => {
  const { config } = req.body || {};
  if (!config || typeof config !== 'object') {
    return res.status(400).json({ error: 'Request body must include a "config" object' });
  }

  try {
    writeConfig(config);
    res.json({ ok: true });
  } catch (err) {
    if (err.validationErrors) {
      return res.status(400).json({ error: err.message, validationErrors: err.validationErrors });
    }
    console.error('[governanceLifecycle] PUT config failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
