'use strict';

// Identity Audit routes — Entra ID transitive group membership lookup via
// natural-language prompts. See docs/adr/0016-identity-audit.md.

const express = require('express');
const { parsePrompt } = require('../services/identityAuditParserService');
const { getFilteredGroups } = require('../services/entraGraphService');
const { getIdentityAuditEnvironments, getEnvConfig } = require('../services/identityAuditConfigService');

const router = express.Router();

// ── GET /identity-audit-config ────────────────────────────────────────────────
// Mirrors permission-config/ranger-config: env IDs/labels only — never
// tenant/client/secret, which stay server-side.
router.get('/identity-audit-config', (req, res) => {
  res.json({ environments: getIdentityAuditEnvironments() });
});

// ── POST /identity-audit ──────────────────────────────────────────────────────
router.post('/identity-audit', async (req, res) => {
  const { prompt, envId } = req.body || {};

  if (!prompt || typeof prompt !== 'string' || !prompt.trim()) {
    return res.status(400).json({ error: 'prompt is required and must be a non-empty string' });
  }

  let upn;
  let filters;
  let mode;
  try {
    ({ upn, filters, mode } = await parsePrompt(prompt));
  } catch (err) {
    console.error('[identityAudit] Parsing failed:', err.message);
    return res.status(502).json({ error: 'Failed to parse the prompt', code: 'PARSE_ERROR' });
  }

  if (!upn) {
    return res.status(422).json({
      error: 'Could not extract a target user (UPN, email, or SAM account) from the prompt',
      code: 'EXTRACTION_INCOMPLETE',
      mode,
    });
  }

  try {
    const resolvedEnvId = (envId || 'DEFAULT').toUpperCase();
    const envConfig = getEnvConfig(resolvedEnvId);
    // Prompt-supplied filters take priority; fall back to this environment's
    // static default filters when the prompt named none.
    const effectiveFilters = filters && filters.length > 0 ? filters : envConfig.defaultFilters;

    const { groups, totalBeforeFilter, mock, resolvedUpn } = await getFilteredGroups(upn, effectiveFilters, envConfig);

    res.json({
      upn,
      ...(resolvedUpn ? { resolvedUpn } : {}),
      groups,
      totalBeforeFilter,
      filters: effectiveFilters,
      filterSource: filters && filters.length > 0 ? 'prompt' : (envConfig.defaultFilters.length > 0 ? 'config' : 'none'),
      mode,
      mock,
      envId: resolvedEnvId,
    });
  } catch (err) {
    console.error('[identityAudit] Graph lookup failed:', err.message);
    const status = err.code === 'UNKNOWN_ENV' ? 400 : 502;
    res.status(status).json({ error: err.message || 'Identity audit lookup failed', code: err.code || 'GRAPH_ERROR' });
  }
});

module.exports = router;
