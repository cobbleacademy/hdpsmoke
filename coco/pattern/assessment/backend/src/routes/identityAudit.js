'use strict';

// Identity Audit routes — Entra ID transitive group membership lookup via
// natural-language prompts. See docs/adr/0016-identity-audit.md.

const express = require('express');
const { parsePrompt } = require('../services/identityAuditParserService');
const { getFilteredGroups, searchAccounts } = require('../services/entraGraphService');
const { getIdentityAuditEnvironments, getEnvConfig } = require('../services/identityAuditConfigService');

const router = express.Router();

// ── GET /identity-audit-config ────────────────────────────────────────────────
// Mirrors permission-config/ranger-config: env IDs/labels only — never
// tenant/client/secret, which stay server-side.
router.get('/identity-audit-config', (req, res) => {
  res.json({ environments: getIdentityAuditEnvironments() });
});

// ── POST /identity-audit ──────────────────────────────────────────────────────
// Body is either { prompt, envId } (parses the prompt first) or
// { upn, filters, envId } (direct mode — used after the caller has already
// disambiguated a personName search down to one account; see below).
router.post('/identity-audit', async (req, res) => {
  const { prompt, envId, upn: directUpn, filters: directFilters } = req.body || {};

  let upn;
  let personName;
  let filters;
  let mode;

  if (directUpn) {
    upn = directUpn;
    filters = directFilters || [];
    mode = 'direct';
  } else {
    if (!prompt || typeof prompt !== 'string' || !prompt.trim()) {
      return res.status(400).json({ error: 'prompt is required and must be a non-empty string' });
    }

    try {
      ({ upn, personName, filters, mode } = await parsePrompt(prompt, { envId }));
    } catch (err) {
      console.error('[identityAudit] Parsing failed:', err.message);
      return res.status(502).json({ error: 'Failed to parse the prompt', code: 'PARSE_ERROR' });
    }

    if (!upn && !personName) {
      return res.status(422).json({
        error: 'Could not extract a target user (UPN, email, SAM account, or name) from the prompt',
        code: 'EXTRACTION_INCOMPLETE',
        mode,
      });
    }

    // Prompt named a person by display name, not an exact identifier — look up
    // candidate accounts and ask the caller to pick one instead of guessing.
    if (!upn && personName) {
      try {
        const resolvedEnvId = (envId || 'DEFAULT').toUpperCase();
        const envConfig = getEnvConfig(resolvedEnvId);
        const { accounts, mock } = await searchAccounts(personName, envConfig);
        return res.json({
          needsSelection: true,
          personName,
          accounts,
          filters,
          mode,
          mock,
          envId: resolvedEnvId,
        });
      } catch (err) {
        console.error('[identityAudit] Account search failed:', err.message);
        const status = err.code === 'UNKNOWN_ENV' ? 400 : 502;
        return res.status(status).json({ error: err.message || 'Account search failed', code: err.code || 'GRAPH_ERROR' });
      }
    }
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
