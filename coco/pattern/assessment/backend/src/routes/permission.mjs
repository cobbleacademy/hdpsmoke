// Group Permission Evaluation routes.
//
// ES module by design — see docs/adr/0015-group-permission-evaluation.md.
// Mounted from server.js via a small async import() wrapper since server.js
// itself is CommonJS.

import express from 'express';
import { parsePrompt } from '../services/permissionParserService.mjs';
import { checkPermission } from '../services/permissionDb.mjs';
import { getPermissionEnvironments } from '../services/permissionConfigService.mjs';

const router = express.Router();

// ── GET /permission-config ───────────────────────────────────────────────────
// Mirrors ranger-config/opa-config: returns only env IDs/labels — never
// connection strings or schema mappings, which stay server-side only.
router.get('/permission-config', (req, res) => {
  res.json({ environments: getPermissionEnvironments() });
});

// ── POST /check-permission ───────────────────────────────────────────────────
// Mounted under /api/pattern/assessment (see server.js) so it's reachable
// through the same Istio route as every other endpoint in this app — there is
// no separate VirtualService for a bare /api/check-permission path.
router.post('/check-permission', async (req, res) => {
  const { prompt, envId } = req.body || {};

  if (!prompt || typeof prompt !== 'string' || !prompt.trim()) {
    return res.status(400).json({ error: 'prompt is required and must be a non-empty string' });
  }

  let userPrincipalName;
  let groupId;
  let mode;
  try {
    ({ userPrincipalName, groupId, mode } = await parsePrompt(prompt));
  } catch (err) {
    console.error('[permission] Parsing failed:', err.message);
    return res.status(502).json({ error: 'Failed to parse the prompt', code: 'PARSE_ERROR' });
  }

  if (!userPrincipalName || !groupId) {
    return res.status(422).json({
      error: 'Could not extract both a user email and a group ID from the prompt',
      code: 'EXTRACTION_INCOMPLETE',
      extracted: { userPrincipalName, groupId },
      mode,
    });
  }

  try {
    const resolvedEnvId = (envId || 'DEFAULT').toUpperCase();
    const { status, userLocation, groupLocation } = await checkPermission(userPrincipalName, groupId, resolvedEnvId);
    res.json({ status, userPrincipalName, groupId, userLocation, groupLocation, mode, envId: resolvedEnvId });
  } catch (err) {
    console.error('[permission] Database query failed:', err.message);
    const status = err.code === 'UNKNOWN_ENV' ? 400 : 500;
    res.status(status).json({ error: err.message || 'Permission lookup failed', code: err.code || 'DB_ERROR' });
  }
});

export default router;
