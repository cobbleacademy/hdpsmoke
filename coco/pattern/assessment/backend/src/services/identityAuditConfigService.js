'use strict';

// Identity Audit — per-environment settings, kept separate from the
// parsing/query logic (identityAuditParserService.js / entraGraphService.js)
// so config concerns live in exactly one place, mirroring
// permissionConfigService.mjs's separation for the Group Permission feature.
//
// IDENTITY_AUDIT_USE_LLM is intentionally its own flag — not shared with the
// Group Permission Checker's USE_LLM — because these are independent
// features that happen to both support an LLM/regex toggle; coupling their
// switches would mean flipping one always flips the other.

const DEFAULT_SCOPE = 'https://graph.microsoft.com/.default';

// "startsWith:AWS-,contains:Finance,endsWith:-Admin" → [{type:'startsWith',value:'AWS-'}, ...]
function parseFilterList(raw) {
  if (!raw || !raw.trim()) return [];
  return raw
    .split(',')
    .map((pair) => {
      const [type, value] = pair.split(':').map((s) => s?.trim());
      return type && value ? { type, value } : null;
    })
    .filter(Boolean);
}

/**
 * Returns the configured environment list — [{ id, label }] — safe to expose
 * to the browser (no tenant/client/secret). Mirrors getPermissionEnvironments().
 */
function getIdentityAuditEnvironments() {
  const raw = process.env.IDENTITY_AUDIT_ENVS || '';
  if (!raw.trim()) {
    return [{ id: 'DEFAULT', label: 'Default' }];
  }
  return raw.split(',').map((s) => {
    const id = s.trim().toUpperCase();
    return { id, label: id };
  });
}

/**
 * Resolves the full server-side-only config for one environment. Throws with
 * .code='UNKNOWN_ENV' if envId isn't in the configured list. Does NOT throw
 * when Entra credentials are missing — instead returns mock:true, so the
 * route layer can fall back to a deterministic mock Graph response (there is
 * no Entra-equivalent of "blank API key" failing the whole feature; this
 * mirrors how OPENAI_API_KEY blank → mock mode elsewhere in this app).
 */
function getEnvConfig(envId) {
  const id = (envId || 'DEFAULT').toUpperCase();
  const known = getIdentityAuditEnvironments().some((e) => e.id === id);
  if (!known) {
    const err = new Error(`Unknown Identity Audit environment: ${id}`);
    err.code = 'UNKNOWN_ENV';
    throw err;
  }

  const isDefault = id === 'DEFAULT';
  const prefix = isDefault ? 'IDENTITY_AUDIT' : `IDENTITY_AUDIT_${id.replace(/-/g, '_')}`;

  const tenantId     = process.env[`${prefix}_TENANT_ID`]     || '';
  const clientId     = process.env[`${prefix}_CLIENT_ID`]     || '';
  const clientSecret = process.env[`${prefix}_CLIENT_SECRET`] || '';
  const scope         = process.env[`${prefix}_SCOPE`]         || DEFAULT_SCOPE;
  const defaultFilters = parseFilterList(process.env[`${prefix}_DEFAULT_FILTERS`]);

  const mock = !(tenantId && clientId && clientSecret);

  return { tenantId, clientId, clientSecret, scope, defaultFilters, mock };
}

function isLlmEnabled() {
  return process.env.IDENTITY_AUDIT_USE_LLM === 'true';
}

function getLlmModel() {
  return process.env.IDENTITY_AUDIT_LLM_MODEL || process.env.OPENAI_MODEL || 'gpt-4o-mini';
}

module.exports = {
  getIdentityAuditEnvironments,
  getEnvConfig,
  isLlmEnabled,
  getLlmModel,
};
