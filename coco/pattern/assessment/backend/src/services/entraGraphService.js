'use strict';

// Identity Audit — Microsoft Graph client: transitive group membership,
// pagination, and filter application.
//
// Real path: GET /v1.0/users/{upn}/transitiveMemberOf already resolves
// nested/transitive membership server-side in Graph — this module's job is
// auth (via the shared entraAuthService), following @odata.nextLink until
// exhausted, and filtering the result. There is no graph-traversal logic
// here; Graph does that.
//
// Mock path: when an environment has no Entra credentials configured
// (identityAuditConfigService.getEnvConfig().mock === true), a deterministic
// mock group list is generated instead — same "blank credential → mock"
// convention used everywhere else in this app (OPENAI_API_KEY, the HSM demo,
// etc.). The mock path does NOT exercise the real pagination loop below
// (there's no fake multi-page HTTP server here) — only the live Graph path
// does. Treat the pagination loop as code-reviewed, not load-tested, until
// it's run against a real tenant.

const entraAuth = require('./entraAuthService');

const GRAPH_BASE = 'https://graph.microsoft.com/v1.0';

// ── Filter application ───────────────────────────────────────────────────────

function matchesFilter(displayName, filter) {
  const name = (displayName || '').toLowerCase();
  const value = (filter.value || '').toLowerCase();
  if (filter.type === 'startsWith') return name.startsWith(value);
  if (filter.type === 'endsWith')   return name.endsWith(value);
  if (filter.type === 'contains')   return name.includes(value);
  return true; // unknown filter type — fail open on filtering, not on access
}

// OR semantics: a group passes if it matches ANY supplied filter. Empty/absent
// filters → no filtering, return everything.
function applyFilters(groups, filters) {
  if (!filters || filters.length === 0) return groups;
  return groups.filter((g) => filters.some((f) => matchesFilter(g.displayName, f)));
}

// ── Real Graph path ───────────────────────────────────────────────────────────

// SAM accounts (no '@') cannot be used directly in the transitiveMemberOf URL.
// Resolve them first via mailNickname — the Azure AD attribute that stores the
// Windows SAM-compatible logon name. Throws with code SAM_NOT_FOUND when the
// mailNickname doesn't match any user in the tenant.
async function resolveSamToUpn(sam, token) {
  const url = `${GRAPH_BASE}/users?$filter=mailNickname eq '${encodeURIComponent(sam)}'&$select=userPrincipalName&$top=1`;
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Graph mailNickname lookup failed HTTP ${resp.status}: ${text.slice(0, 200)}`);
  }
  const data = await resp.json();
  if (!data.value || data.value.length === 0) {
    throw Object.assign(
      new Error(`SAM account '${sam}' not found — no user with mailNickname='${sam}' in this tenant`),
      { code: 'SAM_NOT_FOUND' }
    );
  }
  return data.value[0].userPrincipalName;
}

async function fetchTransitiveGroupsLive(userIdentifier, envConfig) {
  const token = await entraAuth.fetchEntraToken(
    { tenantId: envConfig.tenantId, clientId: envConfig.clientId, clientSecret: envConfig.clientSecret, scope: envConfig.scope },
    'identityAuditService'
  );

  // Resolve SAM account to UPN if the identifier has no '@'
  let resolvedUpn = userIdentifier;
  if (!userIdentifier.includes('@')) {
    resolvedUpn = await resolveSamToUpn(userIdentifier, token);
  }

  const groups = [];
  let url = `${GRAPH_BASE}/users/${encodeURIComponent(resolvedUpn)}/transitiveMemberOf?$select=id,displayName`;

  while (url) {
    const resp = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`Graph API request failed HTTP ${resp.status}: ${text.slice(0, 300)}`);
    }
    const data = await resp.json();
    for (const item of data.value || []) {
      if (item.displayName) groups.push({ id: item.id, displayName: item.displayName });
    }
    url = data['@odata.nextLink'] || null; // exhaustive retrieval — loop until Graph stops paging
  }

  return { groups, resolvedUpn: resolvedUpn !== userIdentifier ? resolvedUpn : null };
}

// ── Mock path ─────────────────────────────────────────────────────────────────
// Deterministic per-UPN (same UPN always returns the same mock groups) so the
// demo behaves consistently across repeated calls and across LLM vs regex
// parsing of the same underlying request.

const MOCK_GROUP_POOL = [
  'AWS-Admins', 'AWS-ReadOnly', 'AWS-Billing',
  'Finance-Team', 'Finance-Approvers', 'SharePoint-Finance',
  'IT-Helpdesk-Admin', 'Security-Auditors', 'Global-Admins',
  'VPN-Users', 'Engineering', 'External-Contractors-Admin',
  'Deployment-Prod', 'Database-Prod', 'Release-Prod',
];

function hashString(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (h * 31 + str.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

function buildMockGroups(upn) {
  const seed = hashString(upn || 'unknown');
  // Deterministic subset: every UPN gets 4-7 groups from the pool, selection
  // and ordering derived from the UPN's hash so it's stable across calls.
  const count = 4 + (seed % 4);
  const groups = [];
  for (let i = 0; i < count; i++) {
    const idx = (seed + i * 7) % MOCK_GROUP_POOL.length;
    const displayName = MOCK_GROUP_POOL[idx];
    if (!groups.some((g) => g.displayName === displayName)) {
      groups.push({ id: `mock-${idx}-${seed}`, displayName });
    }
  }
  return groups;
}

// ── Public entry point ──────────────────────────────────────────────────────────

/**
 * Returns { groups, totalBeforeFilter, mock } for upn in the given
 * environment, with `filters` (OR semantics) already applied.
 */
async function getFilteredGroups(upn, filters, envConfig) {
  let allGroups;
  let resolvedUpn = null;

  if (envConfig.mock) {
    allGroups = buildMockGroups(upn);
  } else {
    const result = await fetchTransitiveGroupsLive(upn, envConfig);
    allGroups = result.groups;
    resolvedUpn = result.resolvedUpn; // set only when SAM → UPN resolution occurred
  }

  const groups = applyFilters(allGroups, filters);
  return { groups, totalBeforeFilter: allGroups.length, mock: envConfig.mock, resolvedUpn };
}

module.exports = { getFilteredGroups };
