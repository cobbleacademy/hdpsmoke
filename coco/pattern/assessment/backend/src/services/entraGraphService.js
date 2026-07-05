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
// Resolve them first via onPremisesSamAccountName — the Graph attribute that
// holds the on-prem AD SAM account name synced into Entra (NOT mailNickname,
// which is an unrelated email-alias attribute and would silently match the
// wrong users or none at all). Throws with code SAM_NOT_FOUND when no user
// in the tenant has that SAM account.
//
// onPremisesSamAccountName is one of Graph's "advanced query" properties —
// filtering on it requires the ConsistencyLevel: eventual header plus
// $count=true, or Graph returns HTTP 400. See:
// https://learn.microsoft.com/graph/aad-advanced-queries
async function resolveSamToUpn(sam, token) {
  const url = `${GRAPH_BASE}/users?$filter=onPremisesSamAccountName eq '${encodeURIComponent(sam)}'&$select=userPrincipalName&$count=true`;
  const resp = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      ConsistencyLevel: 'eventual',
    },
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Graph onPremisesSamAccountName lookup failed HTTP ${resp.status}: ${text.slice(0, 200)}`);
  }
  const data = await resp.json();
  if (!data.value || data.value.length === 0) {
    throw Object.assign(
      new Error(`SAM account '${sam}' not found — no user with onPremisesSamAccountName='${sam}' in this tenant`),
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

// ── Real Graph path: search accounts by display name ──────────────────────────
// Used when the prompt names a person by display name rather than an exact
// UPN/SAM ("show groups for John Smith") — the caller must disambiguate among
// the returned candidates before a group lookup can proceed.
async function searchAccountsByNameLive(name, envConfig) {
  const token = await entraAuth.fetchEntraToken(
    { tenantId: envConfig.tenantId, clientId: envConfig.clientId, clientSecret: envConfig.clientSecret, scope: envConfig.scope },
    'identityAuditService'
  );
  const url = `${GRAPH_BASE}/users?$filter=startswith(displayName,'${encodeURIComponent(name)}')&$select=id,displayName,userPrincipalName&$count=true&$top=10`;
  const resp = await fetch(url, {
    headers: { Authorization: `Bearer ${token}`, ConsistencyLevel: 'eventual' },
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Graph displayName search failed HTTP ${resp.status}: ${text.slice(0, 200)}`);
  }
  const data = await resp.json();
  return (data.value || []).map((u) => ({ id: u.id, displayName: u.displayName, userPrincipalName: u.userPrincipalName }));
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

// Deterministic 1-3 fake accounts per searched name, so picking any one of
// them then re-running buildMockGroups() on its UPN is stable across calls —
// same reasoning as buildMockGroups() above.
function buildMockAccounts(name) {
  const seed = hashString(name.toLowerCase());
  const count = 1 + (seed % 3);
  const localPart = name.trim().toLowerCase().replace(/\s+/g, '.');
  const accounts = [];
  for (let i = 0; i < count; i++) {
    const suffix = i === 0 ? '' : String(i + 1);
    accounts.push({
      id: `mock-user-${seed}-${i}`,
      displayName: i === 0 ? name.trim() : `${name.trim()} ${i + 1}`,
      userPrincipalName: `${localPart}${suffix}@contoso.com`,
    });
  }
  return accounts;
}

// ── Public entry points ──────────────────────────────────────────────────────────

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

/**
 * Returns { accounts, mock } — candidate accounts matching a display-name
 * search, for the caller to disambiguate before calling getFilteredGroups().
 */
async function searchAccounts(name, envConfig) {
  if (envConfig.mock) {
    return { accounts: buildMockAccounts(name), mock: true };
  }
  const accounts = await searchAccountsByNameLive(name, envConfig);
  return { accounts, mock: false };
}

module.exports = { getFilteredGroups, searchAccounts };
