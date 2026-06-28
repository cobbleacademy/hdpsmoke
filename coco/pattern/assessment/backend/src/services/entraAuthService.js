'use strict';

// Shared Microsoft Entra ID (Azure AD) OAuth2 client-credentials token fetch.
//
// Extracted out of providerService.js so the Identity Audit feature (which
// also needs an Entra token, but for the Microsoft Graph API rather than
// APIGEE) doesn't duplicate this logic. Both features call fetchEntraToken();
// neither owns its own copy.

// Tokens are valid for ~3600 s; we refresh when < 5 min remain.
// Keyed by "<tenantId>:<clientId>:<scope>" — the scope is part of the key
// because the same app registration can request tokens for different
// resources (e.g. an APIGEE-fronted API vs. https://graph.microsoft.com/.default)
// and those tokens are not interchangeable.
const _tokenCache = new Map(); // key → { token: string, expiresAt: number }

/**
 * Fetch (or return cached) an Entra OAuth2 client-credentials token.
 * @param {{ tenantId: string, clientId: string, clientSecret: string, scope: string }} creds
 * @param {string} [callerLabel]  Used only in error messages, e.g. "providerService" or "identityAuditService".
 */
async function fetchEntraToken(creds, callerLabel = 'entraAuthService') {
  const { tenantId, clientId, clientSecret, scope } = creds;

  if (!tenantId || !clientId || !clientSecret) {
    throw new Error(
      `[${callerLabel}] Entra auth requires tenantId, clientId, and clientSecret to all be set`
    );
  }

  const cacheKey = `${tenantId}:${clientId}:${scope || ''}`;
  const now = Date.now();
  const cached = _tokenCache.get(cacheKey);
  if (cached && cached.expiresAt - now > 5 * 60 * 1000) {
    return cached.token;
  }

  const tokenUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;
  const body = new URLSearchParams({
    client_id:     clientId,
    client_secret: clientSecret,
    scope:         scope,
    grant_type:    'client_credentials',
  });

  const resp = await fetch(tokenUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(
      `[${callerLabel}] Entra token fetch failed HTTP ${resp.status}: ${text.slice(0, 300)}`
    );
  }

  const data = await resp.json();
  _tokenCache.set(cacheKey, {
    token:     data.access_token,
    expiresAt: now + data.expires_in * 1000,
  });
  return data.access_token;
}

module.exports = { fetchEntraToken };
