'use strict';

// ── Generic OAuth2 client-credentials token fetch ─────────────────────────────
// One engine for every provider (Entra ID, ForgeRock, or any other vendor) —
// each credential set is just a tokenUrl + clientId/clientSecret + an open
// extraParams bag (grant_type, scope, resource, realm, whatever that vendor
// needs), POSTed as application/x-www-form-urlencoded. No per-provider code:
// Entra's own client-credentials contract (POST tokenUrl with grant_type,
// client_id, client_secret, scope all in the body) is just the post-body case
// below with no extra assumptions layered on top.
//
// authMethod:
//   'post-body' (default) — client_id/client_secret included in the form body
//   'basic'                — client_id/client_secret sent via HTTP Basic auth,
//                             extraParams only in the body

const cache = new Map(); // credentialId -> { token, expiresAt }

// Refresh this many seconds before actual expiry, so a token handed back to
// the UI doesn't die moments after being copied.
const EXPIRY_SAFETY_MARGIN_SECONDS = 30;

function buildRequest(credential) {
  const { tokenUrl, clientId, clientSecret, authMethod, extraParams } = credential;
  if (!tokenUrl) throw new Error('Credential set is missing tokenUrl');
  if (!clientId) throw new Error('Credential set is missing clientId');
  if (!clientSecret) throw new Error('Credential set is missing clientSecret');

  const headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
  const params = new URLSearchParams();

  Object.entries(extraParams || {}).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') params.set(key, String(value));
  });

  if (authMethod === 'basic') {
    headers.Authorization = `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`;
  } else {
    params.set('client_id', clientId);
    params.set('client_secret', clientSecret);
  }

  return { tokenUrl, headers, body: params };
}

/**
 * Fetch (or return a cached) bearer token for a stored credential set.
 * credential must include the real clientSecret — callers get this from
 * tokenVaultPersistService.findCredential(), never from a redacted response.
 */
async function fetchToken(credentialId, credential) {
  const cached = cache.get(credentialId);
  if (cached && cached.expiresAt > Date.now()) {
    return { ...cached.token, cached: true };
  }

  const { tokenUrl, headers, body } = buildRequest(credential);

  let res;
  try {
    res = await fetch(tokenUrl, { method: 'POST', headers, body });
  } catch (err) {
    throw new Error(`Could not reach token endpoint: ${err.message}`);
  }

  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`Token endpoint returned non-JSON response (status ${res.status}): ${text.slice(0, 200)}`);
  }

  if (!res.ok) {
    const detail = data.error_description || data.error || text.slice(0, 200);
    throw new Error(`Token endpoint returned ${res.status}: ${detail}`);
  }

  if (!data.access_token) {
    throw new Error('Token endpoint response did not include an access_token');
  }

  const token = {
    access_token: data.access_token,
    token_type:   data.token_type || 'Bearer',
    expires_in:   data.expires_in ?? null,
  };

  if (typeof data.expires_in === 'number') {
    const ttlMs = Math.max(0, (data.expires_in - EXPIRY_SAFETY_MARGIN_SECONDS) * 1000);
    cache.set(credentialId, { token, expiresAt: Date.now() + ttlMs });
  } else {
    cache.delete(credentialId); // unknown TTL — don't cache, always fetch fresh
  }

  return { ...token, cached: false };
}

function clearCache(credentialId) {
  if (credentialId) cache.delete(credentialId);
  else cache.clear();
}

module.exports = { fetchToken, clearCache };
