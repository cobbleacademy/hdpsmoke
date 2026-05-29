'use strict';

// ── In-memory Entra token cache ───────────────────────────────────────────────
// Tokens are valid for ~3600 s; we refresh when < 5 min remain.
let _entraCache = null; // { token: string, expiresAt: number (epoch ms) }

async function fetchEntraToken() {
  const now = Date.now();
  if (_entraCache && _entraCache.expiresAt - now > 5 * 60 * 1000) {
    return _entraCache.token;
  }

  const tenantId = process.env.PROVIDER_ENTRA_TENANT_ID;
  const clientId = process.env.PROVIDER_ENTRA_CLIENT_ID;
  const clientSecret = process.env.PROVIDER_ENTRA_CLIENT_SECRET;
  const scope =
    process.env.PROVIDER_ENTRA_SCOPE || 'https://graph.microsoft.com/.default';

  if (!tenantId || !clientId || !clientSecret) {
    throw new Error(
      'PROVIDER_AUTH_TYPE=entraid-apigee but one or more of ' +
        'PROVIDER_ENTRA_TENANT_ID / CLIENT_ID / CLIENT_SECRET is not set'
    );
  }

  const tokenUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;
  const body = new URLSearchParams({
    client_id: clientId,
    client_secret: clientSecret,
    scope,
    grant_type: 'client_credentials',
  });

  const resp = await fetch(tokenUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(
      `Entra token fetch failed HTTP ${resp.status}: ${text.slice(0, 300)}`
    );
  }

  const data = await resp.json();
  _entraCache = {
    token: data.access_token,
    expiresAt: now + data.expires_in * 1000,
  };
  return _entraCache.token;
}

// ── Auth header builder ───────────────────────────────────────────────────────

/**
 * Build auth headers for a single request.
 * @param {string} authType  One of: 'none' | 'api-key' | 'entraid-apigee' | 'payload-embedded'
 *                           Passed per-request from the frontend dropdown.
 */
async function buildAuthHeaders(authType) {
  const mode = (authType || 'none').toLowerCase();

  if (mode === 'api-key') {
    const key = process.env.PROVIDER_API_KEY || '';
    if (!key) {
      console.warn('[providerService] authType=api-key but PROVIDER_API_KEY is empty');
    }
    return { 'X-API-Key': key };
  }

  if (mode === 'entraid-apigee') {
    const token = await fetchEntraToken();
    const apiKey = process.env.PROVIDER_X_APIKEY || '';
    const apiSecret = process.env.PROVIDER_X_APISECRET || '';
    if (!apiKey) {
      console.warn('[providerService] PROVIDER_AUTH_TYPE=entraid-apigee but PROVIDER_X_APIKEY is empty');
    }
    if (!apiSecret) {
      console.warn('[providerService] PROVIDER_AUTH_TYPE=entraid-apigee but PROVIDER_X_APISECRET is empty');
    }
    return {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
      'x-apikey': apiKey,
      'x-apisecret': apiSecret,
    };
  }

  // 'payload-embedded' or 'none' — no extra headers; credentials are in the payload
  return {};
}


// ── Main call ─────────────────────────────────────────────────────────────────

/**
 * POST payload JSON to a provider URL.
 *
 * @param {string} url            Full HTTPS endpoint URL
 * @param {object|string} payload Already-parsed JSON object (or raw string fallback)
 * @param {string} [authType]     Auth mode for this request: 'none'|'api-key'|'entraid-apigee'|'payload-embedded'
 *                                Falls back to PROVIDER_AUTH_TYPE env var if omitted.
 * @returns {{ status: number, body: any, durationMs: number }}
 * @throws Error with .code = 'TIMEOUT' | 'NETWORK' on hard failures
 */
async function callProvider(url, payload, authType) {
  const resolvedAuthType = authType || process.env.PROVIDER_AUTH_TYPE || 'none';
  const timeoutMs = Math.max(
    10_000,
    parseInt(process.env.PROVIDER_TIMEOUT_MS || '15000', 10) || 15_000
  );

  const authHeaders = await buildAuthHeaders(resolvedAuthType);

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  const startMs = Date.now();

  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...authHeaders,
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    const durationMs = Date.now() - startMs;

    // Parse body — try JSON first, fall back to text
    let body;
    const ct = resp.headers.get('content-type') || '';
    if (ct.includes('application/json')) {
      try {
        body = await resp.json();
      } catch {
        body = await resp.text();
      }
    } else {
      body = await resp.text();
    }

    return { status: resp.status, body, durationMs };
  } catch (err) {
    clearTimeout(timeoutId);
    const durationMs = Date.now() - startMs;
    if (err.name === 'AbortError') {
      const secs = Math.round(timeoutMs / 1000);
      const e = new Error(`Request timed out after ${secs}s`);
      e.code = 'TIMEOUT';
      e.durationMs = durationMs;
      throw e;
    }
    err.code = err.code || 'NETWORK';
    err.durationMs = durationMs;
    throw err;
  }
}

// ── Config helper ─────────────────────────────────────────────────────────────

/**
 * Parse PROVIDER_URLS + PROVIDER_URL_LABELS into an array of { label, url }.
 * Returns [] if the env var is not set.
 */
function getConfiguredUrls() {
  const rawUrls = process.env.PROVIDER_URLS || '';
  const rawLabels = process.env.PROVIDER_URL_LABELS || '';

  const urls = rawUrls
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  const labels = rawLabels
    .split(',')
    .map((s) => s.trim());

  return urls.map((url, i) => ({
    label: labels[i] || `Provider ${i + 1}`,
    url,
  }));
}

module.exports = { callProvider, getConfiguredUrls };
