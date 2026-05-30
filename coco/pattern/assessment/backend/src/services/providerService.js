'use strict';

// ── Per-environment credential resolver ───────────────────────────────────────

/**
 * Resolve auth credentials for a given environment.
 *
 * In tab mode the frontend sends envId (e.g. "ADM-DEV") with every run-payload
 * request. The key is normalised to uppercase with non-alphanumeric chars replaced
 * by underscores ("ADM-DEV" → "ADM_DEV"), then the per-env var is tried first:
 *
 *   PROVIDER_ADM_DEV_API_KEY  →  fallback  →  PROVIDER_API_KEY
 *
 * In legacy mode (envId is null/undefined) only the global vars are used.
 * This keeps existing deployments that have not set per-env vars fully working.
 *
 * @param {string|null} envId  Raw environment ID ("ADM-DEV", "PROD", …) or null
 * @returns {{ apiKey, entraTenantId, entraClientId, entraClientSecret,
 *             entraScope, xApiKey, xApiSecret }}
 */
function getConfiguredCredentials(envId) {
  const key = envId
    ? envId.toUpperCase().replace(/[^A-Z0-9]/g, '_')
    : null;

  // Try per-env var first; fall back to global var.
  const get = (perEnvSuffix, globalVar) =>
    (key && process.env[`PROVIDER_${key}_${perEnvSuffix}`]) ||
    process.env[globalVar] ||
    '';

  return {
    apiKey:            get('API_KEY',            'PROVIDER_API_KEY'),
    entraTenantId:     get('ENTRA_TENANT_ID',    'PROVIDER_ENTRA_TENANT_ID'),
    entraClientId:     get('ENTRA_CLIENT_ID',    'PROVIDER_ENTRA_CLIENT_ID'),
    entraClientSecret: get('ENTRA_CLIENT_SECRET','PROVIDER_ENTRA_CLIENT_SECRET'),
    entraScope:        get('ENTRA_SCOPE',        'PROVIDER_ENTRA_SCOPE')
                       || 'https://graph.microsoft.com/.default',
    xApiKey:           get('X_APIKEY',           'PROVIDER_X_APIKEY'),
    xApiSecret:        get('X_APISECRET',        'PROVIDER_X_APISECRET'),
  };
}

// ── In-memory Entra token cache ───────────────────────────────────────────────
// Tokens are valid for ~3600 s; we refresh when < 5 min remain.
// Keyed by "<tenantId>:<clientId>" so environments with different app registrations
// maintain independent caches; environments sharing the same registration reuse
// the same cached token without an extra fetch.
const _entraTokenCache = new Map(); // key → { token: string, expiresAt: number }

/**
 * Fetch (or return cached) an Entra OAuth2 client-credentials token.
 * @param {{ entraTenantId, entraClientId, entraClientSecret, entraScope }} creds
 */
async function fetchEntraToken(creds) {
  const { entraTenantId, entraClientId, entraClientSecret, entraScope } = creds;

  if (!entraTenantId || !entraClientId || !entraClientSecret) {
    throw new Error(
      'authType=entraid-apigee but one or more of ' +
        'ENTRA_TENANT_ID / ENTRA_CLIENT_ID / ENTRA_CLIENT_SECRET is not set ' +
        '(check per-env PROVIDER_{ENV}_ENTRA_* vars or the global PROVIDER_ENTRA_* fallbacks)'
    );
  }

  const cacheKey = `${entraTenantId}:${entraClientId}`;
  const now = Date.now();
  const cached = _entraTokenCache.get(cacheKey);
  if (cached && cached.expiresAt - now > 5 * 60 * 1000) {
    return cached.token;
  }

  const tokenUrl = `https://login.microsoftonline.com/${entraTenantId}/oauth2/v2.0/token`;
  const body = new URLSearchParams({
    client_id:     entraClientId,
    client_secret: entraClientSecret,
    scope:         entraScope,
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
      `Entra token fetch failed HTTP ${resp.status}: ${text.slice(0, 300)}`
    );
  }

  const data = await resp.json();
  _entraTokenCache.set(cacheKey, {
    token:     data.access_token,
    expiresAt: now + data.expires_in * 1000,
  });
  return data.access_token;
}

// ── Auth header builder ───────────────────────────────────────────────────────

/**
 * Build auth headers for a single request.
 *
 * @param {string} authType  One of: 'none' | 'api-key' | 'entraid-apigee' | 'payload-embedded'
 * @param {{ apiKey, entraTenantId, entraClientId, entraClientSecret,
 *           entraScope, xApiKey, xApiSecret }} creds  Resolved credentials for this env.
 */
async function buildAuthHeaders(authType, creds) {
  const mode = (authType || 'none').toLowerCase();

  if (mode === 'api-key') {
    if (!creds.apiKey) {
      console.warn('[providerService] authType=api-key but API_KEY credential is empty');
    }
    return { 'X-API-Key': creds.apiKey };
  }

  if (mode === 'entraid-apigee') {
    const token = await fetchEntraToken(creds);
    if (!creds.xApiKey) {
      console.warn('[providerService] authType=entraid-apigee but X_APIKEY credential is empty');
    }
    if (!creds.xApiSecret) {
      console.warn('[providerService] authType=entraid-apigee but X_APISECRET credential is empty');
    }
    return {
      Authorization:    `Bearer ${token}`,
      'Content-Type':   'application/json',
      'x-apikey':       creds.xApiKey,
      'x-apisecret':    creds.xApiSecret,
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
 * @param {string} [authType]     Auth mode: 'none'|'api-key'|'entraid-apigee'|'payload-embedded'
 *                                Falls back to PROVIDER_AUTH_TYPE env var if omitted.
 * @param {string|null} [envId]   Environment ID from the frontend tab (e.g. "ADM-DEV").
 *                                null/undefined → use global credential env vars (legacy mode).
 * @returns {{ status: number, body: any, durationMs: number }}
 * @throws Error with .code = 'TIMEOUT' | 'NETWORK' on hard failures
 */
async function callProvider(url, payload, authType, envId) {
  const resolvedAuthType = authType || process.env.PROVIDER_AUTH_TYPE || 'none';
  const timeoutMs = Math.max(
    10_000,
    parseInt(process.env.PROVIDER_TIMEOUT_MS || '15000', 10) || 15_000
  );

  const creds = getConfiguredCredentials(envId || null);
  const authHeaders = await buildAuthHeaders(resolvedAuthType, creds);

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

    // Read the body as text exactly once.
    // Calling resp.json() and falling back to resp.text() in the catch would
    // throw "Body is unusable: Body has already been read" because the fetch
    // stream is consumed by the failed json() attempt — masking the real
    // provider error with a confusing internal message.
    const rawText = await resp.text();
    const durationMs = Date.now() - startMs;

    const ct = resp.headers.get('content-type') || '';
    let body;
    if (ct.includes('application/json')) {
      try {
        body = JSON.parse(rawText);
      } catch {
        // Content-Type said JSON but body wasn't valid — return raw text so
        // the caller still sees the actual provider response rather than nothing.
        body = rawText;
      }
    } else {
      body = rawText;
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

// ── Config helpers ────────────────────────────────────────────────────────────

/**
 * Parse PROVIDER_URLS + PROVIDER_URL_LABELS into an array of { label, url }.
 * Used only in legacy (no-tab) mode when PROVIDER_ENVS is not set.
 * Returns [] if the env var is not set.
 */
function getConfiguredUrls() {
  const rawUrls   = process.env.PROVIDER_URLS   || '';
  const rawLabels = process.env.PROVIDER_URL_LABELS || '';

  const urls   = rawUrls.split(',').map((s) => s.trim()).filter(Boolean);
  const labels = rawLabels.split(',').map((s) => s.trim());

  return urls.map((url, i) => ({
    label: labels[i] || `Provider ${i + 1}`,
    url,
  }));
}

/**
 * Parse PROVIDER_ENVS + PROVIDER_ENV_PAYLOAD_FILES + per-env URL triplets
 * into an array of environment objects for the tab-based UI.
 *
 * Each environment uses three env vars keyed by its uppercased ID:
 *   PROVIDER_{ID}_URLS            — comma CSV of endpoint URLs
 *   PROVIDER_{ID}_URL_LABELS      — comma CSV of display labels (optional)
 *   PROVIDER_{ID}_URL_AUTH_TYPES  — comma CSV of auth types per URL (optional)
 *
 * Returns null when PROVIDER_ENVS is not set → caller falls back to legacy mode.
 *
 * @returns {Array<{id, label, payloadFile, urls: Array<{label, url, authType}>}>|null}
 */
function getConfiguredEnvironments() {
  const rawEnvs  = process.env.PROVIDER_ENVS || '';
  const rawFiles = process.env.PROVIDER_ENV_PAYLOAD_FILES || '';

  const envIds = rawEnvs.split(',').map((s) => s.trim()).filter(Boolean);
  if (envIds.length === 0) return null; // signal: use legacy mode

  const payloadFiles = rawFiles.split(',').map((s) => s.trim());

  return envIds.map((id, i) => {
    // Normalise the key: "DEV" → "DEV", "dev-z3" → "DEV_Z3"
    const key = id.toUpperCase().replace(/[^A-Z0-9]/g, '_');

    const rawUrls   = process.env[`PROVIDER_${key}_URLS`]           || '';
    const rawLabels = process.env[`PROVIDER_${key}_URL_LABELS`]      || '';
    const rawAuths  = process.env[`PROVIDER_${key}_URL_AUTH_TYPES`]  || '';

    const urls   = rawUrls.split(',').map((s) => s.trim()).filter(Boolean);
    const labels = rawLabels.split(',').map((s) => s.trim());
    const auths  = rawAuths.split(',').map((s) => s.trim());

    return {
      id,
      label: id,
      payloadFile: payloadFiles[i] || id.toLowerCase(),
      urls: urls.map((url, j) => ({
        label:    labels[j] || `${id} URL ${j + 1}`,
        url,
        authType: (auths[j] || 'none').toLowerCase(),
      })),
    };
  });
}

module.exports = { callProvider, getConfiguredUrls, getConfiguredEnvironments };
