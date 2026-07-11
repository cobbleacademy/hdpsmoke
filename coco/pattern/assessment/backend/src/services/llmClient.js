'use strict';

const OpenAI = require('openai');

function envPrefix(envId) {
  return envId ? `LLM_${String(envId).toUpperCase()}_` : null;
}

/**
 * True when a real LLM endpoint is configured for the given environment —
 * either that env's own override (LLM_{ENV}_OPENAI_API_KEY /
 * LLM_{ENV}_OPENAI_BASE_URL) or the global OPENAI_API_KEY/OPENAI_BASE_URL
 * fallback. Called with no envId, only the global vars are checked — same
 * behavior as before per-env support existed. When false, callers should
 * fall back to deterministic mock output instead of making a network call.
 */
function isLlmConfigured({ envId } = {}) {
  const prefix = envPrefix(envId);
  return !!(
    (prefix && (process.env[`${prefix}OPENAI_API_KEY`] || process.env[`${prefix}OPENAI_BASE_URL`])) ||
    process.env.OPENAI_API_KEY ||
    process.env.OPENAI_BASE_URL
  );
}

/**
 * Resolves { baseURL, apiKey, defaultHeaders } for one environment + model.
 *
 * Per-env vars (LLM_{ENV}_OPENAI_BASE_URL / _API_KEY / _EXTRA_HEADERS) take
 * priority; each falls back independently to its global OPENAI_* counterpart
 * when not set for that environment — same per-var fallback shape as
 * PROVIDER_{ENV}_API_KEY falling back to PROVIDER_API_KEY.
 *
 * OPENAI_BASE_URL (global or per-env) may contain a literal "{model}"
 * placeholder for gateways that require the model in the URL path rather
 * than the request body (e.g. https://<gateway>/chat/{model}) — substituted
 * with the resolved model before the client is constructed. A baseURL with
 * no placeholder is used as-is (Ollama, plain OpenAI, local mock, etc.).
 */
function resolveConfig({ envId, model } = {}) {
  const prefix = envPrefix(envId);

  const baseURLTemplate =
    (prefix && process.env[`${prefix}OPENAI_BASE_URL`]) ||
    process.env.OPENAI_BASE_URL ||
    undefined;

  const apiKey =
    (prefix && process.env[`${prefix}OPENAI_API_KEY`]) ||
    process.env.OPENAI_API_KEY ||
    'ollama'; // SDK requires a non-empty string; Ollama itself ignores it

  const extraHeadersRaw =
    (prefix && process.env[`${prefix}OPENAI_EXTRA_HEADERS`]) ||
    process.env.OPENAI_EXTRA_HEADERS ||
    '';

  let baseURL = baseURLTemplate;
  if (baseURL && model && baseURL.includes('{model}')) {
    baseURL = baseURL.split('{model}').join(model);
  }

  let defaultHeaders;
  if (extraHeadersRaw) {
    try {
      defaultHeaders = JSON.parse(extraHeadersRaw);
    } catch (err) {
      const varName = prefix ? `${prefix}OPENAI_EXTRA_HEADERS` : 'OPENAI_EXTRA_HEADERS';
      console.warn(`[llmClient] Invalid JSON in ${varName} — ignoring extra headers: ${err.message}`);
    }
  }

  return { baseURL, apiKey, defaultHeaders };
}

// One client per distinct (envId, resolved baseURL) pair. A single shared
// singleton is no longer correct once baseURL can vary by both environment
// and model — some gateways require the model baked into the URL path
// (see resolveConfig above), so different models legitimately need
// different client instances even within the same environment.
const _clients = new Map();

/**
 * Shared OpenAI SDK client, lazily constructed per (envId, model) and reused
 * across llmService.js, opaPolicyService.js, rangerService.js,
 * identityAuditParserService.js, and permissionParserService.mjs.
 *
 * Supports OpenAI-compatible endpoints via OPENAI_BASE_URL (e.g. Ollama's
 * `/v1` shim, or a gateway requiring "{model}" in the URL path) and static
 * extra headers via OPENAI_EXTRA_HEADERS (JSON) — both overridable per
 * environment via the LLM_{ENV}_* vars documented above.
 */
function getClient({ envId, model } = {}) {
  const { baseURL, apiKey, defaultHeaders } = resolveConfig({ envId, model });
  const cacheKey = `${envId || 'default'}::${baseURL || 'default'}`;

  if (!_clients.has(cacheKey)) {
    _clients.set(cacheKey, new OpenAI({ apiKey, baseURL, defaultHeaders }));
  }
  return _clients.get(cacheKey);
}

module.exports = { getClient, isLlmConfigured };
