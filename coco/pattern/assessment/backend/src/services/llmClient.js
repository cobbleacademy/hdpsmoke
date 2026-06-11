'use strict';

const OpenAI = require('openai');

let _client = null;

/**
 * True when a real LLM endpoint is configured — either an OpenAI API key,
 * or an OpenAI-compatible base URL (e.g. a local Ollama server at
 * http://localhost:11434/v1). When false, callers should fall back to
 * deterministic mock output instead of making a network call.
 */
function isLlmConfigured() {
  return !!(process.env.OPENAI_API_KEY || process.env.OPENAI_BASE_URL);
}

/**
 * Shared OpenAI SDK client, lazily constructed and reused across
 * llmService.js and opaPolicyService.js.
 *
 * Supports OpenAI-compatible endpoints via OPENAI_BASE_URL (e.g. Ollama's
 * `/v1` shim). Ollama doesn't check the API key, but the SDK requires a
 * non-empty string, so we fall back to a placeholder.
 */
function getClient() {
  if (!_client) {
    _client = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY || 'ollama',
      baseURL: process.env.OPENAI_BASE_URL || undefined,
    });
  }
  return _client;
}

module.exports = { getClient, isLlmConfigured };
