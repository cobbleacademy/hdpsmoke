const express = require('express');
const router = express.Router();
const questions = require('../data/questions.json');
const { calculateScores, determinePattern, rankPatterns } = require('../services/scoringEngine');
const { generateExplanation } = require('../services/llmService');
const { callProvider, getConfiguredUrls, getConfiguredEnvironments } = require('../services/providerService');
const { readPayload, writePayload } = require('../services/payloadService');

router.get('/questions', (req, res) => {
  res.json(questions);
});

router.post('/submit', async (req, res) => {
  const { answers } = req.body;

  if (!Array.isArray(answers) || answers.length === 0) {
    return res.status(400).json({ error: 'answers must be a non-empty array' });
  }

  for (const a of answers) {
    if (typeof a.questionId !== 'number' || typeof a.value !== 'number') {
      return res
        .status(400)
        .json({ error: 'Each answer must have numeric questionId and value fields' });
    }
  }

  try {
    const scores = calculateScores(questions, answers);
    const pattern = determinePattern(scores);
    const rankedPatterns = rankPatterns(scores);

    const explanation = await generateExplanation({
      pattern,
      rankedPatterns,
      answers,
      questions,
    });

    res.json({ pattern, scores, rankedPatterns, explanation });
  } catch (err) {
    console.error('Assessment error:', err);
    res.status(500).json({ error: 'Failed to process assessment' });
  }
});

// ── Provider API ──────────────────────────────────────────────────────────────

/**
 * GET /provider-config
 *
 * Tab mode   (PROVIDER_ENVS is set):
 *   Returns { environments, timeoutMs, writeAuthRequired }
 *
 * Legacy mode (PROVIDER_ENVS not set — backward compatible):
 *   Returns { urls, defaultAuthType, timeoutMs, writeAuthRequired }
 *
 * writeAuthRequired: true when PAYLOAD_WRITE_AUTH_ENABLED=true and
 *   PAYLOAD_ADMIN_TOKEN is set.  The frontend uses this to show a token
 *   input field in the payload editor before allowing saves.
 */
router.get('/provider-config', (req, res) => {
  const timeoutMs = Math.max(
    10_000,
    parseInt(process.env.PROVIDER_TIMEOUT_MS || '15000', 10) || 15_000
  );

  const writeAuthRequired =
    process.env.PAYLOAD_WRITE_AUTH_ENABLED === 'true' &&
    Boolean(process.env.PAYLOAD_ADMIN_TOKEN);

  const environments = getConfiguredEnvironments();
  if (environments !== null) {
    return res.json({ environments, timeoutMs, writeAuthRequired });
  }

  const urls = getConfiguredUrls();
  const defaultAuthType = (process.env.PROVIDER_AUTH_TYPE || 'none').toLowerCase();
  res.json({ urls, defaultAuthType, timeoutMs, writeAuthRequired });
});

/**
 * POST /run-payload
 * Body: { payload, url, authType, envId? }
 */
router.post('/run-payload', async (req, res) => {
  const { payload, url, authType, envId, skipTlsVerify } = req.body;

  if (typeof url !== 'string' || !url.startsWith('http')) {
    return res.status(400).json({ error: 'url must be a valid http/https string' });
  }
  if (payload === undefined || payload === null) {
    return res.status(400).json({ error: 'payload is required' });
  }

  try {
    const result = await callProvider(url, payload, authType, envId || null, Boolean(skipTlsVerify));
    res.json(result);
  } catch (err) {
    if (err.code === 'TIMEOUT') {
      return res.status(504).json({
        error: err.message,
        code: 'TIMEOUT',
        durationMs: err.durationMs,
      });
    }
    console.error('[run-payload] Provider call failed:', err.message);
    res.status(502).json({
      error: `Provider call failed: ${err.message}`,
      code: err.code || 'NETWORK',
      durationMs: err.durationMs,
    });
  }
});

// ── Payload content API ───────────────────────────────────────────────────────

/**
 * Middleware: verify the Bearer token when PAYLOAD_WRITE_AUTH_ENABLED=true.
 * Token is constant-time compared to prevent timing attacks.
 * If the env var is off (default) or PAYLOAD_ADMIN_TOKEN is not set, this
 * middleware is a no-op — all write requests are allowed.
 */
function checkWriteAuth(req, res, next) {
  if (process.env.PAYLOAD_WRITE_AUTH_ENABLED !== 'true') return next();
  const adminToken = process.env.PAYLOAD_ADMIN_TOKEN || '';
  if (!adminToken) {
    console.warn(
      '[assessment] PAYLOAD_WRITE_AUTH_ENABLED=true but PAYLOAD_ADMIN_TOKEN is not set — ' +
      'write auth is effectively disabled'
    );
    return next();
  }

  const authHeader = req.headers['authorization'] || '';
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';

  // constant-time comparison
  let valid = false;
  try {
    valid =
      token.length === adminToken.length &&
      crypto.timingSafeEqual(Buffer.from(token), Buffer.from(adminToken));
  } catch {
    valid = false;
  }

  if (!valid) {
    return res.status(401).json({
      error: 'Unauthorized: valid Bearer token required to write payload content',
    });
  }
  next();
}
const crypto = require('crypto');

/**
 * GET /payload-content/:envId
 * Returns the decrypted YAML and parsed payloads array for an environment.
 *
 * Response: { yaml: string, payloads: Array, encrypted: boolean }
 *
 * Resolution order (handled by payloadService.readPayload):
 *   1. {PAYLOAD_STORAGE_PATH}/{envId}.enc  — AES-256-GCM encrypted
 *   2. frontend/public/payloads/{envId}.yaml — plain-text fallback (dev / migration)
 *   3. frontend/public/payloads.yaml           — legacy flat-file fallback
 */
router.get('/payload-content/:envId', async (req, res) => {
  const { envId } = req.params;

  if (!envId || envId.length > 64 || !/^[\w\-]+$/.test(envId)) {
    return res.status(400).json({ error: 'Invalid envId' });
  }

  try {
    const result = readPayload(envId);
    if (!result) {
      return res.status(404).json({
        error: `No payload file found for environment "${envId}". ` +
          `Create ${envId}.yaml in frontend/public/payloads/ or use the editor to save one.`,
      });
    }
    res.json(result);
  } catch (err) {
    console.error(`[payload-content] GET "${envId}" failed:`, err.message);
    res.status(500).json({ error: err.message });
  }
});

/**
 * PUT /payload-content/:envId
 * Body: { yaml: string }
 * Validates YAML structure, encrypts (if key is set), and persists to storage.
 *
 * Response: { ok: true }
 *
 * Protected by checkWriteAuth middleware when PAYLOAD_WRITE_AUTH_ENABLED=true.
 */
router.put('/payload-content/:envId', checkWriteAuth, async (req, res) => {
  const { envId } = req.params;
  const { yaml }  = req.body;

  if (!envId || envId.length > 64 || !/^[\w\-]+$/.test(envId)) {
    return res.status(400).json({ error: 'Invalid envId' });
  }
  if (typeof yaml !== 'string' || !yaml.trim()) {
    return res.status(400).json({ error: 'yaml field must be a non-empty string' });
  }

  try {
    writePayload(envId, yaml);
    res.json({ ok: true });
  } catch (err) {
    const status = err.message.startsWith('Invalid YAML') ||
                   err.message.includes('must contain') ||
                   err.message.includes('missing') ? 400 : 500;
    res.status(status).json({ error: err.message });
  }
});

module.exports = router;
