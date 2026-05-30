const express = require('express');
const router = express.Router();
const questions = require('../data/questions.json');
const { calculateScores, determinePattern, rankPatterns } = require('../services/scoringEngine');
const { generateExplanation } = require('../services/llmService');
const { callProvider, getConfiguredUrls, getConfiguredEnvironments } = require('../services/providerService');

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
 *   Returns { environments: [{id, label, payloadFile, urls: [{label,url,authType}]}], timeoutMs }
 *   Each environment bundles its own URL list with per-URL auth types.
 *
 * Legacy mode (PROVIDER_ENVS not set — backward compatible):
 *   Returns { urls: [{label,url}], defaultAuthType, timeoutMs }
 *   Identical shape to the pre-tab version; existing deployments are unaffected.
 */
router.get('/provider-config', (req, res) => {
  const timeoutMs = Math.max(
    10_000,
    parseInt(process.env.PROVIDER_TIMEOUT_MS || '15000', 10) || 15_000
  );

  const environments = getConfiguredEnvironments();
  if (environments !== null) {
    // Tab mode
    return res.json({ environments, timeoutMs });
  }

  // Legacy mode
  const urls = getConfiguredUrls();
  const defaultAuthType = (process.env.PROVIDER_AUTH_TYPE || 'none').toLowerCase();
  res.json({ urls, defaultAuthType, timeoutMs });
});

/**
 * POST /run-payload
 * Body: { payload: <object|string>, url: <string>, authType: <string> }
 * authType is selected per-request by the frontend dropdown.
 * Falls back to PROVIDER_AUTH_TYPE env var if omitted.
 * Returns: { status, body, durationMs } (provider's own HTTP status + response)
 */
router.post('/run-payload', async (req, res) => {
  const { payload, url, authType } = req.body;

  if (typeof url !== 'string' || !url.startsWith('http')) {
    return res.status(400).json({ error: 'url must be a valid http/https string' });
  }
  if (payload === undefined || payload === null) {
    return res.status(400).json({ error: 'payload is required' });
  }

  try {
    const result = await callProvider(url, payload, authType);
    // Always return 200 from our endpoint; provider's status is in result.status
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

module.exports = router;
