'use strict';

// Identity Audit — natural-language prompt parsing.
//
// Mode A (IDENTITY_AUDIT_USE_LLM=true): OpenAI Structured Outputs extracts
//   { upn, filters: [{type, value}, ...] } — an array because a single prompt
//   can legitimately carry more than one condition ("contains 'Finance' OR
//   ends with '-Admin'").
// Mode B (default): regex against three rigid templates — no NLP library;
//   the expected formats are fixed enough that string matching is exact,
//   not approximate (same reasoning as permissionParserService.js).

const { getClient, isLlmConfigured } = require('./llmClient');
const { isLlmEnabled, getLlmModel } = require('./identityAuditConfigService');

// ── Mode B: regex extraction ───────────────────────────────────────────────────

const EMAIL_REGEX = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/;

// Each entry: [regex, filterType]. Tried in order; first match wins for Mode B
// (the bundled template formats only ever carry one filter clause).
const FILTER_PATTERNS = [
  [/\b(?:containing|contains)\s+['"]?([^'"]+?)['"]?\s*$/i, 'contains'],
  [/\b(?:starts?\s+with|starting\s+with)\s+['"]?([^'"]+?)['"]?\s*$/i, 'startsWith'],
  [/\b(?:ending\s+in|ends?\s+with)\s+['"]?([^'"]+?)['"]?\s*$/i, 'endsWith'],
];

function parseWithRegex(prompt) {
  const emailMatch = prompt.match(EMAIL_REGEX);
  const upn = emailMatch ? emailMatch[0] : null;

  const filters = [];
  for (const [regex, type] of FILTER_PATTERNS) {
    const match = prompt.match(regex);
    if (match) {
      filters.push({ type, value: match[1].trim() });
      break; // one filter clause per template, by design
    }
  }

  return { upn, filters };
}

// ── Mode A: LLM structured-output extraction ──────────────────────────────────

const EXTRACTION_SCHEMA = {
  type: 'object',
  properties: {
    upn: { type: 'string', description: 'The target user principal name (email) whose Entra group membership is being audited' },
    filters: {
      type: 'array',
      description: 'Zero or more scoping conditions on the returned group names. Empty array if the prompt names no filter.',
      items: {
        type: 'object',
        properties: {
          type:  { type: 'string', enum: ['startsWith', 'endsWith', 'contains'] },
          value: { type: 'string', description: 'The literal substring/prefix/suffix to match against group names' },
        },
        required: ['type', 'value'],
        additionalProperties: false,
      },
    },
  },
  required: ['upn', 'filters'],
  additionalProperties: false,
};

async function parseWithLlm(prompt) {
  const response = await getClient().chat.completions.create({
    model: getLlmModel(),
    messages: [
      {
        role: 'system',
        content:
          'Extract the target user (upn, an email address) and zero or more group-name ' +
          'filter conditions (startsWith / endsWith / contains) from this identity-audit ' +
          'request. A prompt may name more than one filter condition (e.g. "contains X or ' +
          'ends with Y") — return all of them. Do not invent a filter that is not present.',
      },
      { role: 'user', content: prompt },
    ],
    response_format: {
      type: 'json_schema',
      json_schema: { name: 'identity_audit_extraction', strict: true, schema: EXTRACTION_SCHEMA },
    },
  });

  return JSON.parse(response.choices[0].message.content);
}

// ── Public entry point ──────────────────────────────────────────────────────────

/**
 * Extracts { upn, filters, mode } from a natural-language prompt. mode is
 * 'llm' or 'regex' — regex is also the automatic fallback if
 * IDENTITY_AUDIT_USE_LLM=true but the LLM call fails or isn't configured.
 */
async function parsePrompt(prompt) {
  if (isLlmEnabled() && isLlmConfigured()) {
    try {
      const parsed = await parseWithLlm(prompt);
      if (parsed?.upn) {
        return { upn: parsed.upn, filters: parsed.filters || [], mode: 'llm' };
      }
    } catch (err) {
      console.error('[identityAuditParserService] LLM extraction failed, falling back to regex:', err.message);
    }
  }

  return { ...parseWithRegex(prompt), mode: 'regex' };
}

module.exports = { parsePrompt };
