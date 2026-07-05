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
//
// personName: when the prompt names a person by display name instead of an
// exact UPN/SAM ("show groups for John Smith"), upn is null and personName
// carries the raw name text — the route layer resolves it to a list of
// candidate accounts via entraGraphService.searchAccounts() and asks the
// caller to disambiguate before fetching groups.

const { getClient, isLlmConfigured } = require('./llmClient');
const { isLlmEnabled, getLlmModel } = require('./identityAuditConfigService');

// ── Mode B: regex extraction ───────────────────────────────────────────────────

const EMAIL_REGEX = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/;

// SAM account keyword: "user", "account", "sam", or "sam account", optionally
// followed by ':' or whitespace, then the SAM token (7-8 alphanumeric, no '@').
const SAM_KEYWORD_REGEX = /\b(?:user(?:\s*(?:id|name|account))?|sam(?:\s*account)?|account)\b[\s:]*([A-Za-z0-9]{7,8})/i;

// Each entry: [regex, filterType]. Tried in order; first match wins for Mode B
// (the bundled template formats only ever carry one filter clause).
const FILTER_PATTERNS = [
  [/\b(?:containing|contains)\s+['"]?([^'"]+?)['"]?\s*$/i, 'contains'],
  [/\b(?:starts?\s+with|starting\s+with)\s+['"]?([^'"]+?)['"]?\s*$/i, 'startsWith'],
  [/\b(?:ending\s+in|ends?\s+with)\s+['"]?([^'"]+?)['"]?\s*$/i, 'endsWith'],
];

// Display-name fallback: "for John Smith" / "user John Smith" — 2-4 capitalized
// words, tried only after email/SAM extraction both fail. Matched against the
// prompt with any trailing filter clause already stripped off (see below), so
// "for John Smith containing Finance" doesn't swallow the filter text.
const NAME_REGEX = /\b(?:for|user)\s+([A-Z][a-zA-Z'-]*(?:\s+[A-Z][a-zA-Z'-]*){1,3})\b/;

function parseWithRegex(prompt) {
  // Try UPN/email first
  const emailMatch = prompt.match(EMAIL_REGEX);
  let upn = emailMatch ? emailMatch[0] : null;

  // If no email, try SAM account via explicit user/account/sam keyword
  if (!upn) {
    const samMatch = prompt.match(SAM_KEYWORD_REGEX);
    upn = samMatch ? samMatch[1] : null;
  }

  const filters = [];
  let remaining = prompt;
  for (const [regex, type] of FILTER_PATTERNS) {
    const match = prompt.match(regex);
    if (match) {
      filters.push({ type, value: match[1].trim() });
      remaining = prompt.slice(0, match.index); // strip filter clause before name search
      break; // one filter clause per template, by design
    }
  }

  let personName = null;
  if (!upn) {
    const nameMatch = remaining.match(NAME_REGEX);
    personName = nameMatch ? nameMatch[1].trim() : null;
  }

  return { upn, personName, filters };
}

// ── Mode A: LLM structured-output extraction ──────────────────────────────────

const EXTRACTION_SCHEMA = {
  type: 'object',
  properties: {
    upn: { type: ['string', 'null'], description: 'The target user as an exact UPN/email address (contains @) or a SAM account name (short alphanumeric, no @). Null if the prompt only names a person by display name (e.g. "John Smith") rather than an exact identifier.' },
    personName: { type: ['string', 'null'], description: 'The person\'s display name (e.g. "John Smith") when the prompt does not give an exact UPN/SAM. Null when upn is set.' },
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
  required: ['upn', 'personName', 'filters'],
  additionalProperties: false,
};

async function parseWithLlm(prompt) {
  const response = await getClient().chat.completions.create({
    model: getLlmModel(),
    messages: [
      {
        role: 'system',
        content:
          'Extract the target user from this identity-audit request, plus zero or more ' +
          'group-name filter conditions (startsWith / endsWith / contains). If the prompt gives ' +
          'an exact UPN/email address or a short SAM account name (no @ sign), set upn to that ' +
          'value and personName to null. If the prompt instead names a person only by display ' +
          'name (e.g. "John Smith") with no exact identifier, set upn to null and personName to ' +
          'that display name. A prompt may name more than one filter condition ' +
          '(e.g. "contains X or ends with Y") — return all of them. Do not invent a filter that ' +
          'is not present.',
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
 * Extracts { upn, personName, filters, mode } from a natural-language prompt.
 * mode is 'llm' or 'regex' — regex is also the automatic fallback if
 * IDENTITY_AUDIT_USE_LLM=true but the LLM call fails or isn't configured.
 * Exactly one of upn/personName is set when either is extractable; both may
 * be null if the prompt names no user at all.
 */
async function parsePrompt(prompt) {
  if (isLlmEnabled() && isLlmConfigured()) {
    try {
      const parsed = await parseWithLlm(prompt);
      if (parsed?.upn || parsed?.personName) {
        return { upn: parsed.upn || null, personName: parsed.personName || null, filters: parsed.filters || [], mode: 'llm' };
      }
    } catch (err) {
      console.error('[identityAuditParserService] LLM extraction failed, falling back to regex:', err.message);
    }
  }

  return { ...parseWithRegex(prompt), mode: 'regex' };
}

module.exports = { parsePrompt };
