// Group Permission Evaluation — natural-language prompt parsing.
//
// Mode A (USE_LLM=true): OpenAI Structured Outputs (response_format: json_schema)
//   strictly extracts { userPrincipalName, groupId }.
// Mode B (USE_LLM=false, or LLM call fails): regex-only extraction — no
//   external NLP library, safe to run with zero network dependency.
//
// Reuses this app's existing shared OpenAI client (backend/src/services/llmClient.js)
// rather than creating a second OpenAI integration — Node's CJS/ESM interop lets
// this ES module import named exports from that CommonJS file directly.

import { getClient, isLlmConfigured } from './llmClient.js';

// ── Mode B: regex extraction ───────────────────────────────────────────────────

const EMAIL_REGEX = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/;

// "group" or "group id"/"groupid", optionally followed by ':' or whitespace,
// then the alphanumeric token that is the actual group identifier.
const GROUP_KEYWORD_REGEX = /\bgroup(?:\s*id)?\b[\s:]*([A-Za-z0-9]+)/i;

// Fallback when no "group"/"groupid" keyword is present: the first token that
// mixes letters and digits (e.g. "Alpha12") — distinguishes a real group ID
// from ordinary English words in the surrounding sentence.
const ALNUM_MIXED_REGEX = /\b(?=[A-Za-z0-9]*[A-Za-z])(?=[A-Za-z0-9]*[0-9])[A-Za-z0-9]{2,}\b/;

function parseWithRegex(prompt) {
  const emailMatch = prompt.match(EMAIL_REGEX);
  const userPrincipalName = emailMatch ? emailMatch[0] : null;

  // Strip the email out before hunting for the group ID so the email's local
  // part (which can itself contain digits) never gets mistaken for it.
  const withoutEmail = userPrincipalName ? prompt.replace(userPrincipalName, ' ') : prompt;

  const keywordMatch = withoutEmail.match(GROUP_KEYWORD_REGEX);
  const groupId = keywordMatch ? keywordMatch[1] : (withoutEmail.match(ALNUM_MIXED_REGEX)?.[0] ?? null);

  return { userPrincipalName, groupId };
}

// ── Mode A: LLM structured-output extraction ──────────────────────────────────

const EXTRACTION_SCHEMA = {
  type: 'object',
  properties: {
    userPrincipalName: { type: 'string', description: 'The email address identifying the user making the access request' },
    groupId: { type: 'string', description: 'The alphanumeric resource group identifier referenced in the request' },
  },
  required: ['userPrincipalName', 'groupId'],
  additionalProperties: false,
};

async function parseWithLlm(prompt) {
  const model = process.env.PERMISSION_LLM_MODEL || process.env.OPENAI_MODEL || 'gpt-4o-mini';

  const response = await getClient().chat.completions.create({
    model,
    messages: [
      {
        role: 'system',
        content:
          'Extract exactly two fields from the user\'s access-check request: ' +
          'userPrincipalName (the email address) and groupId (the alphanumeric ' +
          'group identifier). Return only those two fields — do not invent values ' +
          'that are not present in the request.',
      },
      { role: 'user', content: prompt },
    ],
    response_format: {
      type: 'json_schema',
      json_schema: { name: 'permission_extraction', strict: true, schema: EXTRACTION_SCHEMA },
    },
  });

  return JSON.parse(response.choices[0].message.content);
}

// ── Public entry point ──────────────────────────────────────────────────────────

/**
 * Extracts { userPrincipalName, groupId } from a natural-language prompt.
 * Returns { userPrincipalName, groupId, mode } where mode is 'llm' or 'regex'
 * (regex is also the automatic fallback if USE_LLM=true but the LLM call fails
 * or isn't configured).
 */
export async function parsePrompt(prompt) {
  const useLlm = process.env.USE_LLM === 'true';

  if (useLlm && isLlmConfigured()) {
    try {
      const parsed = await parseWithLlm(prompt);
      if (parsed?.userPrincipalName && parsed?.groupId) {
        return { ...parsed, mode: 'llm' };
      }
    } catch (err) {
      console.error('[permissionParserService] LLM extraction failed, falling back to regex:', err.message);
    }
  }

  return { ...parseWithRegex(prompt), mode: 'regex' };
}
