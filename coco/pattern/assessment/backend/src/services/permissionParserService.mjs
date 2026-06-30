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

// "user", "account", "sam", or "sam account", optionally followed by ':' or
// whitespace, then the SAM account name (7–8 alphanumeric chars, no '@').
const USER_KEYWORD_REGEX = /\b(?:user(?:\s*(?:id|name|account))?|sam(?:\s*account)?|account)\b[\s:]*([A-Za-z0-9]{7,8})/i;

// Fallback when no "group"/"groupid" keyword is present: the first token that
// mixes letters and digits (e.g. "Alpha12") — distinguishes a real group ID
// from ordinary English words in the surrounding sentence.
const ALNUM_MIXED_REGEX = /\b(?=[A-Za-z0-9]*[A-Za-z])(?=[A-Za-z0-9]*[0-9])[A-Za-z0-9]{2,}\b/;

function parseWithRegex(prompt) {
  // Try email first (UPN)
  const emailMatch = prompt.match(EMAIL_REGEX);
  let userPrincipalName = emailMatch ? emailMatch[0] : null;

  // If no email, try SAM account via explicit user/account/sam keyword
  if (!userPrincipalName) {
    const userKeyMatch = prompt.match(USER_KEYWORD_REGEX);
    userPrincipalName = userKeyMatch ? userKeyMatch[1] : null;
  }

  // Strip the identified user from prompt before hunting for the group ID so
  // neither the email's local part nor a SAM token gets mistaken for the group.
  const withoutUser = userPrincipalName ? prompt.replace(userPrincipalName, ' ') : prompt;

  const keywordMatch = withoutUser.match(GROUP_KEYWORD_REGEX);
  const groupId = keywordMatch ? keywordMatch[1] : (withoutUser.match(ALNUM_MIXED_REGEX)?.[0] ?? null);

  return { userPrincipalName, groupId };
}

// ── Mode A: LLM structured-output extraction ──────────────────────────────────

const EXTRACTION_SCHEMA = {
  type: 'object',
  properties: {
    userPrincipalName: { type: 'string', description: 'The email address (UPN) or SAM account name (short alphanumeric, no @) identifying the user making the access request' },
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
          'userPrincipalName (the email address/UPN, or a short SAM account name ' +
          'with no @ sign) and groupId (the alphanumeric group identifier). ' +
          'Return only those two fields — do not invent values not present in the request.',
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
