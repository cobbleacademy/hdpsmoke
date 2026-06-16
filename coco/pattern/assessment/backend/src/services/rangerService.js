'use strict';

const { getClient, isLlmConfigured } = require('./llmClient');

// ── Rego input normaliser ─────────────────────────────────────────────────────

/**
 * Server-side Rego cleanup applied before the prompt is built.
 *  - Strips markdown fences (```rego / ```)
 *  - Collapses 3+ consecutive blank lines to 2
 *  - Trims leading/trailing whitespace
 *  - Validates a package declaration is present (throws 400-friendly error)
 */
function normaliseRego(raw) {
  if (typeof raw !== 'string' || !raw.trim()) {
    throw new Error('Rego input must be a non-empty string');
  }

  let cleaned = raw
    .replace(/^```rego\s*/im, '')
    .replace(/^```\s*/im, '')
    .replace(/\s*```\s*$/im, '')
    .replace(/\r\n/g, '\n')          // normalise line endings
    .replace(/\t/g, '    ')          // tabs → 4 spaces
    .replace(/\n{3,}/g, '\n\n')      // collapse 3+ blank lines
    .trim();

  if (!cleaned.includes('package ')) {
    throw new Error(
      'Rego input must contain a package declaration (e.g. "package databricks.abac"). ' +
      'Verify you are pasting valid Rego, not ABAC SQL or another format.'
    );
  }

  return cleaned;
}

// ── Prompt builder ────────────────────────────────────────────────────────────

/**
 * Build the prompt sent to the LLM.
 *
 * The prompt instructs the model to:
 *  1. Parse the Rego rules — identify allow/deny conditions, input fields
 *     (principal, groups, resource, action, row values).
 *  2. Infer the Ranger service type from resource references:
 *       file paths / hdfs: → hdfs
 *       databases / tables / columns → hive
 *       HBase table references → hbase
 *       tag-based policies → tag
 *  3. Map Rego constructs to the Ranger policy JSON schema.
 *  4. Output a single valid Ranger REST API import JSON object.
 *
 * @param {string} regoCode  Normalised Rego content
 * @param {string} [extraHint]  Optional additional instruction (from customPrompt flow)
 */
function buildRangerPrompt(regoCode, extraHint = '') {
  return `You are an Apache Ranger policy expert. Convert the OPA Rego policy below into a valid Apache Ranger policy JSON that can be imported via the Ranger REST API (POST /service/public/v2/api/policy).

── ANALYSIS STEPS ───────────────────────────────────────────────────────────────
1. Parse every Rego rule: identify allow/deny logic, conditions on input.groups,
   input.principal, input.resource, input.action, and input.row field references.
2. Infer the Ranger service type from resource references in the Rego:
     input.resource starts with "/"  → "hdfs"
     input.database / input.table / input.column references → "hive"
     input.hbase_table references → "hbase"
     tag-based references → "tag"
     Default to "hive" when ambiguous.
3. Map Rego constructs to Ranger constructs:
     allow { ... }                → policyItems (grant access)
     deny { ... }                 → denyPolicyItems (explicit deny)
     "group-name" in input.groups → groups: ["group-name"] in policy item
     input.principal == "u@e.com" → users: ["u@e.com"] in policy item
     input.action / input.permission → accesses: [{ type: "<action>", isAllowed: true }]
     input.resource / path refs  → resources block (path / database / table / column)
     not "group" in input.groups → exclude group from policyItems or add to denyPolicyItems
     row-level conditions        → rowFilterPolicyItems (policyType: 2)
     column masking functions    → dataMaskPolicyItems  (policyType: 1)

── RANGER POLICY JSON SCHEMA ────────────────────────────────────────────────────
Output a single JSON object with this shape (omit optional fields when not needed):

{
  "name": "<descriptive policy name derived from Rego package/rule names>",
  "service": "<service-name>",          // e.g. "hive_dev", "hdfs_prod" — infer from Rego context
  "serviceType": "<hdfs|hive|hbase|tag>",
  "description": "<1-sentence summary of what the Rego policy enforces>",
  "isEnabled": true,
  "isAuditEnabled": true,
  "policyType": 0,                      // 0=access, 1=dataMask, 2=rowFilter
  "resources": {
    // For hive:
    "database": { "values": ["<db>"], "isExcludes": false, "isRecursive": false },
    "table":    { "values": ["<table>", "*"], "isExcludes": false, "isRecursive": false },
    "column":   { "values": ["*"], "isExcludes": false, "isRecursive": false }
    // For hdfs:
    // "path": { "values": ["/data/path/*"], "isExcludes": false, "isRecursive": true }
  },
  "policyItems": [
    {
      "accesses": [{ "type": "select", "isAllowed": true }],
      "users": [],
      "groups": ["<group-from-rego>"],
      "conditions": [],
      "delegateAdmin": false
    }
  ],
  "denyPolicyItems": [],           // populated from deny rules
  "denyExceptions": [],
  "dataMaskPolicyItems": [],       // populated for column masking
  "rowFilterPolicyItems": []       // populated for row filter with rowFilterInfo.filterExpr
}

── RANGER ACCESS TYPES by service ───────────────────────────────────────────────
hive:  select, update, create, drop, alter, index, lock, all, read, write
hdfs:  read, write, execute
hbase: read, write, create, admin

── RULES ────────────────────────────────────────────────────────────────────────
- Output valid JSON ONLY. No explanation, no markdown fences, no prose outside the JSON.
- Use the actual group/user/resource values from the Rego — do not invent placeholders.
- If the Rego contains multiple allow rules for different groups, emit one policyItem per group.
- If the Rego masks a column value, set policyType: 1 and populate dataMaskPolicyItems.
- If the Rego filters rows, set policyType: 2 and populate rowFilterPolicyItems with the filter expression.
- "service" value should follow the pattern "<serviceType>_<env>" (e.g. "hive_dev") — infer env from package name or default to "dev".
${extraHint ? `\n── ADDITIONAL INSTRUCTIONS ──────────────────────────────────────────────────────\n${extraHint}\n` : ''}
── OPA REGO INPUT ───────────────────────────────────────────────────────────────
${regoCode}`;
}

// ── Mock fallback — dynamic, derived from the actual Rego input ───────────────

/**
 * Build a mock Ranger policy that reflects the actual Rego content so that
 * changing the input produces a visibly different output in mock mode.
 *
 * Extracts: package name, group strings, catalog/database refs, rule types
 * (row_visible → rowFilter, column_masked → dataMask, allow → access).
 */
function buildMockRangerPolicy(regoCode) {
  // Package name → policy name + service suffix
  const pkgMatch = regoCode.match(/^package\s+([\w.]+)/m);
  const pkg = pkgMatch ? pkgMatch[1] : 'opa.policy';
  const pkgParts = pkg.split('.');
  const policyBaseName = pkgParts[pkgParts.length - 1].replace(/_/g, '-');

  // Groups: "group-name" in input.groups
  const groupMatches = [...regoCode.matchAll(/"([^"]+)"\s+in\s+input\.groups/g)];
  const groups = [...new Set(groupMatches.map((m) => m[1]))];

  // Catalog / database references: input.catalog == "X"
  const catalogMatch = regoCode.match(/input\.catalog\s*==\s*"([^"]+)"/);
  const catalog = catalogMatch ? catalogMatch[1] : 'default';

  // Table refs: input.table == "X" or has_tag_value(col, ...) implies table-level
  const tableMatch = regoCode.match(/input\.table\s*==\s*"([^"]+)"/);
  const table = tableMatch ? tableMatch[1] : '*';

  // Infer policy type from rule names
  const hasRowFilter   = /row_visible\s*\[/.test(regoCode);
  const hasColMask     = /column_masked\s*\[/.test(regoCode);
  const hasAllow       = /^allow\s*\{/m.test(regoCode);

  let policyType = 0; // access
  if (hasRowFilter) policyType = 2;
  else if (hasColMask) policyType = 1;

  // Service type: look for hdfs path refs, hbase refs, default hive
  let serviceType = 'hive';
  if (/input\.resource.*\//.test(regoCode) || /hdfs/.test(regoCode)) serviceType = 'hdfs';
  else if (/hbase_table|input\.hbase/.test(regoCode)) serviceType = 'hbase';

  const service = `${serviceType}_dev`;

  // Description derived from rule names found in the Rego
  const ruleNames = [...regoCode.matchAll(/^(\w+)\s*\[/mg)].map((m) => m[1]).filter(Boolean);
  const uniqueRules = [...new Set(ruleNames)].slice(0, 3).join(', ');
  const description = `Mock policy derived from package ${pkg}${uniqueRules ? ` — rules: ${uniqueRules}` : ''}.`;

  // Build policy items from extracted groups
  const makeItem = (grp) => ({
    accesses:      [{ type: serviceType === 'hdfs' ? 'read' : 'select', isAllowed: true }],
    users:         [],
    groups:        [grp],
    conditions:    [],
    delegateAdmin: false,
  });

  const resources = serviceType === 'hdfs'
    ? { path: { values: ['/data/*'], isExcludes: false, isRecursive: true } }
    : {
        database: { values: [catalog], isExcludes: false, isRecursive: false },
        table:    { values: [table],   isExcludes: false, isRecursive: false },
        column:   { values: ['*'],     isExcludes: false, isRecursive: false },
      };

  const policyItems = (policyType === 0 && groups.length)
    ? groups.map(makeItem)
    : (policyType === 0 ? [makeItem('public')] : []);

  const rowFilterPolicyItems = (policyType === 2 && groups.length)
    ? groups.map((grp) => ({
        ...makeItem(grp),
        rowFilterInfo: { filterExpr: `${grp.replace(/[^a-z0-9_]/gi, '_')} = 1` },
      }))
    : [];

  const dataMaskPolicyItems = (policyType === 1 && groups.length)
    ? groups.map((grp) => ({
        ...makeItem(grp),
        dataMaskInfo: { dataMaskType: 'MASK', conditionExpr: '' },
      }))
    : [];

  return {
    name:            `${policyBaseName}-policy`,
    service,
    serviceType,
    description,
    isEnabled:       true,
    isAuditEnabled:  true,
    policyType,
    resources,
    policyItems,
    denyPolicyItems:    [],
    denyExceptions:     [],
    dataMaskPolicyItems,
    rowFilterPolicyItems,
  };
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function stripJsonFences(text) {
  return text
    .replace(/^```json\s*/im, '')
    .replace(/^```\s*/im, '')
    .replace(/\s*```\s*$/im, '')
    .trim();
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Call OpenAI (or an OpenAI-compatible endpoint) and return the generated
 * Ranger policy JSON, along with the prompt used and token usage.
 *
 * @param {string} regoCode    Normalised Rego content
 * @param {{ model?, customPrompt? }} opts
 * @returns {{ rangerPolicy: object, builtPrompt: string, tokenUsage: object, mock: boolean }}
 */
async function generateRangerPolicy(regoCode, { model, customPrompt } = {}) {
  const resolvedModel =
    model ||
    process.env.RANGER_LLM_MODEL ||
    process.env.OPA_LLM_MODEL ||
    process.env.OPENAI_MODEL ||
    'gpt-4o';

  const normalised = normaliseRego(regoCode);
  const prompt = customPrompt || buildRangerPrompt(normalised);

  if (!isLlmConfigured()) {
    return {
      rangerPolicy: buildMockRangerPolicy(normalised),
      builtPrompt:  prompt,
      tokenUsage:   { promptTokens: 0, completionTokens: 0 },
      mock: true,
    };
  }

  const response = await getClient().chat.completions.create({
    model: resolvedModel,
    messages: [{ role: 'user', content: prompt }],
    max_tokens: 2000,
    temperature: 0.1,
  });

  const raw = response.choices[0].message.content.trim();
  const jsonText = stripJsonFences(raw);

  let rangerPolicy;
  try {
    rangerPolicy = JSON.parse(jsonText);
  } catch (err) {
    throw new Error(
      `LLM returned non-JSON output. Raw response:\n${raw.slice(0, 300)}`
    );
  }

  return {
    rangerPolicy,
    builtPrompt: prompt,
    tokenUsage: {
      promptTokens:      response.usage?.prompt_tokens     ?? 0,
      completionTokens:  response.usage?.completion_tokens ?? 0,
    },
    mock: false,
  };
}

module.exports = { buildRangerPrompt, generateRangerPolicy, normaliseRego };
