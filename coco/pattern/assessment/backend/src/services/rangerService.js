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
  return `You are an Apache Ranger policy expert. Convert the OPA Rego policy below into one or more valid Apache Ranger policy JSON objects that can be imported via the Ranger REST Import API (POST /service/public/v2/api/policy/importPoliciesFromFile).

── ANALYSIS STEPS ───────────────────────────────────────────────────────────────
1. Parse every Rego rule: identify allow/deny logic, conditions on input.groups,
   input.principal, input.resource, input.action, and input.row field references.
2. Infer the Ranger service type from resource references in the Rego:
     input.resource starts with "/"  → "hdfs"
     input.database / input.table / input.column references → "hive"
     input.hbase_table references → "hbase"
     tag-based references → "tag"
     Default to "hive" when ambiguous.
3. Identify ALL distinct policy types needed:
     row_visible[...] rules   → one policy object with policyType: 2 (rowFilter)
     column_masked[...] rules → one policy object with policyType: 1 (dataMask)
     allow { ... } rules      → one policy object with policyType: 0 (access)
     If MULTIPLE types are present, produce a SEPARATE object for EACH type.
4. Map Rego constructs to Ranger constructs:
     allow { ... }                → policyItems (grant access)
     deny { ... }                 → denyPolicyItems (explicit deny)
     "group-name" in input.groups → groups: ["group-name"] in policy item
     not "group" in input.groups  → denyPolicyItems (the group is denied, not granted)
     input.principal == "u@e.com" → users: ["u@e.com"] in policy item
     input.action / input.permission → accesses: [{ type: "<action>", isAllowed: true }]
     input.resource / path refs  → resources block (path / database / table / column)
     row-level conditions        → rowFilterPolicyItems (policyType: 2)
     column masking functions    → dataMaskPolicyItems  (policyType: 1)

── RANGER POLICY JSON SCHEMA (one object per policyType) ────────────────────────
Each element of the output array must follow this shape:

{
  "name": "<descriptive policy name — include rule name and type, e.g. 'region-row-filter'>",
  "service": "<service-name>",          // e.g. "hive_dev" — infer from Rego context
  "serviceType": "<hdfs|hive|hbase|tag>",
  "description": "<1-sentence summary>",
  "isEnabled": true,
  "isAuditEnabled": true,
  "policyType": 0,                      // 0=access, 1=dataMask, 2=rowFilter — set per object
  "resources": {
    "database": { "values": ["<db>"], "isExcludes": false, "isRecursive": false },
    "table":    { "values": ["*"],    "isExcludes": false, "isRecursive": false },
    "column":   { "values": ["*"],    "isExcludes": false, "isRecursive": false }
  },
  "policyItems": [],
  "denyPolicyItems": [],
  "denyExceptions": [],
  "dataMaskPolicyItems": [],
  "rowFilterPolicyItems": []
}

── RANGER ACCESS TYPES by service ───────────────────────────────────────────────
hive:  select, update, create, drop, alter, index, lock, all, read, write
hdfs:  read, write, execute
hbase: read, write, create, admin

── OUTPUT FORMAT RULES ──────────────────────────────────────────────────────────
- Output a JSON ARRAY [...] containing one object per distinct policyType found.
- If the Rego has BOTH row_visible and column_masked rules, output TWO objects: one with policyType:2 and one with policyType:1.
- If the Rego has only access (allow) rules, output ONE object with policyType:0.
- Output valid JSON ONLY. No explanation, no markdown fences, no prose outside the JSON.
- Use the actual group/user/resource values from the Rego — do not invent placeholders.
- Groups appearing with "not X in input.groups" are DENIED — put them in denyPolicyItems, not policyItems.
- rowFilterPolicyItems must include rowFilterInfo.filterExpr with the row condition expression.
- dataMaskPolicyItems must include dataMaskInfo.dataMaskType (e.g. "MASK", "MASK_NULL", "CUSTOM").
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

  // Detect which policy types are present — independent checks, not if/else
  const hasRowFilter = /row_visible\s*\[/.test(regoCode);
  const hasColMask   = /column_masked\s*\[/.test(regoCode);
  const hasAllow     = /^allow\s*\{/m.test(regoCode);

  // Service type: look for hdfs path refs, hbase refs, default hive
  let serviceType = 'hive';
  if (/input\.resource.*\//.test(regoCode) || /hdfs/.test(regoCode)) serviceType = 'hdfs';
  else if (/hbase_table|input\.hbase/.test(regoCode)) serviceType = 'hbase';

  const service = `${serviceType}_dev`;

  // Description derived from rule names found in the Rego
  const ruleNames = [...regoCode.matchAll(/^(\w+)\s*\[/mg)].map((m) => m[1]).filter(Boolean);
  const uniqueRules = [...new Set(ruleNames)].slice(0, 3).join(', ');
  const description = `Mock policy derived from package ${pkg}${uniqueRules ? ` — rules: ${uniqueRules}` : ''}.`;

  // Shared helpers
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

  const basePolicy = {
    service,
    serviceType,
    description,
    isEnabled:      true,
    isAuditEnabled: true,
    resources,
    policyItems:         [],
    denyPolicyItems:     [],
    denyExceptions:      [],
    dataMaskPolicyItems: [],
    rowFilterPolicyItems: [],
  };

  const policies = [];

  // Row filter policy (policyType: 2)
  if (hasRowFilter) {
    const items = groups.length
      ? groups.map((grp) => ({
          ...makeItem(grp),
          rowFilterInfo: { filterExpr: `${grp.replace(/[^a-z0-9_]/gi, '_')} = 1` },
        }))
      : [];
    policies.push({
      ...basePolicy,
      name:      `${policyBaseName}-row-filter`,
      policyType: 2,
      rowFilterPolicyItems: items,
    });
  }

  // Column masking policy (policyType: 1)
  if (hasColMask) {
    const items = groups.length
      ? groups.map((grp) => ({
          ...makeItem(grp),
          dataMaskInfo: { dataMaskType: 'MASK', conditionExpr: '' },
        }))
      : [];
    policies.push({
      ...basePolicy,
      name:      `${policyBaseName}-col-mask`,
      policyType: 1,
      dataMaskPolicyItems: items,
    });
  }

  // Access policy (policyType: 0) — only when no row/col policy types detected
  if (!hasRowFilter && !hasColMask) {
    const items = groups.length ? groups.map(makeItem) : [makeItem('public')];
    policies.push({
      ...basePolicy,
      name:      `${policyBaseName}-policy`,
      policyType: 0,
      policyItems: items,
    });
  }

  return policies;
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
      rangerPolicies: buildMockRangerPolicy(normalised),
      builtPrompt:    prompt,
      tokenUsage:     { promptTokens: 0, completionTokens: 0 },
      mock: true,
    };
  }

  const response = await getClient().chat.completions.create({
    model: resolvedModel,
    messages: [{ role: 'user', content: prompt }],
    max_tokens: 3000,
    temperature: 0.1,
  });

  const raw = response.choices[0].message.content.trim();
  const jsonText = stripJsonFences(raw);

  let rangerPolicies;
  try {
    const parsed = JSON.parse(jsonText);
    // Normalise: LLM may return a single object or an array
    rangerPolicies = Array.isArray(parsed) ? parsed : [parsed];
  } catch (err) {
    throw new Error(
      `LLM returned non-JSON output. Raw response:\n${raw.slice(0, 300)}`
    );
  }

  return {
    rangerPolicies,
    builtPrompt: prompt,
    tokenUsage: {
      promptTokens:      response.usage?.prompt_tokens     ?? 0,
      completionTokens:  response.usage?.completion_tokens ?? 0,
    },
    mock: false,
  };
}

module.exports = { buildRangerPrompt, generateRangerPolicy, normaliseRego, buildMockRangerPolicy };
