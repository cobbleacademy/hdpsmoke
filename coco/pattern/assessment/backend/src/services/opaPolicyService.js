'use strict';

const { getClient, isLlmConfigured } = require('./llmClient');

// ── Prompt templates ──────────────────────────────────────────────────────────

function buildDefaultPrompt(abacContent, extraHint = '') {
  return `You are an OPA (Open Policy Agent) Rego expert. Convert the Databricks Unity Catalog ABAC SQL below to valid Rego.

The SQL may contain five construct types — handle each as specified:

── SKIP (emit nothing) ──────────────────────────────────────────────────────────
1. CREATE GOVERNED TAG ...       → tag schema definition, skip entirely
2. ALTER TABLE ... SET TAGS ...  → runtime metadata, skip entirely

── TRANSLATE: UDFs ──────────────────────────────────────────────────────────────
3. CREATE [OR REPLACE] FUNCTION name(params) RETURNS TYPE RETURN <expr>
   Convert RETURN expression to Rego. SQL → Rego builtin map:
     LOWER(x)                   → lower(x)
     x LIKE '%s%'               → contains(lower(x), "s")
     CASE WHEN c THEN a ELSE b  → two rules: one \`if { c }\`, one \`if { not c }\`
     OR compound                → separate rules (one per OR branch)
     AND compound               → multiple conditions in same rule body
     LEFT(s, n)                 → substring(s, 0, n)
     RIGHT(s, n)                → substring(s, count(s)-n, n)
     CONCAT(a, b, c)            → concat("", [a, b, c])
     SUBSTRING_INDEX(s,'@',-1)  → split(s, "@")[1]
     is_account_group_member(g) → "g" in input.groups   ← ALWAYS apply this, even inside UDF bodies
     TRUE / FALSE               → true / false (lowercase Rego literals)
   RETURNS BOOLEAN → \`fname(params) if { ... }\`  (one rule per OR branch)
   RETURNS STRING  → \`fname(params) := value if { ... }\`  (one rule per CASE branch)

   CRITICAL — UDF parameters are VALUES, not column names:
     UDF params represent the actual data values passed at call time (e.g. a region string "east").
     NEVER write \`input.row[param]\` inside a UDF rule body.
     Use the parameter directly: \`param == "east"\`, not \`input.row[param] == "east"\`.
     NEVER add catalog, schema, or group access checks inside UDF rules — those belong only in policy rules.
     UDF rules are pure helper functions: they only use their parameters and input.groups.

   Example — correct UDF translation (use the ACTUAL function name from the SQL, never "fname"):
     SQL:   CREATE FUNCTION region_filter(region STRING) RETURNS BOOLEAN
            RETURN (is_account_group_member('team-east') AND region = 'east')
                OR (is_account_group_member('team-west') AND region = 'west')
     Rego:  region_filter(region) if { "team-east" in input.groups; region == "east" }
            region_filter(region) if { "team-west" in input.groups; region == "west" }

── TRANSLATE: Policies ──────────────────────────────────────────────────────────
4. CREATE POLICY name
     ON CATALOG cat | ON SCHEMA cat.sch
     [COMMENT '...']
     ROW FILTER udf | COLUMN MASK udf
     TO \`account users\` [EXCEPT \`group_or_user\`]
     FOR TABLES MATCH COLUMNS expr AS alias [, expr AS alias2]
     [ON COLUMN target_alias]
     [USING COLUMNS (ctx_alias)]

   Scope check:
     ON CATALOG cat   → \`input.catalog == "cat"\`
     ON SCHEMA cat.sch → \`input.schema == "cat.sch"\`

   EXCEPT clause:
     EXCEPT \`some-group\`        → \`not "some-group" in input.groups\`
     EXCEPT \`user@example.com\`  → \`input.principal != "user@example.com"\`

   ROW FILTER policy → emit:
     row_visible["<name>"] if {
         <scope check>
         [not "except_group" in input.groups]
         some <alias>
         has_tag_value(<alias>, "<key>", "<val>")
         <udf>(input.row[<alias>])
     }

   COLUMN MASK (no USING COLUMNS) → emit:
     column_masked["<name>"][<alias>] := <udf>(input.row[<alias>]) if {
         <scope check>
         [not "except_group" in input.groups]
         has_tag_value(<alias>, "<key>", "<val>")
     }

   COLUMN MASK (with USING COLUMNS) → emit:
     column_masked["<name>"][<target>] := <udf>(input.row[<target>], input.row[<ctx>]) if {
         <scope check>
         [not "except_group" in input.groups]
         has_tag_value(<target>, "<target_key>", "<target_val>")
         some <ctx>
         has_tag(<ctx>, "<ctx_key>")
     }

── ALWAYS emit these at the top (once, in this order) ───────────────────────────
package databricks.abac

import future.keywords.if
import future.keywords.in

# Input schema:
# input.catalog:     string               — catalog name (e.g. "demos")
# input.schema:      string               — "catalog.schema"
# input.principal:   string               — user email or service principal
# input.groups:      [string]             — account-level group memberships
# input.column_tags: {col: {key: value}}  — live tag metadata from Unity Catalog
# input.row:         {col: value}         — row under evaluation

has_tag_value(col, key, val) if { input.column_tags[col][key] == val }
has_tag(col, key) if { _ := input.column_tags[col][key] }
${extraHint ? `\n── ADDITIONAL INSTRUCTIONS ─────────────────────────────────────────────────────\n${extraHint}\n` : ''}
Output: Rego ONLY. No explanation. No markdown fences. No prose.

## Databricks ABAC SQL
${abacContent}`;
}

// Registry — add new schema variants here without touching route or service logic
const PROMPT_TEMPLATES = {
  default: buildDefaultPrompt,
  // 'legacy-grant': buildLegacyGrantPrompt,  // future: old GRANT + ALTER TABLE SET ROW FILTER
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function stripMarkdownFences(text) {
  return text
    .replace(/^```rego\s*/im, '')
    .replace(/^```\s*/im, '')
    .replace(/\s*```\s*$/im, '')
    .trim();
}

// ── Mock fallback ─────────────────────────────────────────────────────────────
// Returned verbatim when neither OPENAI_API_KEY nor OPENAI_BASE_URL is set.
// Matches the validated output from the uc-governance-demo example.
const MOCK_REGO = `package databricks.abac

import future.keywords.if
import future.keywords.in

# Input schema:
# input.catalog:     string               — catalog name (e.g. "demos")
# input.schema:      string               — "catalog.schema"
# input.principal:   string               — user email or service principal
# input.groups:      [string]             — account-level group memberships
# input.column_tags: {col: {key: value}}  — live tag metadata from Unity Catalog
# input.row:         {col: value}         — row under evaluation

# ── Tag helper rules ──────────────────────────────────────────────────────────
has_tag_value(col, key, val) if { input.column_tags[col][key] == val }
has_tag(col, key) if { _ := input.column_tags[col][key] }

# ── UDF: mask_pii_string ──────────────────────────────────────────────────────
# Source: CREATE FUNCTION mask_pii_string(column_value STRING) RETURNS STRING
mask_pii_string(_) := "***REDACTED***"

# ── UDF: region_filter_abac ───────────────────────────────────────────────────
# Source: CREATE FUNCTION region_filter_abac(region STRING) RETURNS BOOLEAN
region_filter_abac(region) if {
    "analysts-east" in input.groups
    region == "east"
}

region_filter_abac(region) if {
    "analysts-west" in input.groups
    region == "west"
}

# ── Policy: mask_all_pii_strings (COLUMN MASK) ────────────────────────────────
# ON CATALOG demos | TO \`account users\` EXCEPT \`pii-readers\`
# MATCH COLUMNS has_tag_value('demo_sensitivity','pii') AS c | ON COLUMN c
column_masked["mask_all_pii_strings"][c] := mask_pii_string(input.row[c]) if {
    input.catalog == "demos"
    not "pii-readers" in input.groups
    has_tag_value(c, "demo_sensitivity", "pii")
}

# ── Policy: region_row_filter (ROW FILTER) ────────────────────────────────────
# ON CATALOG demos | TO \`account users\` EXCEPT \`pii-readers\`
# MATCH COLUMNS has_tag_value('demo_row_scope','region') AS region | USING COLUMNS (region)
row_visible["region_row_filter"] if {
    input.catalog == "demos"
    not "pii-readers" in input.groups
    some region_col
    has_tag_value(region_col, "demo_row_scope", "region")
    region_filter_abac(input.row[region_col])
}`;

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Build the prompt string for a given ABAC content + schema variant.
 * Exposed so the route can return the builtPrompt to the frontend.
 */
function buildOpaPrompt(abacContent, schemaVariant = 'default', extraHint = '') {
  const builder = PROMPT_TEMPLATES[schemaVariant] || PROMPT_TEMPLATES.default;
  return builder(abacContent, extraHint);
}

/**
 * Call OpenAI (or an OpenAI-compatible endpoint, e.g. Ollama via
 * OPENAI_BASE_URL) and return the generated Rego policy.
 *
 * @param {string} abacContent     SQL content to convert
 * @param {{ schemaVariant?, model?, customPrompt? }} opts
 * @returns {{ regoPolicy, builtPrompt, tokenUsage, mock }}
 */
async function generateOpaPolicy(abacContent, { schemaVariant = 'default', model, customPrompt } = {}) {
  const resolvedModel =
    model ||
    process.env.OPA_LLM_MODEL ||
    process.env.OPENAI_MODEL ||
    'gpt-4o';

  const prompt = customPrompt || buildOpaPrompt(abacContent, schemaVariant);

  if (!isLlmConfigured()) {
    return {
      regoPolicy: MOCK_REGO,
      builtPrompt: prompt,
      tokenUsage: { promptTokens: 0, completionTokens: 0 },
      mock: true,
    };
  }

  const response = await getClient().chat.completions.create({
    model: resolvedModel,
    messages: [{ role: 'user', content: prompt }],
    max_tokens: 1500,
    temperature: 0.1,
  });

  const raw = response.choices[0].message.content.trim();
  const regoPolicy = stripMarkdownFences(raw);

  return {
    regoPolicy,
    builtPrompt: prompt,
    tokenUsage: {
      promptTokens: response.usage?.prompt_tokens ?? 0,
      completionTokens: response.usage?.completion_tokens ?? 0,
    },
    mock: false,
  };
}

module.exports = { buildOpaPrompt, generateOpaPolicy, PROMPT_TEMPLATES };
