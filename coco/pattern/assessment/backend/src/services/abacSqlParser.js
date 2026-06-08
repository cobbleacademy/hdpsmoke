'use strict';

/**
 * Parse CREATE POLICY statements from Databricks Unity Catalog ABAC SQL.
 *
 * Handles three scope levels:
 *   ON CATALOG cat                         → scope:'catalog'  path:[cat]
 *   ON SCHEMA  cat.sch                     → scope:'schema'   path:[cat,sch]
 *   ON CATALOG cat … ON SCHEMA sch         → scope:'schema'   path:[cat,sch]
 *   ON TABLE   cat.sch.tbl                 → scope:'table'    path:[cat,sch,tbl]
 *
 * Backtick-quoted and unquoted identifiers are both handled.
 * One SQL string can produce entries at multiple levels (multi-policy files).
 *
 * @param {string} sql  Raw Databricks ABAC SQL
 * @returns {Array<{ policyName, catalog, schema, table, scope }>}
 */
function parseAbacPolicies(sql) {
  const policies = [];

  // Split on each CREATE POLICY boundary so we process one block at a time.
  // The split keeps the delimiter by using a lookahead.
  const blocks = sql.split(/(?=\bCREATE\s+(?:OR\s+REPLACE\s+)?POLICY\b)/i);

  for (const block of blocks) {
    const trimmed = block.trim();
    if (!/^CREATE\s+(?:OR\s+REPLACE\s+)?POLICY\b/i.test(trimmed)) continue;

    // ── Policy name ──────────────────────────────────────────────────────────
    const nameMatch = trimmed.match(
      /^CREATE\s+(?:OR\s+REPLACE\s+)?POLICY\s+`?([^`\s;]+)`?/i
    );
    if (!nameMatch) continue;
    const policyName = nameMatch[1].replace(/`/g, '');

    // ── Scope detection — most specific wins ─────────────────────────────────
    // TABLE:  ON TABLE cat.sch.tbl
    const tblM = trimmed.match(
      /\bON\s+TABLE\s+`?(\w+)`?\.`?(\w+)`?\.`?(\w+)`?/i
    );

    // SCHEMA full:  ON SCHEMA cat.sch
    const schFullM = trimmed.match(
      /\bON\s+SCHEMA\s+`?(\w+)`?\.`?(\w+)`?/i
    );

    // SCHEMA split: ON CATALOG cat … ON SCHEMA sch  (no dot in schema clause)
    const schSplitM = trimmed.match(
      /\bON\s+CATALOG\s+`?(\w+)`?[\s\S]*?\bON\s+SCHEMA\s+`?(\w+)`?(?!\s*\.)/i
    );

    // CATALOG: ON CATALOG cat
    const catM = trimmed.match(/\bON\s+CATALOG\s+`?(\w+)`?/i);

    let catalog = null, schema = null, table = null, scope = null;

    if (tblM) {
      [, catalog, schema, table] = tblM;
      scope = 'table';
    } else if (schFullM) {
      [, catalog, schema] = schFullM;
      scope = 'schema';
    } else if (schSplitM) {
      [, catalog, schema] = schSplitM;
      scope = 'schema';
    } else if (catM) {
      [, catalog] = catM;
      scope = 'catalog';
    }

    if (!policyName || !scope) continue;

    policies.push({
      policyName,
      catalog: catalog ? catalog.replace(/`/g, '') : null,
      schema:  schema  ? schema.replace(/`/g, '')  : null,
      table:   table   ? table.replace(/`/g, '')   : null,
      scope,
    });
  }

  return policies;
}

// ── Policy key helpers ────────────────────────────────────────────────────────

/**
 * Build a stable 4-segment key from the policy's path coordinates.
 * Format: catalog__schema__table__policyName  (empty string for absent levels)
 *
 * Examples:
 *   catalog-level:  "demos____mask_all_pii"
 *   schema-level:   "demos__customers___region_row_filter"
 *   table-level:    "demos__customers__profiles__ssn_mask"
 */
function buildPolicyKey(catalog, schema, table, policyName) {
  const norm = (s) => (s || '').toLowerCase().replace(/[^a-z0-9]/g, '_');
  return `${norm(catalog)}__${norm(schema)}__${norm(table)}__${norm(policyName)}`;
}

/**
 * Parse a policyKey back into its four components.
 * Empty segments map to null.
 */
function parsePolicyKey(key) {
  const [catalog, schema, table, policyName] = key.split('__');
  return {
    catalog:    catalog    || null,
    schema:     schema     || null,
    table:      table      || null,
    policyName: policyName || null,
  };
}

/**
 * Derive the scope from a parsed node (which fields are non-empty).
 */
function deriveScope(catalog, schema, table) {
  if (table)  return 'table';
  if (schema) return 'schema';
  return 'catalog';
}

module.exports = { parseAbacPolicies, buildPolicyKey, parsePolicyKey, deriveScope };
