// Group Permission Evaluation — database access layer.
//
// ES module by design (see docs/adr/0015-group-permission-evaluation.md) —
// scoped to this feature's files only; the rest of the backend stays CommonJS.

import pg from 'pg';
import { getEnvConfig, assertSafeIdentifier } from './permissionConfigService.mjs';

const { Pool } = pg;

// One Pool per environment, lazily created and cached — each environment is
// a genuinely different physical database, not just different config.
const _pools = new Map();

function getPool(envId, databaseUrl) {
  if (!_pools.has(envId)) {
    _pools.set(envId, new Pool({ connectionString: databaseUrl }));
  }
  return _pools.get(envId);
}

/**
 * Evaluates whether userPrincipalName may access groupId in the given
 * environment, per the ONSHORE > NEARSHORE > OFFSHORE > NONE hierarchy.
 * Returns 'PERMIT' or 'DENY'.
 *
 * Table/column names come from server-side env config (never request input)
 * and are allowlist-validated before being interpolated into the query —
 * Postgres has no parameter syntax for identifiers, only values. The four
 * tier literals (which may differ per environment, e.g. 'ON'/'NS'/'OS') are
 * still passed as real query parameters alongside userPrincipalName/groupId.
 */
export async function checkPermission(userPrincipalName, groupId, envId = 'DEFAULT') {
  const cfg = getEnvConfig(envId);

  const usersTable          = assertSafeIdentifier(cfg.usersTable, 'usersTable');
  const userKeyColumn       = assertSafeIdentifier(cfg.userKeyColumn, 'userKeyColumn');
  const userLocationColumn  = assertSafeIdentifier(cfg.userLocationColumn, 'userLocationColumn');
  const groupsTable         = assertSafeIdentifier(cfg.groupsTable, 'groupsTable');
  const groupKeyColumn      = assertSafeIdentifier(cfg.groupKeyColumn, 'groupKeyColumn');
  const groupLocationColumn = assertSafeIdentifier(cfg.groupLocationColumn, 'groupLocationColumn');

  const vm = cfg.locationValueMap;

  // Mirrors the commented reference query in backend/db/init.sql, generalized
  // to dynamic table/column names and a per-env tier-value mapping. The
  // location columns are explicitly cast to ::text before comparison —
  // some environments (e.g. this app's own DEV demo schema) use a Postgres
  // ENUM column, others use plain VARCHAR; without the cast, comparing an
  // ENUM column to the location_weights CTE's text values fails with
  // "operator does not exist: text = location_tier".
  const query = `
    WITH location_weights (location, weight) AS (
      VALUES ($3::text, 3), ($4::text, 2), ($5::text, 1), ($6::text, 0)
    ),
    user_weight AS (
      SELECT lw.weight AS weight
      FROM ${usersTable} u
      JOIN location_weights lw ON lw.location = u.${userLocationColumn}::text
      WHERE u.${userKeyColumn} = $1
    ),
    group_weight AS (
      SELECT lw.weight AS weight
      FROM ${groupsTable} g
      JOIN location_weights lw ON lw.location = g.${groupLocationColumn}::text
      WHERE g.${groupKeyColumn} = $2
    )
    SELECT
      CASE
        WHEN (SELECT weight FROM user_weight)  IS NULL THEN 'DENY'
        WHEN (SELECT weight FROM group_weight) IS NULL THEN 'DENY'
        WHEN (SELECT weight FROM user_weight) >= (SELECT weight FROM group_weight)
          THEN 'PERMIT'
        ELSE 'DENY'
      END AS permission_status;
  `;

  const params = [userPrincipalName, groupId, vm.ONSHORE, vm.NEARSHORE, vm.OFFSHORE, vm.NONE];
  const pool = getPool(envId, cfg.databaseUrl);
  const { rows } = await pool.query(query, params);
  return rows[0]?.permission_status ?? 'DENY';
}
