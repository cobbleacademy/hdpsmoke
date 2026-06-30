// Group Permission Evaluation — per-environment schema mapping.
//
// Mirrors the RANGER_ENVS / RANGER_{ENV}_* pattern (rangerPersistService.js)
// and the PROVIDER_{ENV}_* pattern (providerService.js), extended to cover
// table/column *names* — not just connection strings or URLs — because each
// environment's real database may shape `users`/`groups` completely
// differently. See docs/adr/0015-group-permission-evaluation.md, Decision 5.
//
// ES module by design — see ADR-015, Decision 3.

const DEFAULT_SCHEMA = {
  dbSchema:             '',              // no schema prefix by default (resolves via search_path)
  usersTable:           'users',
  userKeyColumn:        'user_principal_name',
  userSamColumn:        '',             // optional SAM account column — empty = no SAM support
  userLocationColumn:   'location',
  groupsTable:          'groups',
  groupKeyColumn:       'group_id',
  groupLocationColumn:  'restricted_location',
  // Identity mapping — assumes the database stores the same literal tier
  // names this app uses internally. Override via PERMISSION_{ENV}_LOCATION_VALUE_MAP
  // when the real database uses different stored values (e.g. 'ON'/'NS'/'OS').
  locationValueMap: { ONSHORE: 'ONSHORE', NEARSHORE: 'NEARSHORE', OFFSHORE: 'OFFSHORE', NONE: 'NONE' },
};

// "ONSHORE:ON,NEARSHORE:NS,OFFSHORE:OS,NONE:NONE" → { ONSHORE: 'ON', ... }
function parseValueMap(raw) {
  if (!raw || !raw.trim()) return null;
  const map = {};
  for (const pair of raw.split(',')) {
    const [key, value] = pair.split(':').map((s) => s?.trim());
    if (key && value) map[key.toUpperCase()] = value;
  }
  return map;
}

/**
 * Returns the configured environment list — [{ id, label }] — safe to expose
 * to the browser (no connection strings, no schema details). Mirrors
 * getRangerEnvironments(): PERMISSION_ENVS unset → single "DEFAULT" env.
 */
export function getPermissionEnvironments() {
  const raw = process.env.PERMISSION_ENVS || '';
  if (!raw.trim()) {
    return [{ id: 'DEFAULT', label: 'Default' }];
  }
  return raw.split(',').map((s) => {
    const id = s.trim().toUpperCase();
    return { id, label: id };
  });
}

/**
 * Resolves the full server-side-only config (connection string + schema
 * mapping) for one environment. Throws with .code='UNKNOWN_ENV' if envId
 * isn't in the configured list, or .code='NO_DATABASE_URL' if that
 * environment has no connection string configured.
 */
export function getEnvConfig(envId) {
  const id = (envId || 'DEFAULT').toUpperCase();
  const known = getPermissionEnvironments().some((e) => e.id === id);
  if (!known) {
    throw Object.assign(new Error(`Unknown Group Permission environment: ${id}`), { code: 'UNKNOWN_ENV' });
  }

  const isDefault = id === 'DEFAULT';
  // Single-env mode (PERMISSION_ENVS unset) reads the original flat
  // DATABASE_URL var for backward compatibility with the pre-multi-env setup.
  const prefix = isDefault ? 'PERMISSION' : `PERMISSION_${id.replace(/-/g, '_')}`;
  const databaseUrl = process.env[`${prefix}_DATABASE_URL`] || (isDefault ? process.env.DATABASE_URL : undefined);

  if (!databaseUrl) {
    throw Object.assign(
      new Error(`No DATABASE_URL configured for environment ${id} (expected ${prefix}_DATABASE_URL)`),
      { code: 'NO_DATABASE_URL' }
    );
  }

  // SSL is OFF by default — the bundled local/demo Postgres (docker-compose's
  // postgres:16-alpine) has no certificate configured and would reject an SSL
  // handshake attempt. Real managed Postgres (Azure Database for PostgreSQL,
  // RDS, etc.) almost always *requires* SSL and rejects plaintext connections
  // with "no pg_hba.conf entry ... no encryption" — set {ENV}_DB_SSL=true for
  // those. rejectUnauthorized defaults to true (full chain validation); set
  // {ENV}_DB_SSL_REJECT_UNAUTHORIZED=false only if the provider's certificate
  // isn't in Node's trust store and you can't supply NODE_EXTRA_CA_CERTS.
  const dbSsl = process.env[`${prefix}_DB_SSL`] === 'true';
  const dbSslRejectUnauthorized = process.env[`${prefix}_DB_SSL_REJECT_UNAUTHORIZED`] !== 'false';

  return {
    databaseUrl,
    dbSsl,
    dbSslRejectUnauthorized,
    // Optional Postgres schema prefix — empty string means no prefix (resolves via search_path).
    // Set PERMISSION_{ENV}_SCHEMA=myschema to qualify all table references as myschema.tablename.
    dbSchema:            process.env[`${prefix}_SCHEMA`]               || DEFAULT_SCHEMA.dbSchema,
    usersTable:          process.env[`${prefix}_USERS_TABLE`]          || DEFAULT_SCHEMA.usersTable,
    userKeyColumn:       process.env[`${prefix}_USER_KEY_COLUMN`]      || DEFAULT_SCHEMA.userKeyColumn,
    // Optional SAM account column — when set, inputs without '@' (SAM accounts) use this column
    // instead of userKeyColumn for the WHERE lookup. Leave empty to disable SAM support.
    userSamColumn:       process.env[`${prefix}_USER_SAM_COLUMN`]      || DEFAULT_SCHEMA.userSamColumn,
    userLocationColumn:  process.env[`${prefix}_USER_LOCATION_COLUMN`] || DEFAULT_SCHEMA.userLocationColumn,
    groupsTable:         process.env[`${prefix}_GROUPS_TABLE`]         || DEFAULT_SCHEMA.groupsTable,
    groupKeyColumn:      process.env[`${prefix}_GROUP_KEY_COLUMN`]     || DEFAULT_SCHEMA.groupKeyColumn,
    groupLocationColumn: process.env[`${prefix}_GROUP_LOCATION_COLUMN`] || DEFAULT_SCHEMA.groupLocationColumn,
    locationValueMap: parseValueMap(process.env[`${prefix}_LOCATION_VALUE_MAP`]) || DEFAULT_SCHEMA.locationValueMap,
  };
}

// Postgres can only parameterize values, never identifiers (table/column
// names) — $1/$2 placeholders don't work for those. Every identifier pulled
// from env config is validated against this allowlist before being
// interpolated into a query string. Schema-qualified names ("schema.table")
// are allowed; anything else (quotes, semicolons, whitespace) is rejected.
const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$/;

export function assertSafeIdentifier(name, label) {
  if (typeof name !== 'string' || !IDENTIFIER_RE.test(name)) {
    throw Object.assign(new Error(`Invalid ${label} configured: ${JSON.stringify(name)}`), { code: 'INVALID_IDENTIFIER' });
  }
  return name;
}
