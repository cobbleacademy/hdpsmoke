-- Group Permission Evaluation — schema + seed data for three demo environments.
--
-- This script demonstrates the per-environment schema-mapping config (see
-- docs/adr/0015-group-permission-evaluation.md, Decision 5): DEV, NPE, and
-- PROD intentionally use DIFFERENT table names, column names, and (for PROD)
-- different literal tier codes — proving that permissionDb.mjs's dynamic
-- query builder works against genuinely different real-world schemas, not
-- just the one shape baked into the demo.
--
-- Hierarchy (higher weight = more privileged), independent of how each
-- environment spells it:
--   ONSHORE     = 3
--   NEARSHORE   = 2
--   OFFSHORE    = 1
--   NONE        = 0   (no restriction — any user may access)
--
-- A user may access a group when their location's weight is >= the group's
-- restricted-location weight. Unknown user or unknown group → DENY (fail
-- closed, never fail open on a lookup miss).
--
-- This script is mounted into the postgres container at
-- /docker-entrypoint-initdb.d/init.sql, so it runs once automatically on
-- first container start against an empty data volume. CREATE DATABASE
-- cannot run inside a transaction block — docker's init runner executes each
-- statement individually via psql, so this works as a single script.

-- ════════════════════════════════════════════════════════════════════════════
-- DEV — also the single-env "DEFAULT" fallback shape (PERMISSION_ENVS unset)
-- ════════════════════════════════════════════════════════════════════════════
-- Connects to the default database created by POSTGRES_DB (permission_db).

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'location_tier') THEN
    CREATE TYPE location_tier AS ENUM ('ONSHORE', 'NEARSHORE', 'OFFSHORE', 'NONE');
  END IF;
END$$;

-- A user always belongs to one of the three real tiers — 'NONE' is a
-- group-only concept (no restriction), so it's explicitly disallowed here.
CREATE TABLE IF NOT EXISTS users (
  user_principal_name VARCHAR(255) PRIMARY KEY,
  location             location_tier NOT NULL,
  CONSTRAINT users_location_not_none CHECK (location <> 'NONE')
);

-- restricted_location defaults to 'NONE' (unrestricted) when a group has no
-- explicit tier requirement.
CREATE TABLE IF NOT EXISTS groups (
  group_id            VARCHAR(64) PRIMARY KEY,
  restricted_location  location_tier NOT NULL DEFAULT 'NONE'
);

INSERT INTO users (user_principal_name, location) VALUES
  ('onshore.user@company.com',   'ONSHORE'),
  ('nearshore.user@company.com', 'NEARSHORE'),
  ('offshore.user@company.com',  'OFFSHORE'),
  ('test@company.com',           'NEARSHORE')
ON CONFLICT (user_principal_name) DO NOTHING;

INSERT INTO groups (group_id, restricted_location) VALUES
  ('Alpha12',  'ONSHORE'),
  ('Beta07',   'NEARSHORE'),
  ('Gamma99',  'OFFSHORE'),
  ('Public01', 'NONE')
ON CONFLICT (group_id) DO NOTHING;

-- ════════════════════════════════════════════════════════════════════════════
-- NPE — a differently-named, differently-shaped schema (same literal tiers)
-- ════════════════════════════════════════════════════════════════════════════

CREATE DATABASE permission_npe;
\connect permission_npe

CREATE TABLE identity_users (
  upn         VARCHAR(255) PRIMARY KEY,
  region_tier VARCHAR(20) NOT NULL CHECK (region_tier <> 'NONE')
);

CREATE TABLE resource_groups (
  grp_id      VARCHAR(64) PRIMARY KEY,
  access_tier VARCHAR(20) NOT NULL DEFAULT 'NONE'
);

INSERT INTO identity_users (upn, region_tier) VALUES
  ('onshore.user@company.com',   'ONSHORE'),
  ('nearshore.user@company.com', 'NEARSHORE'),
  ('offshore.user@company.com',  'OFFSHORE'),
  ('test@company.com',           'NEARSHORE')
ON CONFLICT (upn) DO NOTHING;

INSERT INTO resource_groups (grp_id, access_tier) VALUES
  ('Alpha12',  'ONSHORE'),
  ('Beta07',   'NEARSHORE'),
  ('Gamma99',  'OFFSHORE'),
  ('Public01', 'NONE')
ON CONFLICT (grp_id) DO NOTHING;

-- ════════════════════════════════════════════════════════════════════════════
-- PROD — different shape AND abbreviated tier codes (ON/NS/OS instead of
-- the full ONSHORE/NEARSHORE/OFFSHORE names) — exercises locationValueMap.
-- ════════════════════════════════════════════════════════════════════════════

CREATE DATABASE permission_prod;
\connect permission_prod

CREATE TABLE app_users (
  email    VARCHAR(255) PRIMARY KEY,
  loc_tier VARCHAR(10) NOT NULL CHECK (loc_tier <> 'NONE')
);

CREATE TABLE app_groups (
  code     VARCHAR(64) PRIMARY KEY,
  min_tier VARCHAR(10) NOT NULL DEFAULT 'NONE'
);

INSERT INTO app_users (email, loc_tier) VALUES
  ('onshore.user@company.com',   'ON'),
  ('nearshore.user@company.com', 'NS'),
  ('offshore.user@company.com',  'OS'),
  ('test@company.com',           'NS')
ON CONFLICT (email) DO NOTHING;

INSERT INTO app_groups (code, min_tier) VALUES
  ('Alpha12',  'ON'),
  ('Beta07',   'NS'),
  ('Gamma99',  'OS'),
  ('Public01', 'NONE')
ON CONFLICT (code) DO NOTHING;

-- ════════════════════════════════════════════════════════════════════════════
-- Reference copy of the evaluation query — DEV shape, generalized form lives
-- in backend/src/services/permissionDb.mjs (kept in sync with this comment).
-- ════════════════════════════════════════════════════════════════════════════
--
-- WITH location_weights (location, weight) AS (
--   VALUES ('ONSHORE'::text, 3), ('NEARSHORE'::text, 2), ('OFFSHORE'::text, 1), ('NONE'::text, 0)
-- ),
-- user_weight AS (
--   SELECT lw.weight AS weight
--   FROM users u
--   JOIN location_weights lw ON lw.location = u.location::text
--   WHERE u.user_principal_name = $1
-- ),
-- group_weight AS (
--   SELECT lw.weight AS weight
--   FROM groups g
--   JOIN location_weights lw ON lw.location = g.restricted_location::text
--   WHERE g.group_id = $2
-- )
-- SELECT
--   CASE
--     WHEN (SELECT weight FROM user_weight)  IS NULL THEN 'DENY'
--     WHEN (SELECT weight FROM group_weight) IS NULL THEN 'DENY'
--     WHEN (SELECT weight FROM user_weight) >= (SELECT weight FROM group_weight)
--       THEN 'PERMIT'
--     ELSE 'DENY'
--   END AS permission_status;
