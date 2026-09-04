-- Closes a real gap found during Spark-adapter testing: a dek_name was
-- previously scoped per-app (idx_edek_current_name on (app_id,
-- current_dek_name), V7), so two different apps could each independently
-- mint their own DEK under the identical dek_name string with zero
-- relationship or enforcement between them. Not a data leak (each row stayed
-- fully scoped to its own app_id; decrypt never even looks at dek_name, only
-- edek_id), but no ownership concept existed at all, so a naming collision
-- between two unrelated apps was silently accepted instead of rejected.
--
-- Fix: a dek_name may now be "current" for at most ONE app across the whole
-- system. Ownership is first-encrypt-wins -- whichever app's EdekRecord
-- already holds a given current_dek_name is that name's owner from then on;
-- any other app trying to /encrypt, /encrypt/batch, or /dek/issue under that
-- same name is now rejected (EncryptionService.resolveDek /
-- DekIssueService.issueOne) unless authorized via the grants below. The
-- symmetric read-side check (DecryptionService / DekUnwrapService) already
-- existed via app_decrypt_grants; this migration generalizes that existing
-- mechanism rather than leaving it as a separate, decrypt-only concept.
--
-- Reconcile pre-existing cross-app dek_name collisions before enforcing
-- global uniqueness below. Before this migration, current_dek_name was
-- unique only within (app_id, current_dek_name) -- so two different apps
-- could each already have a row with the identical current_dek_name, both
-- legitimately "current" under the old rule. The new global unique index
-- cannot coexist with that; it must be created against a clean set.
--
-- Resolution applies the SAME first-encrypt-wins rule this migration
-- establishes going forward: for each duplicated current_dek_name, the
-- OLDEST row (earliest created_at, NULLS LAST -- a row with no timestamp at
-- all must never win over one with a real, earlier one; edek_id as a final
-- deterministic tiebreak) keeps the name and becomes its owner; every other
-- row sharing that name has current_dek_name set to NULL. This does not
-- touch dek_name (permanent, unaffected -- audit history is preserved) or
-- edek_id, and has NO effect on decryptability of anything already
-- encrypted -- /decrypt and /dek/unwrap are keyed by edek_id, never
-- current_dek_name. The only real-world effect is on the *losing* app(s):
-- their next /encrypt, /encrypt/batch, or /dek/issue call under that
-- dek_name mints a genuinely fresh DEK instead of continuing to silently
-- reuse the old shared one -- i.e. this is exactly the "even if it breaks
-- the current (accidental, ungranted) sharing" tradeoff the fix is for. If
-- that losing app actually needs continued access to the winner's DEK, grant
-- it explicitly afterward via POST /admin/grants or /admin/dek-grants rather
-- than relying on the accidental name collision.
--
-- Idempotent and a no-op on a fresh/demo DB (nothing to reconcile when no
-- two rows ever shared a current_dek_name -- confirmed the case for this
-- project's own demo/test data). Must run before the index below.
UPDATE ${crypto_schema}.edek_records
SET current_dek_name = NULL
WHERE current_dek_name IS NOT NULL
  AND edek_id <> (
      SELECT e2.edek_id
      FROM ${crypto_schema}.edek_records e2
      WHERE e2.current_dek_name = edek_records.current_dek_name
      ORDER BY e2.created_at ASC NULLS LAST, e2.edek_id ASC
      LIMIT 1
  );

DROP INDEX idx_edek_current_name;
CREATE UNIQUE INDEX idx_edek_current_name ON ${crypto_schema}.edek_records (current_dek_name);

-- app_decrypt_grants (V1) is replaced, not extended: it has no scope column
-- at all (every row implicitly meant "decrypt"), and adding a NOT NULL scope
-- column to an existing table with existing rows needs a backfill this
-- project's migrations don't otherwise need to carry. Dropping and
-- recreating as a generalized, scope-aware table is simpler than an ALTER +
-- backfill for no real benefit -- existing rows are carried forward by the
-- INSERT ... SELECT below before the DROP, so no grant is silently lost even
-- on a deployment with real (non-demo) data.
--
-- scope is a plain, unconstrained column, not a DB-level enum or CHECK
-- constraint -- deliberately, so a future third grant type (beyond
-- encrypt/decrypt) needs no schema migration at all to add, the same
-- low-friction extensibility app_registrations.allowed_scopes already has
-- (also a free-text column, not a fixed list). Validation of which scope
-- values currently mean something real happens at the application layer
-- (AdminController), not here -- see its own comment for why.
--
-- Two tables, not one, and not four: app_grants is coarse (granteeAppId may
-- use ANY dek_name ownerAppId owns, for the given scope); app_dek_grants is
-- fine-grained (granteeAppId may use SPECIFICALLY this one dek_name of
-- ownerAppId's). Checked in that order (coarse first) by
-- AppRegistryService.isGranted. Both scopes (encrypt and decrypt) share the
-- same two tables via the scope column, rather than four near-identical
-- scope-specific tables -- consistent with the allowed_scopes precedent
-- above, and avoids real duplication for no structural benefit.
CREATE TABLE ${access_schema}.app_grants (
    grantee_app_id VARCHAR(128) NOT NULL,
    owner_app_id   VARCHAR(128) NOT NULL,
    scope          VARCHAR(32)  NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (grantee_app_id, owner_app_id, scope)
);

CREATE TABLE ${access_schema}.app_dek_grants (
    grantee_app_id VARCHAR(128) NOT NULL,
    owner_app_id   VARCHAR(128) NOT NULL,
    dek_name       VARCHAR(256) NOT NULL,
    scope          VARCHAR(32)  NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (grantee_app_id, owner_app_id, dek_name, scope)
);

-- Preserve every existing grant rather than silently dropping it: every row
-- in app_decrypt_grants implicitly meant "decrypt" (it had no scope column),
-- so it maps 1:1 onto a coarse app_grants row with scope='decrypt' -- exactly
-- the coarse-grant behavior those apps already had. Runs before the DROP
-- below. No-op on a fresh/demo DB with no rows to carry forward.
INSERT INTO ${access_schema}.app_grants (grantee_app_id, owner_app_id, scope, created_at)
SELECT grantee_app_id, owner_app_id, 'decrypt', created_at
FROM ${access_schema}.app_decrypt_grants;

DROP TABLE ${access_schema}.app_decrypt_grants;
