-- Multi-KEK support: KEK selection keyed by (app_id, dek_name), resolved via
-- kek_registry -- not one single, globally-configured KEK for the whole
-- service. Resolution happens exactly once, when minting a brand-new EDEK;
-- every EDEK then carries its own resolved kek_name forward permanently
-- (see edek_records changes below), so nothing downstream of encrypt time
-- (decrypt, rotation) ever needs to re-consult this table again.
--
-- Empty string ('', not NULL) is the "not set" sentinel for dek_name and
-- data_classification, the same portable-across-Postgres-and-H2 workaround
-- V7's own comment already documents for current_dek_name: a composite
-- PRIMARY KEY across nullable columns doesn't reliably enforce "one row per
-- combination" the same way on every database, since ANSI SQL never treats
-- two NULLs as equal. A plain (non-partial) primary key over '' sentinels
-- avoids that entirely and needs no database-specific syntax.
--
-- Exactly one row exists per app_id per resolution tier:
--   (app_id, '<dek_name>', '')              -- tier 1: exact dek_name match
--   (app_id, '', '<classification>')        -- tier 2: classification fallback
--   (app_id, '', '')                        -- tier 3: per-app default
-- An app_id with no row at any tier falls back to the legacy single-KEK
-- config value (hsm.service.azure.kek-name) -- the one deliberate exception
-- to "unprovisioned combinations fail closed": failing closed here would
-- break every existing app the moment this table exists, since none of them
-- would have any rows in it yet. See KekRegistryService.
CREATE TABLE ${crypto_schema}.kek_registry (
    app_id              VARCHAR(128) NOT NULL,
    dek_name            VARCHAR(256) NOT NULL DEFAULT '',
    data_classification VARCHAR(32) NOT NULL DEFAULT '',
    kek_name            VARCHAR(127) NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE,
    updated_at          TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (app_id, dek_name, data_classification)
);

-- kek_name: which KEK actually wrapped this row's edek_blob, alongside the
-- existing kek_version -- required, not just nice-to-have, once there's more
-- than one KEK: kek_version alone ("version 3") is meaningless without
-- knowing which key it's a version of. NULL on existing rows (written before
-- this column existed) means "the single legacy KEK from static config",
-- same backward-compatible-NULL pattern V7 already used for dek_name.
--
-- previous_kek_name / previous_kek_version / previous_edek_blob: a
-- single-level undo buffer for the "rekey" operation (moving an EDEK from
-- one KEK to a different one -- compromise response, retroactive isolation
-- changes, key decommissioning), not a multi-row history table. rekey
-- copies the row's current kek_name/kek_version/edek_blob into these columns
-- before overwriting them; reversion swaps them back and clears these. This
-- mutates in place deliberately, matching the existing rewrapRecord pattern
-- for routine version rotation, rather than inserting a new row per rotation
-- event the way rotateNamedDek does -- that would grow this table
-- unboundedly on every rotation with no real benefit, since the full,
-- unbounded historical trail already belongs in the audit log, not here.
ALTER TABLE ${crypto_schema}.edek_records ADD COLUMN kek_name VARCHAR(127);
ALTER TABLE ${crypto_schema}.edek_records ADD COLUMN previous_kek_name VARCHAR(127);
ALTER TABLE ${crypto_schema}.edek_records ADD COLUMN previous_kek_version VARCHAR(64);
ALTER TABLE ${crypto_schema}.edek_records ADD COLUMN previous_edek_blob TEXT;

CREATE INDEX idx_edek_kek_name ON ${crypto_schema}.edek_records (kek_name);
