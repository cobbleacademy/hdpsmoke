-- Named-DEK reuse: dek_name lets a caller ask for the SAME DEK across many
-- encrypt calls (e.g. one DEK per logical column, "customers.ssn", instead of
-- one per value) instead of always minting a fresh one. NULL for every
-- existing/default per-value issuance path -- fully backward compatible, and
-- hsm-core-service's real /decrypt never looks at this column at all (it
-- resolves purely by edek_id embedded in the ciphertext_token).
--
-- current_dek_name is a separate, nullable shadow column, not the same thing
-- as dek_name: this project's migrations must run unmodified on both Postgres
-- and H2 (demo mode, see V1's own comment), and Postgres's partial-unique-index
-- syntax (CREATE UNIQUE INDEX ... WHERE ...) has no H2 equivalent (verified
-- directly -- H2 2.4.240 rejects it with a syntax error). The standard portable
-- workaround: current_dek_name mirrors dek_name only while rotation_status is
-- 'current', and is set back to NULL the moment a row rotates away from
-- 'current' -- a PLAIN (non-partial) unique index on (app_id, current_dek_name)
-- then gets the exact same effect on both databases, since ANSI SQL unique
-- indexes never consider NULL equal to another NULL (verified: multiple NULL
-- current_dek_name rows insert freely; two 'ssn'/'ssn' rows do not). dek_name
-- itself is never nulled out on rotation, so historical/rotated rows keep
-- their name for audit purposes even after current_dek_name clears.
ALTER TABLE ${crypto_schema}.edek_records ADD COLUMN dek_name VARCHAR(256);
ALTER TABLE ${crypto_schema}.edek_records ADD COLUMN current_dek_name VARCHAR(256);

CREATE UNIQUE INDEX idx_edek_current_name ON ${crypto_schema}.edek_records (app_id, current_dek_name);
