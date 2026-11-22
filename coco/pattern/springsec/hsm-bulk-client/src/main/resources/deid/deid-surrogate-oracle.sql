-- The de-identification surrogate table (java/docs/DEIDENTIFICATION.md section 8), Oracle.
-- Applied by the data owner's DBA or Flyway, in the owner's database; hsm-bulk-client
-- never creates tables. Only needed when a profile uses the surrogate method.
-- Back it up with the owner's other Tier 0 data: without it, surrogates already sent
-- can't be mapped back. The job only reads and inserts; it never updates or deletes.
--
-- The job's database user needs SELECT and INSERT on this table, nothing else.
-- key_edek_id is the UUID's text form (36 characters).

CREATE TABLE deid_surrogate (
    scope          VARCHAR2(64)                NOT NULL,   -- the profile's surrogate-scope, normally the vendor
    domain         VARCHAR2(64)                NOT NULL,   -- e.g. member, card
    lookup_hash    RAW(32)                     NOT NULL,   -- HMAC(K_lookup, domain || 0x00 || normalize(value))
    value_enc      BLOB                        NOT NULL,   -- IV || AES-GCM(K_enc, normalize(value)) || tag; AAD binds scope and domain
    key_edek_id    VARCHAR2(36)                NOT NULL,   -- the surrogate-table key that encrypted value_enc
    surrogate      VARCHAR2(64)                NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE    DEFAULT SYSTIMESTAMP NOT NULL,
    created_run_id VARCHAR2(64)                NOT NULL,
    CONSTRAINT deid_surrogate_pk PRIMARY KEY (scope, domain, lookup_hash),
    CONSTRAINT deid_surrogate_uq UNIQUE (scope, domain, surrogate)
);

-- GRANT SELECT, INSERT ON deid_surrogate TO <job_user>;
