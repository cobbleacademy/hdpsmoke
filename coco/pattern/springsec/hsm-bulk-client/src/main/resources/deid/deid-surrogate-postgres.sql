-- The de-identification surrogate table (java/docs/DEIDENTIFICATION.md section 8), PostgreSQL.
-- Applied by the data owner's DBA or Flyway, in the owner's database; hsm-bulk-client
-- never creates tables. Only needed when a profile uses the surrogate method.
-- Back it up with the owner's other Tier 0 data: without it, surrogates already sent
-- can't be mapped back. The job only reads and inserts; it never updates or deletes.
--
-- The job's database user needs SELECT and INSERT on this table, nothing else.

CREATE TABLE deid_surrogate (
    scope          varchar(64)  NOT NULL,   -- the profile's surrogate-scope, normally the vendor
    domain         varchar(64)  NOT NULL,   -- e.g. member, card
    lookup_hash    bytea        NOT NULL,   -- HMAC(K_lookup, domain || 0x00 || normalize(value)), 32 bytes
    value_enc      bytea        NOT NULL,   -- IV || AES-GCM(K_enc, normalize(value)) || tag; AAD binds scope and domain
    key_edek_id    uuid         NOT NULL,   -- the surrogate-table key that encrypted value_enc
    surrogate      varchar(64)  NOT NULL,
    created_at     timestamp with time zone NOT NULL DEFAULT now(),
    created_run_id varchar(64)  NOT NULL,
    CONSTRAINT deid_surrogate_pk PRIMARY KEY (scope, domain, lookup_hash),
    CONSTRAINT deid_surrogate_uq UNIQUE (scope, domain, surrogate)
);

-- GRANT SELECT, INSERT ON deid_surrogate TO <job_user>;
