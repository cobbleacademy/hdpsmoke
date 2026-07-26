-- Ported from migrations/versions/0001_initial_schema.py (Alembic revision 0001).
-- Schema names are Flyway placeholders resolved from CRYPTO_SCHEMA / ACCESS_SCHEMA
-- (default "public" when unset) -- see spring.flyway.placeholders in application.yml.
--
-- rotation_status uses VARCHAR + CHECK instead of a native Postgres ENUM so this
-- single script runs unmodified on both Postgres and H2 (demo mode).

CREATE SCHEMA IF NOT EXISTS ${crypto_schema};
CREATE SCHEMA IF NOT EXISTS ${access_schema};

CREATE TABLE ${crypto_schema}.edek_records (
    edek_id             UUID PRIMARY KEY,
    app_id              VARCHAR(128) NOT NULL,
    edek_blob           TEXT NOT NULL,
    kek_version         VARCHAR(64) NOT NULL,
    algorithm           VARCHAR(32) NOT NULL DEFAULT 'AES-256-GCM',
    encoding            VARCHAR(16) NOT NULL DEFAULT 'utf8',
    data_classification VARCHAR(32),
    rotation_status     VARCHAR(16) NOT NULL DEFAULT 'current'
                             CHECK (rotation_status IN ('current', 'pending', 'rotated')),
    created_at          TIMESTAMP WITH TIME ZONE,
    rotated_at          TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_edek_app_id ON ${crypto_schema}.edek_records (app_id);
CREATE INDEX idx_edek_rotation_status ON ${crypto_schema}.edek_records (rotation_status);
CREATE INDEX idx_edek_kek_version ON ${crypto_schema}.edek_records (kek_version);
CREATE INDEX idx_edek_classification ON ${crypto_schema}.edek_records (data_classification);
CREATE INDEX idx_edek_created_at ON ${crypto_schema}.edek_records (created_at);

CREATE TABLE ${access_schema}.app_registrations (
    app_id         VARCHAR(128) PRIMARY KEY,
    allowed_scopes VARCHAR(512) NOT NULL,
    description    VARCHAR(512) NOT NULL DEFAULT '',
    active         BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE ${access_schema}.app_decrypt_grants (
    grantee_app_id VARCHAR(128) NOT NULL,
    owner_app_id   VARCHAR(128) NOT NULL,
    PRIMARY KEY (grantee_app_id, owner_app_id)
);
