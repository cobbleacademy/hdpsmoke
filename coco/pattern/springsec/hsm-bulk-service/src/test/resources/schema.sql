-- Test-only schema, hand-written to match the combined shape of
-- hsm-core-service's V1/V5/V6 migrations (edek_records, app_registrations +
-- public_key_pem, app_decrypt_grants) -- hsm-bulk-service itself never runs
-- Flyway (schema consumer, not owner; see pom.xml), so tests need their own
-- bootstrap. Auto-run by Spring Boot against the embedded H2 test datasource
-- (spring.sql.init.mode: always, see src/test/resources/application.yml).

CREATE TABLE edek_records (
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
    rotated_at          TIMESTAMP WITH TIME ZONE,
    fingerprint         VARCHAR(16)
);

CREATE TABLE app_registrations (
    app_id         VARCHAR(128) PRIMARY KEY,
    allowed_scopes VARCHAR(512) NOT NULL,
    description    VARCHAR(512) NOT NULL DEFAULT '',
    active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMP WITH TIME ZONE,
    updated_at     TIMESTAMP WITH TIME ZONE,
    public_key_pem TEXT
);

CREATE TABLE app_decrypt_grants (
    grantee_app_id VARCHAR(128) NOT NULL,
    owner_app_id   VARCHAR(128) NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (grantee_app_id, owner_app_id)
);
