-- Registers hsm_encrypt/hsm_decrypt as Unity Catalog Python Functions.
-- Run once per catalog/schema you want them available in -- these are
-- governed catalog objects (see DATABRICKS_UDF_DESIGN.md §11), grantable
-- like any other Unity Catalog asset independent of which cluster/warehouse
-- a caller uses.
--
-- The ENVIRONMENT clause below declares this package's wheel + its own
-- dependencies (cryptography, requests) directly on the function --
-- confirmed via Databricks' own CREATE FUNCTION docs, not assumed. Resolved
-- wherever the function actually executes, not wherever it was registered
-- from.
--
-- CREDENTIALS: passed in as an explicit `credentials_json` argument on
-- every call, NOT resolved inside the function body via dbutils.secrets.get().
-- An earlier version of this script tried the latter and it does not work:
-- confirmed directly against Databricks' own docs/KB -- dbutils is only
-- available in the calling notebook/job's own driver context, never inside
-- a UDF or Unity Catalog Python Function body (this applies to
-- CREATE FUNCTION ... LANGUAGE PYTHON specifically, not just plain PySpark
-- UDFs registered via udf()). Calling it from inside AS $$ ... $$ raises
-- NameError: name 'dbutils' is not defined (or a permissions error, in the
-- cases where Databricks Runtime does inject a restricted dbutils object).
--
-- The fix Databricks documents for this exact failure: fetch the secret on
-- the caller's side, where dbutils DOES work, and pass it into the function
-- call as an argument. See DEPLOYMENT.md for the full calling pattern from
-- a notebook/job (dbutils.secrets.get(...) -> build a small JSON object ->
-- pass as a bind parameter, never interpolated into SQL text).
--
-- credentials_json is a JSON object with the same field names as this
-- package's HSM_* environment variables -- see config.py's Config.from_json.
-- Minimal example (STATIC auth):
--   {"HSM_SERVICE_BASE_URL": "https://hsm-core-service.internal:8443/api/sensec/hsm/v1",
--    "HSM_APP_ID": "databricks-udf", "HSM_AUTH_MODE": "STATIC",
--    "HSM_PRIVATE_KEY_PEM": "-----BEGIN PRIVATE KEY-----...",
--    "HSM_BEARER_TOKEN": "..."}
-- SELF_SIGNED_JWT swaps the last two fields for HSM_SIGNING_PRIVATE_KEY_PEM
-- (and optionally HSM_SELF_SIGNED_AUDIENCE) instead of HSM_BEARER_TOKEN --
-- see DEPLOYMENT.md.
--
-- Prerequisites (see DEPLOYMENT.md for the full walkthrough):
--   1. Build the wheel and upload it to a Unity Catalog volume.
--   2. Create a Databricks secret scope holding whichever credential your
--      chosen auth mode needs; whoever CALLS the function (not whoever
--      registers it) needs READ SECRET access to it, since the secret is
--      fetched at call time in the caller's own notebook/job.
--   3. This app_id already registered in hsm-core-service with a
--      dek_issue,dek_unwrap-capable token/signing key and its
--      encryption_public_key_pem provisioned via POST /admin/apps/keys
--      (see java/docs/ADMIN_OPERATIONS.md).

CREATE OR REPLACE FUNCTION main.hsm.hsm_encrypt(
    plaintext STRING COMMENT 'The value to encrypt',
    dek_name STRING COMMENT 'Logical name for DEK reuse across many calls -- e.g. one per column, "customers.ssn"',
    data_classification STRING COMMENT 'Optional tag, e.g. "pii" -- persisted for compliance queries, never enforced here; pass NULL if not used',
    credentials_json STRING COMMENT 'JSON object with this app''s HSM_* config -- fetched via dbutils.secrets.get(...) by the CALLER, never hardcoded here; see DEPLOYMENT.md'
)
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (
  dependencies = '["cryptography>=42.0", "requests>=2.31",
                   "/Volumes/main/hsm/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl"]',
  environment_version = 'None'
)
COMMENT 'Encrypts plaintext, returning hsm-core-service''s own ciphertext_token wire format -- decryptable through the ordinary /decrypt endpoint or hsm_decrypt below.'
AS $$
    from hsm_databricks_udf.udf import encrypt
    return encrypt(plaintext, dek_name, data_classification, credentials_json)
$$;

CREATE OR REPLACE FUNCTION main.hsm.hsm_decrypt(
    ciphertext_token STRING COMMENT 'A token produced by /encrypt, /encrypt/batch, or hsm_encrypt above',
    credentials_json STRING COMMENT 'JSON object with this app''s HSM_* config -- fetched via dbutils.secrets.get(...) by the CALLER, never hardcoded here; see DEPLOYMENT.md'
)
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (
  dependencies = '["cryptography>=42.0", "requests>=2.31",
                   "/Volumes/main/hsm/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl"]',
  environment_version = 'None'
)
COMMENT 'Decrypts a ciphertext_token, resolving its owning app via hsm-core-service -- fails the row (not silently null) on a denied grant or a malformed token.'
AS $$
    from hsm_databricks_udf.udf import decrypt
    return decrypt(ciphertext_token, credentials_json)
$$;

-- Grant usage to whichever principals should be able to call these --
-- independent of which cluster/warehouse they use, since these are Unity
-- Catalog-governed objects (see DATABRICKS_UDF_DESIGN.md §11). Callers still
-- need their own READ SECRET grant on the `hsm` scope to build
-- credentials_json themselves -- EXECUTE on the function alone is not
-- enough now that credentials travel as a caller-supplied argument rather
-- than being resolved inside the function body.
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_encrypt TO `data-engineers`;
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_decrypt TO `data-engineers`;
