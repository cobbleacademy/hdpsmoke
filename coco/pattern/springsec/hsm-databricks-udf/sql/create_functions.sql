-- Registers hsm_encrypt/hsm_decrypt as Unity Catalog Python Functions.
-- Run once per catalog/schema you want them available in -- these are
-- governed catalog objects (see DATABRICKS_UDF_DESIGN.md §11), grantable
-- like any other Unity Catalog asset independent of which cluster/warehouse
-- a caller uses.
--
-- Two things this version fixes versus an earlier draft that relied on
-- cluster-attached libraries and a notebook %pip install: neither travels
-- with the function to a DIFFERENT caller invoking it later (a SQL
-- warehouse query, a job that never ran that notebook). Both replaced with
-- mechanisms that are part of the function's own definition instead:
--
--   1. The ENVIRONMENT clause below declares this package's wheel + its own
--      dependencies (cryptography, requests) directly on the function --
--      confirmed via Databricks' own CREATE FUNCTION docs, not assumed.
--      Resolved wherever the function actually executes, not wherever it
--      was registered from.
--   2. Credentials are resolved via dbutils.secrets.get(...) inside the
--      function body itself -- Unity Catalog's own governed secrets
--      mechanism (definer-rights model: the function's CREATOR needs access
--      to the secret scope; CALLERS only need EXECUTE on the function, never
--      the underlying secret). NOT independently verified against a live
--      Databricks workspace (this environment has no Databricks access) --
--      confirmed only that dbutils.secrets.get() is documented to work
--      inside a Unity Catalog Python function body; the exact scoping
--      (bare `dbutils` name vs needing an explicit import) should be
--      smoke-tested as the very first rollout step. If it doesn't resolve
--      as written, the fallback is cluster-level environment variables
--      (config.py already reads os.environ either way, unchanged).
--
-- Prerequisites (see DEPLOYMENT.md for the full walkthrough):
--   1. Build the wheel and upload it to a Unity Catalog volume.
--   2. Create a Databricks secret scope holding the bearer token and the
--      private key PEM; the function creator (whoever runs this script)
--      needs READ SECRET access to it.
--   3. This app_id already registered in hsm-core-service with a
--      dek_issue,dek_unwrap-capable token and its encryption_public_key_pem
--      provisioned via POST /admin/apps/keys (see java/docs/ADMIN_OPERATIONS.md).

CREATE OR REPLACE FUNCTION main.hsm.hsm_encrypt(
    plaintext STRING COMMENT 'The value to encrypt',
    dek_name STRING COMMENT 'Logical name for DEK reuse across many calls -- e.g. one per column, "customers.ssn"',
    data_classification STRING DEFAULT NULL COMMENT 'Optional tag, e.g. "pii" -- persisted for compliance queries, never enforced here'
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
    import os
    if "HSM_BEARER_TOKEN" not in os.environ:
        os.environ["HSM_SERVICE_BASE_URL"] = "https://hsm-core-service.internal:8443/api/sensec/hsm/v1"
        os.environ["HSM_APP_ID"] = "databricks-udf"
        os.environ["HSM_BEARER_TOKEN"] = dbutils.secrets.get(scope="hsm", key="databricks-udf-token")
        os.environ["HSM_PRIVATE_KEY_PEM"] = dbutils.secrets.get(scope="hsm", key="databricks-udf-private-key")
    from hsm_databricks_udf.udf import encrypt
    return encrypt(plaintext, dek_name, data_classification)
$$;

CREATE OR REPLACE FUNCTION main.hsm.hsm_decrypt(
    ciphertext_token STRING COMMENT 'A token produced by /encrypt, /encrypt/batch, or hsm_encrypt above'
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
    import os
    if "HSM_BEARER_TOKEN" not in os.environ:
        os.environ["HSM_SERVICE_BASE_URL"] = "https://hsm-core-service.internal:8443/api/sensec/hsm/v1"
        os.environ["HSM_APP_ID"] = "databricks-udf"
        os.environ["HSM_BEARER_TOKEN"] = dbutils.secrets.get(scope="hsm", key="databricks-udf-token")
        os.environ["HSM_PRIVATE_KEY_PEM"] = dbutils.secrets.get(scope="hsm", key="databricks-udf-private-key")
    from hsm_databricks_udf.udf import decrypt
    return decrypt(ciphertext_token)
$$;

-- Grant usage to whichever principals should be able to call these --
-- independent of which cluster/warehouse they use, since these are Unity
-- Catalog-governed objects (see DATABRICKS_UDF_DESIGN.md §11). Callers need
-- only EXECUTE here -- never access to the `hsm` secret scope itself, per
-- Unity Catalog's definer-rights model for secrets used inside a function.
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_encrypt TO `data-engineers`;
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_decrypt TO `data-engineers`;
