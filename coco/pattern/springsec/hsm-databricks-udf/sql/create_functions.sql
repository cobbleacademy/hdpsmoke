-- Registers hsm_encrypt/hsm_decrypt as Unity Catalog Python Functions.
-- Run once per catalog/schema you want them available in -- these are
-- governed catalog objects (see DATABRICKS_UDF_DESIGN.md §11), grantable
-- like any other Unity Catalog asset independent of which cluster/warehouse
-- a caller uses.
--
-- Prerequisites (see DEPLOYMENT.md for the full walkthrough):
--   1. hsm_databricks_udf installed on the target compute (wheel, per
--      DEPLOYMENT.md's per-cluster-type instructions).
--   2. HSM_SERVICE_BASE_URL, HSM_APP_ID, HSM_BEARER_TOKEN, HSM_PRIVATE_KEY_PEM
--      set as environment variables on the compute (see config.py).
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
COMMENT 'Encrypts plaintext, returning hsm-core-service''s own ciphertext_token wire format -- decryptable through the ordinary /decrypt endpoint or hsm_decrypt below.'
AS $$
    from hsm_databricks_udf.udf import encrypt
    return encrypt(plaintext, dek_name, data_classification)
$$;

CREATE OR REPLACE FUNCTION main.hsm.hsm_decrypt(
    ciphertext_token STRING COMMENT 'A token produced by /encrypt, /encrypt/batch, or hsm_encrypt above'
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Decrypts a ciphertext_token, resolving its owning app via hsm-core-service -- fails the row (not silently null) on a denied grant or a malformed token.'
AS $$
    from hsm_databricks_udf.udf import decrypt
    return decrypt(ciphertext_token)
$$;

-- Grant usage to whichever principals should be able to call these --
-- independent of which cluster/warehouse they use, since these are Unity
-- Catalog-governed objects (see DATABRICKS_UDF_DESIGN.md §11).
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_encrypt TO `data-engineers`;
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_decrypt TO `data-engineers`;
