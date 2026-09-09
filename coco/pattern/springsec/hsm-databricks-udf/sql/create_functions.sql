-- Registers hsm_encrypt/hsm_decrypt as Unity Catalog Python Functions, plus
-- a hsm_credentials() helper (Unity Catalog SQL function -- see
-- DATABRICKS_UDF_DESIGN.md §11) that is the single, standardized way
-- credentials_json gets built. Run once per catalog/schema you want them
-- available in -- these are governed catalog objects, grantable like any
-- other Unity Catalog asset independent of which cluster/warehouse a caller
-- uses.
--
-- The ENVIRONMENT clause on hsm_encrypt/hsm_decrypt declares this package's
-- wheel + its own dependencies (cryptography, requests) directly on the
-- function -- confirmed via Databricks' own CREATE FUNCTION docs, not
-- assumed. Resolved wherever the function actually executes, not wherever
-- it was registered from.
--
-- CREDENTIALS: passed in as an explicit `credentials_json` argument on
-- every call, NOT resolved inside the function body via dbutils.secrets.get().
-- An earlier version of this script tried the latter and it does not work:
-- confirmed directly against Databricks' own docs/KB -- dbutils is only
-- available in the calling notebook/job's own driver context, never inside
-- a UDF or Unity Catalog Python Function body. Calling it from inside
-- AS $$ ... $$ raises NameError: name 'dbutils' is not defined.
--
-- Instead, credentials_json is built with plain Databricks SQL -- no
-- dbutils, no notebook required -- via the built-in secret(scope, key)
-- function (a general SQL scalar expression, Databricks Runtime 11.3 LTS+,
-- usable anywhere a string expression is, not just inside CREATE CONNECTION)
-- combined with to_json(named_struct(...)). hsm_credentials() below wraps
-- that exactly once so every caller (SQL editor, dashboard, SQL warehouse
-- query, or a notebook via spark.sql -- see DEPLOYMENT.md for a Python
-- test using databricks-sql-connector) shares one definition instead of
-- copy-pasting secret scope/key names into every query:
--
--   SELECT main.hsm.hsm_decrypt(ciphertext_token, main.hsm.hsm_credentials())
--   FROM main.payments.customer_accounts;
--
-- Edit the literals inside hsm_credentials() below to match your deployment
-- (base URL, app_id, auth mode, secret scope/key names) -- everything else
-- in this file should not need to change.
--
-- GOVERNANCE: a plain CREATE FUNCTION ... LANGUAGE SQL body (hsm_credentials()
-- below) runs with the FUNCTION OWNER's privileges by default (definer
-- rights, same as a view) -- confirmed directly against Databricks' own
-- docs, not assumed. So secret('hsm', ...) inside it checks the owner's
-- READ SECRET grant, never the caller's: whoever registers hsm_credentials()
-- needs READ SECRET on the `hsm` scope, and every other caller needs only
-- EXECUTE on the functions -- exactly the isolation the original,
-- since-abandoned dbutils-in-body design was trying to get, restored here
-- via a different mechanism.
--
-- Prerequisites (see DEPLOYMENT.md for the full walkthrough):
--   1. Build the wheel and upload it to a Unity Catalog volume.
--   2. Create a Databricks secret scope holding whichever credential your
--      chosen auth mode needs. Only the person who registers
--      hsm_credentials() (its owner) needs READ SECRET on it.
--   3. This app_id already registered in hsm-core-service with a
--      dek_issue,dek_unwrap-capable token/signing key and its
--      encryption_public_key_pem provisioned via POST /admin/apps/keys
--      (see java/docs/ADMIN_OPERATIONS.md).

CREATE OR REPLACE FUNCTION main.hsm.hsm_credentials()
RETURNS STRING
LANGUAGE SQL
COMMENT 'Builds the credentials_json argument hsm_encrypt/hsm_decrypt expect -- the one place secret scope/key names and auth mode are set for this deployment. STATIC auth shown; see the commented-out SELF_SIGNED_JWT variant below.'
RETURN to_json(named_struct(
    'HSM_SERVICE_BASE_URL', 'https://hsm-core-service.internal:8443/api/sensec/hsm/v1',
    'HSM_APP_ID',           'databricks-udf',
    'HSM_AUTH_MODE',        'STATIC',
    'HSM_PRIVATE_KEY_PEM',  secret('hsm', 'databricks-udf-private-key'),
    'HSM_BEARER_TOKEN',     secret('hsm', 'databricks-udf-token')
));

-- SELF_SIGNED_JWT variant -- swap the RETURN clause above for this instead
-- (do not define both for the same app_id):
--
-- RETURN to_json(named_struct(
--     'HSM_SERVICE_BASE_URL',        'https://hsm-core-service.internal:8443/api/sensec/hsm/v1',
--     'HSM_APP_ID',                  'databricks-udf',
--     'HSM_AUTH_MODE',               'SELF_SIGNED_JWT',
--     'HSM_PRIVATE_KEY_PEM',         secret('hsm', 'databricks-udf-private-key'),
--     'HSM_SIGNING_PRIVATE_KEY_PEM', secret('hsm', 'databricks-udf-signing-key')
--     -- HSM_SELF_SIGNED_AUDIENCE defaults to "hsm-core-service" -- add it
--     -- here only if the server's own hsm.jwt.audience config differs.
-- ));

CREATE OR REPLACE FUNCTION main.hsm.hsm_encrypt(
    plaintext STRING COMMENT 'The value to encrypt',
    dek_name STRING COMMENT 'Logical name for DEK reuse across many calls -- e.g. one per column, "customers.ssn"',
    data_classification STRING COMMENT 'Optional tag, e.g. "pii" -- persisted for compliance queries, never enforced here; pass NULL if not used',
    credentials_json STRING COMMENT 'JSON object with this app''s HSM_* config -- build with main.hsm.hsm_credentials(), never hardcoded here; see DEPLOYMENT.md'
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
    credentials_json STRING COMMENT 'JSON object with this app''s HSM_* config -- build with main.hsm.hsm_credentials(), never hardcoded here; see DEPLOYMENT.md'
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

-- Example usage, pure SQL, no notebook or dbutils required:
--   SELECT main.hsm.hsm_encrypt('4111-1111-1111-1234', 'customers.account_number', 'pci', main.hsm.hsm_credentials());
--   SELECT id, main.hsm.hsm_decrypt(ciphertext_token, main.hsm.hsm_credentials()) AS account_number
--   FROM main.payments.customer_accounts;

-- Grant usage to whichever principals should be able to call these --
-- independent of which cluster/warehouse they use, since these are Unity
-- Catalog-governed objects (see DATABRICKS_UDF_DESIGN.md §11). Callers need
-- only EXECUTE on all three functions -- hsm_credentials() runs with its
-- OWNER's privileges (definer rights, the default for a plain
-- LANGUAGE SQL function), so secret(...) inside it checks the owner's
-- READ SECRET grant, never the caller's. Callers never need direct access
-- to the `hsm` secret scope at all.
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_credentials TO `data-engineers`;
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_encrypt TO `data-engineers`;
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_decrypt TO `data-engineers`;
