-- Registers four Unity Catalog functions -- see DATABRICKS_UDF_DESIGN.md §11:
--   hsm_credentials()                                  -- builds credentials_json (LANGUAGE SQL)
--   hsm_encrypt_detail(..., credentials_json)           -- the real crypto (LANGUAGE PYTHON)
--   hsm_decrypt_detail(ciphertext_token, credentials_json)  -- the real crypto (LANGUAGE PYTHON)
--   hsm_encrypt(...)  / hsm_decrypt(ciphertext_token)   -- simplified wrappers (LANGUAGE SQL)
--     that call the _detail versions with hsm_credentials() baked in, so the
--     common case never has to mention credentials at all:
--       SELECT main.hsm.hsm_decrypt(ciphertext_token) FROM t;
--     The _detail versions remain for callers who need to pass a specific,
--     non-default credentials_json (e.g. calling on behalf of a different
--     app_id). Unity Catalog does NOT support function overloading -- a
--     single name can't have two coexisting signatures (CREATE OR REPLACE
--     requires the same parameter list as before) -- confirmed directly
--     against Databricks' own docs, not assumed. Hence the separate name
--     rather than two same-named hsm_decrypt variants.
--
-- Run once per catalog/schema you want them available in -- these are
-- governed catalog objects, grantable like any other Unity Catalog asset
-- independent of which cluster/warehouse a caller uses.
--
-- The ENVIRONMENT clause on the _detail functions declares this package's
-- wheel + its own dependencies (cryptography, requests) directly on the
-- function -- confirmed via Databricks' own CREATE FUNCTION docs, not
-- assumed. Resolved wherever the function actually executes, not wherever
-- it was registered from.
--
-- CREDENTIALS: passed as an explicit `credentials_json` argument to the
-- _detail functions, NOT resolved inside the function body via
-- dbutils.secrets.get(). An earlier version of this script tried the
-- latter and it does not work: confirmed directly against Databricks' own
-- docs/KB -- dbutils is only available in the calling notebook/job's own
-- driver context, never inside a UDF or Unity Catalog Python Function body.
-- Calling it from inside AS $$ ... $$ raises
-- NameError: name 'dbutils' is not defined.
--
-- Instead, credentials_json is built with plain Databricks SQL -- no
-- dbutils, no notebook required -- via the built-in secret(scope, key)
-- function (a general SQL scalar expression, Databricks Runtime 11.3 LTS+,
-- usable anywhere a string expression is, not just inside CREATE CONNECTION)
-- combined with to_json(named_struct(...)). hsm_credentials() below wraps
-- that exactly once so every caller (SQL editor, dashboard, SQL warehouse
-- query, or a notebook via spark.sql -- see DEPLOYMENT.md for a Python
-- test using databricks-sql-connector) shares one definition instead of
-- copy-pasting secret scope/key names into every query.
--
-- Edit the literals inside hsm_credentials() below to match your deployment
-- (base URL, app_id, auth mode, secret scope/key names) -- everything else
-- in this file should not need to change.
--
-- GOVERNANCE: a plain CREATE FUNCTION ... LANGUAGE SQL body (hsm_credentials()
-- and the simplified hsm_encrypt/hsm_decrypt wrappers below) runs with the
-- FUNCTION OWNER's privileges by default (definer rights, same as a view)
-- -- confirmed directly against Databricks' own docs, not assumed. So
-- secret('hsm', ...) inside hsm_credentials() checks the owner's READ
-- SECRET grant, never the caller's: whoever registers these functions
-- needs READ SECRET on the `hsm` scope, and every other caller needs only
-- EXECUTE on whichever functions they call -- exactly the isolation the
-- original, since-abandoned dbutils-in-body design was trying to get,
-- restored here via a different mechanism. This holds through the nested
-- call chain (hsm_decrypt -> hsm_decrypt_detail + hsm_credentials()) as
-- long as one principal owns all four functions, same as any definer-rights
-- view referencing another view.
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
COMMENT 'Builds the credentials_json argument hsm_encrypt_detail/hsm_decrypt_detail expect -- the one place secret scope/key names and auth mode are set for this deployment. STATIC auth shown; see the commented-out SELF_SIGNED_JWT variant below.'
RETURN to_json(named_struct(
    'HSM_SERVICE_BASE_URL', 'https://hsm-core-service.internal:8443/api/sensec/hsm/v1',
    'HSM_APP_ID',           'databricks-udf',
    'HSM_AUTH_MODE',        'STATIC',
    'HSM_PRIVATE_KEY_PEM',  secret('hsm', 'databricks-udf-private-key'),
    'HSM_BEARER_TOKEN',     secret('hsm', 'databricks-udf-token')
));

-- SELF_SIGNED_JWT variant -- swap the RETURN clause above for this instead
-- (do not define both for the same app_id; HSM_BEARER_TOKEN is STATIC-only
-- and must NOT appear here -- SELF_SIGNED_JWT never reads it):
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

CREATE OR REPLACE FUNCTION main.hsm.hsm_encrypt_detail(
    plaintext STRING COMMENT 'The value to encrypt',
    dek_name STRING COMMENT 'Logical name for DEK reuse across many calls -- e.g. one per column, "customers.ssn"',
    data_classification STRING COMMENT 'Optional tag, e.g. "pii" -- persisted for compliance queries, never enforced here; pass NULL if not used',
    credentials_json STRING COMMENT 'JSON object with an app''s HSM_* config -- pass a non-default one explicitly here; the common case should use hsm_encrypt() instead, which builds this from hsm_credentials() automatically'
)
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (
  dependencies = '["cryptography>=42.0", "requests>=2.31",
                   "/Volumes/main/hsm/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl"]',
  environment_version = 'None'
)
COMMENT 'Encrypts plaintext with an explicit credentials_json -- prefer hsm_encrypt() unless you need a non-default credential. Returns hsm-core-service''s own ciphertext_token wire format.'
AS $$
    from hsm_databricks_udf.udf import encrypt
    return encrypt(plaintext, dek_name, data_classification, credentials_json)
$$;

CREATE OR REPLACE FUNCTION main.hsm.hsm_decrypt_detail(
    ciphertext_token STRING COMMENT 'A token produced by /encrypt, /encrypt/batch, or hsm_encrypt/hsm_encrypt_detail above',
    credentials_json STRING COMMENT 'JSON object with an app''s HSM_* config -- pass a non-default one explicitly here; the common case should use hsm_decrypt() instead, which builds this from hsm_credentials() automatically'
)
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (
  dependencies = '["cryptography>=42.0", "requests>=2.31",
                   "/Volumes/main/hsm/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl"]',
  environment_version = 'None'
)
COMMENT 'Decrypts a ciphertext_token with an explicit credentials_json -- prefer hsm_decrypt() unless you need a non-default credential. Resolves the owning app via hsm-core-service; fails the row (not silently null) on a denied grant or a malformed token.'
AS $$
    from hsm_databricks_udf.udf import decrypt
    return decrypt(ciphertext_token, credentials_json)
$$;

-- Simplified wrappers -- the ones almost every caller should actually use.
-- LANGUAGE SQL, so (per the governance note above) they run with their
-- OWNER's privileges: the caller never needs to know hsm_credentials()
-- exists, let alone call it themselves.

CREATE OR REPLACE FUNCTION main.hsm.hsm_encrypt(
    plaintext STRING COMMENT 'The value to encrypt',
    dek_name STRING COMMENT 'Logical name for DEK reuse across many calls -- e.g. one per column, "customers.ssn"',
    data_classification STRING COMMENT 'Optional tag, e.g. "pii" -- persisted for compliance queries, never enforced here; pass NULL if not used'
)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Encrypts plaintext using this deployment''s default credentials (see hsm_credentials()). Use hsm_encrypt_detail(...) instead if you need to pass a different credentials_json.'
RETURN main.hsm.hsm_encrypt_detail(plaintext, dek_name, data_classification, main.hsm.hsm_credentials());

CREATE OR REPLACE FUNCTION main.hsm.hsm_decrypt(
    ciphertext_token STRING COMMENT 'A token produced by /encrypt, /encrypt/batch, or hsm_encrypt above'
)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Decrypts ciphertext_token using this deployment''s default credentials (see hsm_credentials()). Use hsm_decrypt_detail(...) instead if you need to pass a different credentials_json.'
RETURN main.hsm.hsm_decrypt_detail(ciphertext_token, main.hsm.hsm_credentials());

-- Example usage, pure SQL, no notebook or dbutils required -- the common case:
--   SELECT main.hsm.hsm_encrypt('4111-1111-1111-1234', 'customers.account_number', 'pci');
--   SELECT id, main.hsm.hsm_decrypt(ciphertext_token) AS account_number
--   FROM main.payments.customer_accounts;
--
-- Explicit-credentials case (a different app_id/credential than the deployment default):
--   SELECT main.hsm.hsm_decrypt_detail(ciphertext_token, <some other credentials_json>) ...

-- Grant usage to whichever principals should be able to call these --
-- independent of which cluster/warehouse they use, since these are Unity
-- Catalog-governed objects (see DATABRICKS_UDF_DESIGN.md §11). Most callers
-- only need EXECUTE on hsm_encrypt/hsm_decrypt (the simplified wrappers) --
-- they run with their OWNER's privileges (definer rights), so the caller
-- never needs EXECUTE on hsm_credentials()/hsm_encrypt_detail/
-- hsm_decrypt_detail or READ SECRET on the `hsm` scope at all. Grant the
-- _detail functions too only to whoever specifically needs to pass a
-- non-default credentials_json.
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_encrypt TO `data-engineers`;
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_decrypt TO `data-engineers`;
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_encrypt_detail TO `hsm-power-users`;
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_decrypt_detail TO `hsm-power-users`;
--
-- NOTE: the "caller only needs EXECUTE on the top-level wrapper" claim above
-- follows the same definer-rights delegation a view uses when it references
-- another view/table the caller can't see directly -- standard, well-
-- documented Databricks/SQL behavior, not something specific to this file.
-- That said, the flat 2-argument hsm_encrypt/hsm_decrypt (credentials_json
-- as an explicit argument) is what was actually confirmed working end-to-end
-- against a real Databricks workspace (both STATIC and SELF_SIGNED_JWT).
-- This 4-function nested-wrapper version has not been smoke-tested live yet
-- -- verify a grants-only caller (EXECUTE on hsm_encrypt/hsm_decrypt alone,
-- nothing else) can actually call them before relying on this for real
-- access control.
