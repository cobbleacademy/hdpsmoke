-- Registers four Unity Catalog functions -- see DATABRICKS_UDF_DESIGN.md §11:
--   hsm_credentials()                                  -- builds credentials_json (LANGUAGE SQL) -- standalone use only, see CONFIRMED BUG below
--   hsm_encrypt_detail(..., credentials_json)           -- the real crypto (LANGUAGE PYTHON)
--   hsm_decrypt_detail(ciphertext_token, credentials_json)  -- the real crypto (LANGUAGE PYTHON)
--   hsm_encrypt(...)  / hsm_decrypt(ciphertext_token)   -- simplified wrappers (LANGUAGE SQL)
--     that call the _detail versions with the SAME credentials expression
--     hsm_credentials() uses, INLINED directly (not by calling
--     hsm_credentials() -- see CONFIRMED BUG below), so the common case
--     still never has to mention credentials at all:
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
-- CONFIRMED BUG -- a LANGUAGE SQL function's body calling ANOTHER
-- user-defined SQL function reliably breaks Unity Catalog's dependency
-- tracking for that edge specifically, raising UC_INVALID_DEPENDENCIES.SQL_UDF
-- on every subsequent call. This is NOT fixed by: restarting a cluster or
-- SQL warehouse (UC function metadata lives in the metastore, not on any
-- compute); dropping and recreating every function in the chain, in order;
-- or reducing UDF-call count below Databricks' 5-per-query limit (a single
-- hsm_decrypt() call, at 3 nested invocations, was never near it).
-- Isolated by direct experiment: hsm_decrypt (SQL) calling hsm_decrypt_detail
-- (PYTHON) works reliably; hsm_decrypt (SQL) calling hsm_credentials()
-- (SQL, zero-arg) is what breaks. The original flat 2-argument design
-- (confirmed working end-to-end on a real workspace) never hit this,
-- because it called hsm_credentials() from an ad-hoc caller query, never
-- from inside another stored function's own body -- that's a fundamentally
-- different mechanism to Unity Catalog than one function's definition
-- referencing another's.
--
-- FIX: hsm_encrypt/hsm_decrypt below do NOT call hsm_credentials() --
-- they inline the identical to_json(named_struct(...)) expression directly
-- in their own RETURN clause instead. hsm_credentials() itself is kept,
-- standalone, for a caller who wants to build credentials_json explicitly
-- for use with hsm_encrypt_detail/hsm_decrypt_detail from their own ad-hoc
-- query (not from inside another function body) -- that path is unaffected.
--
-- REAL COST of this fix: the credentials-building literal now exists in
-- THREE places (hsm_credentials(), hsm_encrypt(), hsm_decrypt()) instead of
-- one. There is no single source of truth anymore for this deployment's
-- base URL/app_id/auth mode/secret names -- keep all three in sync by hand
-- on every edit. This is a deliberate, confirmed-necessary tradeoff, not an
-- oversight: DRY-ness lost in exchange for the wrappers actually working.
--
-- MAINTENANCE GOTCHA, still applicable -- confirmed live, not fixed by a
-- cluster/warehouse restart (Unity Catalog function metadata lives in the
-- metastore, not on any compute session):
--   - CREATE OR REPLACE FUNCTION cannot change a function's PARAMETER LIST,
--     only the body/return type if the signature stays identical. If you
--     ever change hsm_encrypt/hsm_decrypt's argument list again, DROP them
--     explicitly first (see below) rather than relying on OR REPLACE.
--   - Safest habit regardless: always re-run this ENTIRE file top-to-bottom
--     after any edit, in order, rather than re-running only the one
--     statement you changed.
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
-- combined with to_json(named_struct(...)).
--
-- Edit the credentials literal in ALL THREE places below (hsm_credentials(),
-- hsm_encrypt(), hsm_decrypt()) to match your deployment (base URL, app_id,
-- auth mode, secret scope/key names) -- see the "REAL COST" note above for
-- why this can't be centralized in one place.
--
-- GOVERNANCE: a plain CREATE FUNCTION ... LANGUAGE SQL body runs with the
-- FUNCTION OWNER's privileges by default (definer rights, same as a view)
-- -- confirmed directly against Databricks' own docs, not assumed. So
-- secret('hsm', ...) inside hsm_credentials()/hsm_encrypt()/hsm_decrypt()
-- checks the owner's READ SECRET grant, never the caller's: whoever
-- registers these functions needs READ SECRET on the `hsm` scope, and
-- every other caller needs only EXECUTE on whichever functions they call.
--
-- Prerequisites (see DEPLOYMENT.md for the full walkthrough):
--   1. Build the wheel and upload it to a Unity Catalog volume.
--   2. Create a Databricks secret scope holding whichever credential your
--      chosen auth mode needs. Only the person who registers these
--      functions (their owner) needs READ SECRET on it.
--   3. This app_id already registered in hsm-core-service with a
--      dek_issue,dek_unwrap-capable token/signing key and its
--      encryption_public_key_pem provisioned via POST /admin/apps/keys
--      (see java/docs/ADMIN_OPERATIONS.md).

CREATE OR REPLACE FUNCTION main.hsm.hsm_credentials()
RETURNS STRING
LANGUAGE SQL
COMMENT 'Builds a credentials_json value for standalone/ad-hoc use with hsm_encrypt_detail/hsm_decrypt_detail -- e.g. SELECT hsm_decrypt_detail(token, hsm_credentials()). Do NOT call this from inside another function''s body (see CONFIRMED BUG at the top of this file) -- hsm_encrypt()/hsm_decrypt() below inline the identical expression instead. STATIC auth shown; see the commented-out SELF_SIGNED_JWT variant below.'
RETURN to_json(named_struct(
    'HSM_SERVICE_BASE_URL', 'https://hsm-core-service.internal:8443/api/sensec/hsm/v1',
    'HSM_APP_ID',           'databricks-udf',
    'HSM_AUTH_MODE',        'STATIC',
    'HSM_PRIVATE_KEY_PEM',  secret('hsm', 'databricks-udf-private-key'),
    'HSM_BEARER_TOKEN',     secret('hsm', 'databricks-udf-token')
));

-- SELF_SIGNED_JWT variant -- swap the RETURN clause above for this instead
-- (do not define both for the same app_id; HSM_BEARER_TOKEN is STATIC-only
-- and must NOT appear here -- SELF_SIGNED_JWT never reads it). If you use
-- this variant, apply the SAME swap to hsm_encrypt()/hsm_decrypt()'s
-- inlined expressions below too:
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
    credentials_json STRING COMMENT 'JSON object with an app''s HSM_* config -- pass a non-default one explicitly here; the common case should use hsm_encrypt() instead'
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
    credentials_json STRING COMMENT 'JSON object with an app''s HSM_* config -- pass a non-default one explicitly here; the common case should use hsm_decrypt() instead'
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
-- OWNER's privileges: the caller never needs to know how credentials_json
-- gets built, let alone build it themselves. Their credentials expression
-- is INLINED, not a call to hsm_credentials() -- see CONFIRMED BUG at the
-- top of this file for why.
--
-- Explicit DROP before CREATE, not just CREATE OR REPLACE: these two
-- changed parameter lists during this design's evolution (from 4/2-arg
-- credentials_json-as-argument Python functions to today's 3/1-arg SQL
-- wrappers), and CREATE OR REPLACE cannot change a function's signature.
-- If you rename or change these functions' argument lists again in the
-- future, DROP them first the same way.

DROP FUNCTION IF EXISTS main.hsm.hsm_encrypt;
DROP FUNCTION IF EXISTS main.hsm.hsm_decrypt;

CREATE OR REPLACE FUNCTION main.hsm.hsm_encrypt(
    plaintext STRING COMMENT 'The value to encrypt',
    dek_name STRING COMMENT 'Logical name for DEK reuse across many calls -- e.g. one per column, "customers.ssn"',
    data_classification STRING COMMENT 'Optional tag, e.g. "pii" -- persisted for compliance queries, never enforced here; pass NULL if not used'
)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Encrypts plaintext using this deployment''s default credentials (inlined below -- keep in sync with hsm_credentials() and hsm_decrypt(), see the top-of-file note on why this isn''t centralized). Use hsm_encrypt_detail(...) instead if you need to pass a different credentials_json.'
RETURN main.hsm.hsm_encrypt_detail(plaintext, dek_name, data_classification, to_json(named_struct(
    'HSM_SERVICE_BASE_URL', 'https://hsm-core-service.internal:8443/api/sensec/hsm/v1',
    'HSM_APP_ID',           'databricks-udf',
    'HSM_AUTH_MODE',        'STATIC',
    'HSM_PRIVATE_KEY_PEM',  secret('hsm', 'databricks-udf-private-key'),
    'HSM_BEARER_TOKEN',     secret('hsm', 'databricks-udf-token')
)));

CREATE OR REPLACE FUNCTION main.hsm.hsm_decrypt(
    ciphertext_token STRING COMMENT 'A token produced by /encrypt, /encrypt/batch, or hsm_encrypt above'
)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Decrypts ciphertext_token using this deployment''s default credentials (inlined below -- keep in sync with hsm_credentials() and hsm_encrypt(), see the top-of-file note on why this isn''t centralized). Use hsm_decrypt_detail(...) instead if you need to pass a different credentials_json.'
RETURN main.hsm.hsm_decrypt_detail(ciphertext_token, to_json(named_struct(
    'HSM_SERVICE_BASE_URL', 'https://hsm-core-service.internal:8443/api/sensec/hsm/v1',
    'HSM_APP_ID',           'databricks-udf',
    'HSM_AUTH_MODE',        'STATIC',
    'HSM_PRIVATE_KEY_PEM',  secret('hsm', 'databricks-udf-private-key'),
    'HSM_BEARER_TOKEN',     secret('hsm', 'databricks-udf-token')
)));

-- Example usage, pure SQL, no notebook or dbutils required -- the common case:
--   SELECT main.hsm.hsm_encrypt('4111-1111-1111-1234', 'customers.account_number', 'pci');
--   SELECT id, main.hsm.hsm_decrypt(ciphertext_token) AS account_number
--   FROM main.payments.customer_accounts;
--
-- Explicit-credentials case (a different app_id/credential than the deployment default) --
-- calling hsm_credentials() here is safe: this is an ad-hoc caller query,
-- not one function's body calling another's:
--   SELECT main.hsm.hsm_decrypt_detail(ciphertext_token, main.hsm.hsm_credentials()) ...

-- Grant usage to whichever principals should be able to call these --
-- independent of which cluster/warehouse they use, since these are Unity
-- Catalog-governed objects (see DATABRICKS_UDF_DESIGN.md §11). Most callers
-- only need EXECUTE on hsm_encrypt/hsm_decrypt (the simplified wrappers) --
-- they run with their OWNER's privileges (definer rights), so the caller
-- never needs EXECUTE on hsm_encrypt_detail/hsm_decrypt_detail or READ
-- SECRET on the `hsm` scope at all. Grant the _detail functions (and
-- hsm_credentials(), for ad-hoc use with them) only to whoever specifically
-- needs to pass a non-default credentials_json.
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_encrypt TO `data-engineers`;
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_decrypt TO `data-engineers`;
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_encrypt_detail TO `hsm-power-users`;
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_decrypt_detail TO `hsm-power-users`;
-- GRANT EXECUTE ON FUNCTION main.hsm.hsm_credentials TO `hsm-power-users`;
