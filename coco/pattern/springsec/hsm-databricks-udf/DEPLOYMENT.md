# Deploying `hsm-databricks-udf` to Databricks

Companion to [`../java/docs/DATABRICKS_UDF_DESIGN.md`](../java/docs/DATABRICKS_UDF_DESIGN.md)
(the design/rationale) and [`sql/create_functions.sql`](sql/create_functions.sql)
(the actual `CREATE FUNCTION` DDL).

**Status:** the package itself is built and verified — its crypto is proven
byte-for-byte compatible with `hsm-core-service`'s real Java implementation in
both directions, including the credential-passing call path described below
(see `tests/test_live_interop.py`, run for real against a live local
instance). **The Databricks-side deployment steps themselves have not been
run against a real Databricks workspace** — this repo has no Databricks
access.

## How credentials reach the function

An earlier version of this doc had the function body call
`dbutils.secrets.get(...)` directly inside `AS $$ ... $$`. **That does not
work** — confirmed directly against Databricks' own docs/KB: `dbutils` is
only available in the calling notebook/job's own driver context, never
inside a UDF or Unity Catalog Python Function body. It raises
`NameError: name 'dbutils' is not defined`.

The fix: `hsm_encrypt`/`hsm_decrypt` take an explicit `credentials_json`
argument — a JSON object with the same field names as this package's `HSM_*`
config (see `config.py`'s `Config.from_json`) — instead of resolving
credentials internally. `credentials_json` is built in **pure SQL**, with no
`dbutils`/notebook/Python required, via `sql/create_functions.sql`'s
`hsm_credentials()` helper function, which wraps Databricks' built-in
`secret(scope, key)` scalar function (Databricks Runtime 11.3 LTS+ — a
general SQL expression, not something confined to `CREATE CONNECTION`)
combined with `to_json(named_struct(...))`:

```sql
SELECT main.hsm.hsm_decrypt(ciphertext_token, main.hsm.hsm_credentials())
FROM main.payments.customer_accounts;
```

`hsm_credentials()` is the **single, standardized** place `credentials_json`
gets built — every caller (SQL editor, dashboard, SQL warehouse query, job,
or a notebook via `spark.sql(...)`) shares this one definition rather than
each re-typing secret scope/key names. Edit its literals in
`sql/create_functions.sql` to match your deployment (base URL, app_id, auth
mode, secret scope/key names); nothing else in that file should need to
change per-deployment.

**Governance — better than the original `dbutils`-in-body design, not worse**:
a plain `CREATE FUNCTION ... LANGUAGE SQL` body (`hsm_credentials()`) runs
with the **function owner's** privileges by default (definer rights, same as
a view) — confirmed directly against Databricks' own docs. So `secret(...)`
inside it checks the *owner's* `READ SECRET` grant, never the caller's:
only whoever registers `hsm_credentials()` needs `READ SECRET` on the `hsm`
scope; every other caller needs only `EXECUTE` on the three functions
(`hsm_credentials`, `hsm_encrypt`, `hsm_decrypt`) and never touches the raw
secret at all.

## Why one flow now covers all three compute types for the wheel itself

`CREATE FUNCTION`'s `ENVIRONMENT` clause declares a function's Python
dependencies (PyPI packages, or a wheel path in a Unity Catalog volume) *as
part of the function's own definition* — confirmed directly against
Databricks' `CREATE FUNCTION` docs. The wheel + its dependencies are
resolved wherever the function executes, not wherever it was registered
from, regardless of compute type.

## 0. One-time prerequisites

1. **Register the calling app in `hsm-core-service`** (if not already), with
   at minimum the `dek_issue`/`dek_unwrap` scopes — see
   [`java/docs/APP_ONBOARDING.md`](../java/docs/APP_ONBOARDING.md).
2. **Generate an RSA keypair** for this app (the DEK-transport keypair —
   separate from any JWT-signing key). **Do not passphrase-protect it** —
   `transport.py`'s `parse_private_key_pem` always parses with no password;
   a passphrase-protected key fails with `ValueError: Could not deserialize
   key data...` (see §5's troubleshooting table):
   ```bash
   openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out hsm-databricks-key.pem
   openssl pkey -in hsm-databricks-key.pem -pubout -out hsm-databricks-key.pub.pem
   ```
   `HSM_PRIVATE_KEY_PEM`/`HSM_SIGNING_PRIVATE_KEY_PEM` accept either the raw
   multi-line PEM *or* that same PEM base64-encoded as a single line —
   deliberately supported, not a workaround (`transport.py`'s
   `_normalize_key_material` detects which one it got). **Base64 is the
   recommended way to store it** in a secret scope's plain string field,
   since it sidesteps every newline-mangling risk a raw multi-line value is
   exposed to across different upload paths (CLI quoting, UI text boxes,
   etc.):
   ```bash
   databricks secrets put-secret hsm databricks-udf-private-key --string-value "$(base64 -w0 hsm-databricks-key.pem)"
   ```
   (`base64 -w0` disables line-wrapping — without it, most `base64`
   implementations insert newlines every 76 characters, which would defeat
   the point.) Storing the raw PEM via `--file` still works too.
3. **Register the public key** via `POST /admin/apps/keys` (see
   [`java/docs/ADMIN_OPERATIONS.md`](../java/docs/ADMIN_OPERATIONS.md)):
   ```bash
   curl -X POST "$BASE/admin/apps/keys" \
     -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin" \
     -H "Content-Type: application/json" \
     -d "{\"app_id\": \"databricks-udf\", \"encryption_public_key_pem\": \"$(cat hsm-databricks-key.pub.pem)\"}"
   ```
4. **Choose an auth mode** — `STATIC` (a fixed bearer token, simplest) or
   `SELF_SIGNED_JWT` (this app signs its own short-lived token locally on
   every call, no externally-managed token to renew). `AZURE_AD`/`MTLS`
   aren't implemented in this package yet — see `DATABRICKS_UDF_DESIGN.md`
   §14.
   - **STATIC**: get a bearer token for this app the usual way (a demo token,
     or whatever `hsm-core-service`'s deployment issues for real tokens).
   - **SELF_SIGNED_JWT**: generate a *second*, dedicated signing keypair
     (independent of the DEK-transport keypair from step 2 — though it may
     reuse the same PEM, the legacy one-keypair fallback both
     `HsmCryptoClient.Builder` and this package support), also unencrypted,
     and register its public half via `POST /admin/apps/keys`'
     `signing_public_key_pem` field:
     ```bash
     openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out hsm-databricks-signing-key.pem
     openssl pkey -in hsm-databricks-signing-key.pem -pubout -out hsm-databricks-signing-key.pub.pem
     curl -X POST "$BASE/admin/apps/keys" \
       -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin" \
       -H "Content-Type: application/json" \
       -d "{\"app_id\": \"databricks-udf\", \"signing_public_key_pem\": \"$(cat hsm-databricks-signing-key.pub.pem)\"}"
     ```
     The `aud` claim this package signs defaults to `"hsm-core-service"` —
     override via `HSM_SELF_SIGNED_AUDIENCE` if the server's own
     `hsm.jwt.audience` config differs. Verified end-to-end against a real
     `hsm-core-service` instance (`tests/test_live_interop.py::test_self_signed_jwt_accepted_by_real_server_end_to_end`),
     not just structurally.
5. **Build the wheel**:
   ```bash
   cd hsm-databricks-udf
   python -m build   # produces dist/hsm_databricks_udf-0.1.0-py3-none-any.whl
   ```
6. **Upload the wheel to a Unity Catalog volume** — this is what the
   `ENVIRONMENT` clause's wheel path points at, so it must be a volume, not
   DBFS:
   ```bash
   databricks fs cp dist/hsm_databricks_udf-0.1.0-py3-none-any.whl \
     dbfs:/Volumes/main/hsm/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl
   ```
7. **Create a Databricks secret scope** holding whichever credential your
   chosen auth mode needs, plus the DEK-transport private key either way.
   Store the keys base64-encoded, per §0.2's recommendation:
   ```bash
   databricks secrets create-scope hsm
   databricks secrets put-secret hsm databricks-udf-private-key --string-value "$(base64 -w0 hsm-databricks-key.pem)"

   # STATIC:
   databricks secrets put-secret hsm databricks-udf-token --string-value "$TOKEN"

   # SELF_SIGNED_JWT (instead of the token above):
   databricks secrets put-secret hsm databricks-udf-signing-key --string-value "$(base64 -w0 hsm-databricks-signing-key.pem)"
   ```
   Only the identity that will *register* `hsm_credentials()` (its owner)
   needs `READ SECRET` on this scope — see the governance note above.

## 1. Register the functions

Edit the literals inside `hsm_credentials()` and the volume path in
[`sql/create_functions.sql`](sql/create_functions.sql) to match your
deployment, then run the whole file — from a notebook, the SQL editor, or a
job, on **any** compute type (job/classic, shared, or serverless all work
identically here, since nothing in this step is compute-specific).

## 2. Run it

Pure SQL, no notebook or `dbutils` required:

```sql
SELECT main.hsm.hsm_encrypt('4111-1111-1111-1234', 'customers.account_number', 'pci', main.hsm.hsm_credentials()) AS token;
-- -> "v1.AbC123..."

SELECT main.hsm.hsm_decrypt('v1.AbC123...', main.hsm.hsm_credentials()) AS plaintext;
-- -> "4111-1111-1111-1234"

-- Over a real table:
SELECT id, main.hsm.hsm_decrypt(ciphertext_token, main.hsm.hsm_credentials()) AS account_number
FROM main.payments.customer_accounts;
```

## 3. Testing from Python (standardized on the same `hsm_credentials()`)

Use the official `databricks-sql-connector` (DB-API driver for a Databricks
SQL warehouse) to run the *same* SQL — this tests the actual deployed UC
function through the real SQL layer, not a local stand-in, and never
duplicates the credential-building logic in Python:

```python
from databricks import sql

with sql.connect(server_hostname="<workspace-hostname>",
                  http_path="<warehouse-http-path>",
                  access_token="<personal-access-token-or-oauth>") as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT main.hsm.hsm_decrypt(
                main.hsm.hsm_encrypt(%(pt)s, %(dn)s, NULL, main.hsm.hsm_credentials()),
                main.hsm.hsm_credentials()
            ) = %(pt)s AS round_trip_ok
        """, {"pt": "test value", "dn": "deployment.verify.column"})
        row = cur.fetchone()
        assert row.round_trip_ok
```

This is the recommended smoke test for a fresh deployment (§4 has the
cross-check against `/decrypt` directly). `pip install databricks-sql-connector`
locally or run this from a notebook (where `spark.sql(...)` works equally
well as a substitute for the connector).

## 4. Serverless-specific note: network egress

Python UDFs can reach external HTTPS endpoints on serverless — confirmed
directly against Databricks docs — but if the workspace runs *restricted*
serverless egress, `hsm-core-service`'s domain must be explicitly added:
Settings → Network → Network Policies → allowed internet destinations. This
is a one-time workspace-level change, unrelated to the function registration
above.

## 5. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `NameError: name 'dbutils' is not defined` (raised from inside `hsm_encrypt`/`hsm_decrypt`) | The function body is calling `dbutils` directly — confirmed to never work inside a Unity Catalog Python Function; use `main.hsm.hsm_credentials()` instead (see above) |
| `ValueError: Could not deserialize key data. The data may be in an incorrect format, the provided password may be incorrect...` | Two likely causes: (1) the DEK-transport (or signing) private key was generated **with a passphrase** — `transport.py` always parses with no password; regenerate unencrypted (§0.2); (2) the secret's content is neither raw PEM nor valid base64 of PEM — both formats are accepted (`transport.py`'s `_normalize_key_material` auto-detects), so this means the upload itself was corrupted (wrong file, truncated, extra quoting). Verify without printing the raw value: `SELECT length(secret(...)) AS len` and compare against the expected length of your base64'd (or raw) key file |
| `ValueError: base64-decoded key material does not contain a PEM '-----BEGIN' marker` | The secret decoded from base64 fine but isn't actually a PEM key — wrong secret name/scope, or the value stored there is something else entirely |
| `ValueError: key material is neither raw PEM ... nor valid base64` | The secret's string value doesn't start with `-----BEGIN` and also isn't valid base64 — most likely it was stored with the file's raw bytes read incorrectly, or a placeholder/empty value was uploaded by mistake |
| `ValueError: RSA-OAEP unwrap failed -- the private key in use does not match the public_key_pem currently registered ...` (raised from `cache.get_or_unwrap_for_decrypt`, after `/dek/issue` or `/dek/unwrap` already succeeded) | The key **parses** fine but is the **wrong** key — hsm-core-service wraps every DEK against whatever `public_key_pem` is *currently* registered for this `app_id`. Most often: the private key was regenerated/re-exported (e.g. while fixing a base64 issue) without re-running step 0.3/`POST /admin/apps/keys` with the matching new public key, or `databricks-udf-private-key` and `databricks-udf-signing-key` got swapped in the secret scope. Fix: `openssl pkey -in <the exact private key file you're using> -pubout` and re-register that public key — this guarantees a match |
| `ConfigError: credentials_json is not valid JSON: ...` | `hsm_credentials()`'s `to_json(named_struct(...))` output isn't reaching the function correctly, or something else is being passed as the last argument |
| `ConfigError: HSM_SERVICE_BASE_URL is not set (expected as a credentials_json field)` | A required field is missing from `hsm_credentials()`'s `named_struct(...)` |
| `SvcClientError: /dek/issue -> 403: ...` | App not registered, wrong scope, or (for a cross-app `dek_name`) no grant — see [`java/docs/ADMIN_OPERATIONS.md`](../java/docs/ADMIN_OPERATIONS.md)'s `GET /admin/edek/{edekId}` support workflow |
| `SvcClientError: /dek/issue -> 422: App '...' has no public_key_pem registered` | Step 0.3 (register the public key) wasn't done for this `HSM_APP_ID` |
| `ConfigError: HSM_AUTH_MODE=SELF_SIGNED_JWT requires HSM_SIGNING_PRIVATE_KEY_PEM ...` | `HSM_AUTH_MODE` is `SELF_SIGNED_JWT` in `hsm_credentials()`'s output but `HSM_SIGNING_PRIVATE_KEY_PEM` (or `HSM_PRIVATE_KEY_PEM` as a fallback) wasn't included |
| `SvcClientError: /dek/issue -> 401: Invalid token signature` (with `HSM_AUTH_MODE=SELF_SIGNED_JWT`) | The private key actually signing the JWT doesn't match whatever `signing_public_key_pem` is registered for this `app_id` — same key-mismatch category as the RSA-OAEP failure above, but on the signing keypair. **If the exact same key values work from a JVM client (`hsm-spark-adapter`/`hsm-crypto-client`) against the same server, the keys and registration are proven fine** — the bug is in how the value reaches Python, not the key itself. The most common cause: `hsm_credentials()` is still wired from the `STATIC` template (setting `HSM_BEARER_TOKEN`, which `SELF_SIGNED_JWT` never reads) instead of the `SELF_SIGNED_JWT` template (setting `HSM_SIGNING_PRIVATE_KEY_PEM`) — swap to the commented-out `SELF_SIGNED_JWT` block in `sql/create_functions.sql`. If `HSM_SIGNING_PRIVATE_KEY_PEM` is present but resolves to an empty value, `Config.from_json`/`from_env` now raise a clear `ConfigError` instead of silently falling back to the transport key (`HSM_PRIVATE_KEY_PEM`) and producing this opaque 401 |
| `ConfigError: HSM_SIGNING_PRIVATE_KEY_PEM was provided but empty ...` | `hsm_credentials()` (or an env var) sets this field but it resolves to an empty string — check the secret name it references actually holds the signing key, not a leftover/wrong value |
| Timeout / connection error | Network egress from this compute type to `hsm-core-service` isn't allowed — see §4 for serverless specifically |
| `ImportError: No module named 'hsm_databricks_udf'` | The `ENVIRONMENT` clause's wheel path is wrong, or the wheel wasn't actually uploaded to that Unity Catalog volume path — see §0.6 |
| `PERMISSION_DENIED` calling `hsm_credentials()`/`hsm_encrypt`/`hsm_decrypt` | The caller needs `EXECUTE` on all three functions — they do **not** need `READ SECRET` on the `hsm` scope directly (see the governance note above); only the function owner does |

## 6. Open items before a production rollout

- See [`DATABRICKS_UDF_DESIGN.md`](../java/docs/DATABRICKS_UDF_DESIGN.md) §14
  — in particular, whether the RSA-OAEP transport-unwrap needs to stay inside
  a FIPS-140-validated module, still unconfirmed at the time this was built.
