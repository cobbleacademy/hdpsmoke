# Deploying `hsm-databricks-udf` to Databricks

Companion to [`../java/docs/DATABRICKS_UDF_DESIGN.md`](../java/docs/DATABRICKS_UDF_DESIGN.md)
(the design/rationale) and [`sql/create_functions.sql`](sql/create_functions.sql)
(the actual `CREATE FUNCTION` DDL).

**Status:** built and verified — its crypto is proven byte-for-byte
compatible with `hsm-core-service`'s real Java implementation in both
directions (see `tests/test_live_interop.py`, run for real against a live
local instance), **and confirmed working end-to-end against a real
Databricks workspace**, both `STATIC` and `SELF_SIGNED_JWT` auth modes. The
originally-confirmed pattern used the flat, explicit-`credentials_json`
functions (`hsm_encrypt_detail`/`hsm_decrypt_detail`) with `hsm_credentials()`
called from an ad-hoc query; the simplified `hsm_encrypt`/`hsm_decrypt`
wrappers were added afterward and required a real fix of their own (a
confirmed Unity Catalog bug — see "How credentials reach the function"
below) before they worked too. This repo itself has no Databricks access,
so all live verification happened on the deploying user's own workspace,
not something this session could run directly — treat compute types,
network policies, or steps not explicitly exercised there as still
unconfirmed rather than assuming full coverage.

## How credentials reach the function

An earlier version of this doc had the function body call
`dbutils.secrets.get(...)` directly inside `AS $$ ... $$`. **That does not
work** — confirmed directly against Databricks' own docs/KB: `dbutils` is
only available in the calling notebook/job's own driver context, never
inside a UDF or Unity Catalog Python Function body. It raises
`NameError: name 'dbutils' is not defined`.

The fix: the real crypto functions, `hsm_encrypt_detail`/`hsm_decrypt_detail`,
take an explicit `credentials_json` argument — a JSON object with the same
field names as this package's `HSM_*` config (see `config.py`'s
`Config.from_json`) — instead of resolving credentials internally.
`credentials_json` is built in **pure SQL**, with no `dbutils`/notebook/
Python required, via Databricks' built-in `secret(scope, key)` scalar
function (Databricks Runtime 11.3 LTS+ — a general SQL expression, not
something confined to `CREATE CONNECTION`) combined with
`to_json(named_struct(...))`.

Two simplified wrappers — `hsm_encrypt`/`hsm_decrypt` — apply that same
expression automatically, so the common case never has to mention
credentials at all:

```sql
SELECT main.hsm.hsm_decrypt(ciphertext_token)
FROM main.payments.customer_accounts;
```

**Confirmed bug, found live**: `hsm_encrypt`/`hsm_decrypt` do *not* get
there by calling a separate `hsm_credentials()` helper function from
their own body — an earlier version of this design tried exactly that, and
it reliably breaks. A `LANGUAGE SQL` function's body calling *another*
user-defined SQL function corrupts Unity Catalog's dependency tracking for
that specific edge, raising `UC_INVALID_DEPENDENCIES.SQL_UDF` on every
subsequent call — not fixed by restarting a cluster or SQL warehouse (UC
function metadata is metastore-level, not compute-scoped), nor by dropping
and recreating every function in the chain. Isolated by direct experiment:
`hsm_decrypt` (SQL) calling `hsm_decrypt_detail` (**Python**) is reliable;
`hsm_decrypt` (SQL) calling `hsm_credentials()` (**SQL**, zero-arg) is what
breaks. The original flat 2-argument design (confirmed working end-to-end
earlier) never hit this, because it called `hsm_credentials()` from an
*ad-hoc caller query*, never from inside another stored function's body —
a fundamentally different mechanism to Unity Catalog than one function's
definition referencing another's.

**Fix, and its real cost**: `hsm_encrypt`/`hsm_decrypt` now **inline** the
credentials-building expression directly in their own `RETURN` clause,
instead of calling `hsm_credentials()`. `hsm_credentials()` itself is kept
as a standalone function, safe to call from an *ad-hoc* query alongside
`hsm_encrypt_detail`/`hsm_decrypt_detail` (that's not a function-body-to-
function-body dependency, so it isn't affected) — just never from inside
another function's own definition. The cost: the credentials literal now
exists in **three places** in `sql/create_functions.sql`
(`hsm_credentials()`, `hsm_encrypt()`, `hsm_decrypt()`) — there is no single
source of truth anymore. Edit all three together on any change to base URL,
app_id, auth mode, or secret scope/key names.

**Governance — still better than the original `dbutils`-in-body design**: a
plain `CREATE FUNCTION ... LANGUAGE SQL` body runs with the **function
owner's** privileges by default (definer rights, same as a view) —
confirmed directly against Databricks' own docs. So `secret(...)` inside
`hsm_encrypt`/`hsm_decrypt`'s inlined expression checks the *owner's*
`READ SECRET` grant, never the caller's: only whoever registers these
functions needs `READ SECRET` on the `hsm` scope; a caller of `hsm_encrypt`/
`hsm_decrypt` needs only `EXECUTE` on those two and never touches the raw
secret.

## Two calling patterns — ad-hoc `SELECT` vs. persisted writes

Everything above works for an ad-hoc `SELECT` (a SQL editor query, a
dashboard, a plain read). **It does not work for `CREATE TABLE ... AS
SELECT` or `INSERT INTO ... SELECT`** — confirmed live, and this is a hard
Databricks platform restriction, not something more SQL cleverness fixes:

- **`SECRET_FUNCTION_INVALID_LOCATION`**: Databricks categorically blocks
  `secret(...)` from appearing anywhere in the expression tree feeding a
  *persisted* write, whether called directly or transitively through a
  UDF — confirmed against Databricks' own error-class docs ("you cannot
  execute INSERT command with... non-encrypted references to the SECRET
  function"). This is a static check on the write statement's own SQL text,
  regardless of whether the actual persisted output contains the secret
  value. Since `hsm_encrypt`/`hsm_decrypt` have `secret(...)` inlined
  directly in their bodies, *any* CTAS/`INSERT` calling them hits this.
- **Redaction on `.collect()`**: resolving `credentials_json` a level up —
  `spark.sql("SELECT main.hsm.hsm_credentials()").collect()[0][0]` — doesn't
  work around it either. Databricks redacts the *output* of any SQL command
  that invokes `secret()`, replacing it with the literal string
  `"[REDACTED]"` (confirmed live: a `credentials_json` built this way came
  back as a 292-character JSON blob with both key/token fields substituted
  for the 10-character string `"[REDACTED]"`, not the real values) — before
  the value ever reaches the Python driver. This applies however that SQL
  result is extracted, not just when printed/displayed.

**The confirmed, working fix for persisted writes**: resolve
`credentials_json` via `dbutils.secrets.get(...)` in the notebook/job's own
driver code — **never** via `hsm_credentials()` or SQL's `secret()` for this
path — and pass it into `hsm_encrypt_detail`/`hsm_decrypt_detail` (never the
simplified `hsm_encrypt`/`hsm_decrypt` wrappers, which have `secret(...)`
baked in) as a genuine bound parameter (`args={...}`, `:name` markers — not
string interpolation):

```python
import json

creds = json.dumps({
    "HSM_SERVICE_BASE_URL": "https://hsm-core-service.internal:8443/api/sensec/hsm/v1",
    "HSM_APP_ID": "databricks-udf",
    "HSM_AUTH_MODE": "STATIC",
    "HSM_PRIVATE_KEY_PEM": dbutils.secrets.get(scope="hsm", key="databricks-udf-private-key"),
    "HSM_BEARER_TOKEN": dbutils.secrets.get(scope="hsm", key="databricks-udf-token"),
})

spark.sql("""
    CREATE TABLE encrypted_customers AS
    SELECT id, main.hsm.hsm_encrypt_detail(ssn, 'customers.ssn', 'pii', :creds) AS ssn_token
    FROM customers
""", args={"creds": creds})
```

`dbutils.secrets.get(...)` runs entirely in the driver's own Python process
— it's not a SQL command invoking `secret()`, so neither restriction above
applies to it. Confirmed working end-to-end: a real `CREATE TABLE ... AS
SELECT main.hsm.hsm_encrypt_detail(...)` using exactly this pattern
succeeded, and the resulting table was readable.

**Decision rule**:
| Use case | Function | Credentials |
|---|---|---|
| Ad-hoc `SELECT` (no write) | `hsm_encrypt`/`hsm_decrypt` | Inlined automatically — nothing to do |
| `CREATE TABLE`/`INSERT INTO` (persisted write) | `hsm_encrypt_detail`/`hsm_decrypt_detail` | Build via `dbutils.secrets.get()` in the driver, pass as a bound parameter |

Bulk table encrypt/decrypt — the actual primary use case this package
exists for — always falls in the second row.

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
   Only the identity that will *register* these functions (their owner)
   needs `READ SECRET` on this scope — see the governance note above.

## 1. Register the functions

Edit the credentials literal in **all three places** it appears in
[`sql/create_functions.sql`](sql/create_functions.sql) — `hsm_credentials()`,
`hsm_encrypt()`, and `hsm_decrypt()` (see "How credentials reach the
function" above for why it's not centralized in one place) — plus the
volume path, to match your deployment. Then run the whole file — from a
notebook, the SQL editor, or a job, on **any** compute type (job/classic,
shared, or serverless all work identically here, since nothing in this step
is compute-specific).

**Always re-run the whole file, top-to-bottom, on any future edit.**
`sql/create_functions.sql` `DROP FUNCTION IF EXISTS`s `hsm_encrypt`/
`hsm_decrypt` before recreating them, since `CREATE OR REPLACE` cannot
change a function's parameter list, and those two changed arity earlier in
this design's evolution. If you still hit `UC_INVALID_DEPENDENCIES.SQL_UDF`
after that, it means a *stored function calling another stored SQL
function* — not restarting a cluster/warehouse, not dropping and
recreating, not the arity issue — see §5's troubleshooting table for the
confirmed root cause and fix.

## 2. Run it

Pure SQL, no notebook or `dbutils` required — the simplified wrappers cover
the common case:

```sql
SELECT main.hsm.hsm_encrypt('4111-1111-1111-1234', 'customers.account_number', 'pci') AS token;
-- -> "v1.AbC123..."

SELECT main.hsm.hsm_decrypt('v1.AbC123...') AS plaintext;
-- -> "4111-1111-1111-1234"

-- Over a real table:
SELECT id, main.hsm.hsm_decrypt(ciphertext_token) AS account_number
FROM main.payments.customer_accounts;
```

Need a *different* credential than this deployment's default (e.g. a
different `app_id`)? Use the `_detail` functions with an explicit
`credentials_json` instead:

```sql
SELECT main.hsm.hsm_decrypt_detail(ciphertext_token, main.hsm.hsm_credentials()) AS account_number
FROM main.payments.customer_accounts;
```

**Writing results to a table** (`CREATE TABLE ... AS SELECT`, `INSERT INTO
... SELECT`)? None of the examples above work for that — see "Two calling
patterns" above for the confirmed, working pattern (`_detail` functions +
`dbutils.secrets.get()`, never `secret()`/`hsm_credentials()`).

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
            SELECT main.hsm.hsm_decrypt(main.hsm.hsm_encrypt(%(pt)s, %(dn)s, NULL)) = %(pt)s AS round_trip_ok
        """, {"pt": "test value", "dn": "deployment.verify.column"})
        row = cur.fetchone()
        assert row.round_trip_ok
```

This is the recommended smoke test for a fresh deployment. `pip install
databricks-sql-connector` locally or run this from a notebook (where
`spark.sql(...)` works equally well as a substitute for the connector). To
cross-check against the real `/decrypt` endpoint directly — proving the
token these UDFs produce is genuinely `hsm-core-service`'s own wire format,
not just internally self-consistent:

```bash
TOKEN=$(databricks sql query "SELECT main.hsm.hsm_encrypt('cross-check', 'deployment.verify.column2', NULL)" | tail -1)
curl -X POST "$BASE/decrypt" -H "Authorization: Bearer $SOME_TOKEN" -H "X-App-ID: databricks-udf" \
  -H "Content-Type: application/json" -d "{\"ciphertext\": \"$TOKEN\"}"
# -> {"plaintext": "cross-check", ...}
```

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
| `NameError: name 'dbutils' is not defined` (raised from inside `hsm_encrypt`/`hsm_decrypt`) | The function body is calling `dbutils` directly — confirmed to never work inside a Unity Catalog Python Function; use the `secret(...)`/`to_json(named_struct(...))` pattern instead (see above) |
| `ValueError: Could not deserialize key data. The data may be in an incorrect format, the provided password may be incorrect...` | Two likely causes: (1) the DEK-transport (or signing) private key was generated **with a passphrase** — `transport.py` always parses with no password; regenerate unencrypted (§0.2); (2) the secret's content is neither raw PEM nor valid base64 of PEM — both formats are accepted (`transport.py`'s `_normalize_key_material` auto-detects), so this means the upload itself was corrupted (wrong file, truncated, extra quoting). Verify without printing the raw value: `SELECT length(secret(...)) AS len` and compare against the expected length of your base64'd (or raw) key file |
| `ValueError: base64-decoded key material does not contain a PEM '-----BEGIN' marker` | The secret decoded from base64 fine but isn't actually a PEM key — wrong secret name/scope, or the value stored there is something else entirely |
| `ValueError: key material is neither raw PEM ... nor valid base64` | Two possible causes, confirmed both live: (1) the secret was stored with the file's raw bytes read incorrectly, or a placeholder/empty value was uploaded by mistake; (2) **`credentials_json` was built via `hsm_credentials()`/SQL's `secret()`, extracted with `.collect()`, and got redacted** — see "Two calling patterns" above. Check `len()` of the field, not just whether it parses: a 10-character value is almost certainly the literal string `"[REDACTED]"`, not a real key |
| `SECRET_FUNCTION_INVALID_LOCATION` | `secret(...)` (directly, via `hsm_credentials()`, or inlined in `hsm_encrypt`/`hsm_decrypt`) appears somewhere feeding a `CREATE TABLE`/`INSERT INTO` — Databricks blocks this categorically, confirmed live and via Databricks' own error-class docs, regardless of whether the actual persisted value contains the secret. See "Two calling patterns" above: use `hsm_encrypt_detail`/`hsm_decrypt_detail` with `credentials_json` built via `dbutils.secrets.get()` for any persisted write |
| `ValueError: RSA-OAEP unwrap failed -- the private key in use does not match the public_key_pem currently registered ...` (raised from `cache.get_or_unwrap_for_decrypt`, after `/dek/issue` or `/dek/unwrap` already succeeded) | The key **parses** fine but is the **wrong** key — hsm-core-service wraps every DEK against whatever `public_key_pem` is *currently* registered for this `app_id`. Most often: the private key was regenerated/re-exported (e.g. while fixing a base64 issue) without re-running step 0.3/`POST /admin/apps/keys` with the matching new public key, or `databricks-udf-private-key` and `databricks-udf-signing-key` got swapped in the secret scope. Fix: `openssl pkey -in <the exact private key file you're using> -pubout` and re-register that public key — this guarantees a match |
| `ConfigError: credentials_json is not valid JSON: ...` | The `to_json(named_struct(...))` output (inlined in `hsm_encrypt`/`hsm_decrypt`, or `hsm_credentials()`'s output if calling the `_detail` functions directly) isn't reaching the function correctly, or something else is being passed as the last argument |
| `ConfigError: HSM_SERVICE_BASE_URL is not set (expected as a credentials_json field)` | A required field is missing from the `named_struct(...)` — remember it now appears in three places (`hsm_credentials()`, `hsm_encrypt()`, `hsm_decrypt()`); check whichever one you actually called |
| `SvcClientError: /dek/issue -> 403: ...` | App not registered, wrong scope, or (for a cross-app `dek_name`) no grant — see [`java/docs/ADMIN_OPERATIONS.md`](../java/docs/ADMIN_OPERATIONS.md)'s `GET /admin/edek/{edekId}` support workflow |
| `SvcClientError: /dek/issue -> 422: App '...' has no public_key_pem registered` | Step 0.3 (register the public key) wasn't done for this `HSM_APP_ID` |
| `ConfigError: HSM_AUTH_MODE=SELF_SIGNED_JWT requires HSM_SIGNING_PRIVATE_KEY_PEM ...` | `HSM_AUTH_MODE` is `SELF_SIGNED_JWT` in the credentials expression but `HSM_SIGNING_PRIVATE_KEY_PEM` (or `HSM_PRIVATE_KEY_PEM` as a fallback) wasn't included — check every place the expression appears, not just the one you last edited |
| `SvcClientError: /dek/issue -> 401: Invalid token signature` (with `HSM_AUTH_MODE=SELF_SIGNED_JWT`) | The private key actually signing the JWT doesn't match whatever `signing_public_key_pem` is registered for this `app_id` — same key-mismatch category as the RSA-OAEP failure above, but on the signing keypair. **If the exact same key values work from a JVM client (`hsm-spark-adapter`/`hsm-crypto-client`) against the same server, the keys and registration are proven fine** — the bug is in how the value reaches Python, not the key itself. The most common cause: one of the three credentials-expression copies is still wired from the `STATIC` template (setting `HSM_BEARER_TOKEN`, which `SELF_SIGNED_JWT` never reads) instead of the `SELF_SIGNED_JWT` template (setting `HSM_SIGNING_PRIVATE_KEY_PEM`) — since there's no single source of truth anymore, a mismatch between which copy you edited and which one the failing call actually used is easy to introduce. If `HSM_SIGNING_PRIVATE_KEY_PEM` is present but resolves to an empty value, `Config.from_json`/`from_env` now raise a clear `ConfigError` instead of silently falling back to the transport key (`HSM_PRIVATE_KEY_PEM`) and producing this opaque 401 |
| `ConfigError: HSM_SIGNING_PRIVATE_KEY_PEM was provided but empty ...` | One of the three credentials-expression copies sets this field but it resolves to an empty string — check the secret name it references actually holds the signing key, not a leftover/wrong value |
| Timeout / connection error | Network egress from this compute type to `hsm-core-service` isn't allowed — see §4 for serverless specifically |
| `ImportError: No module named 'hsm_databricks_udf'` | The `ENVIRONMENT` clause's wheel path is wrong, or the wheel wasn't actually uploaded to that Unity Catalog volume path — see §0.6 |
| `PERMISSION_DENIED` calling `hsm_encrypt`/`hsm_decrypt` | The caller needs `EXECUTE` on `hsm_encrypt`/`hsm_decrypt` only — their credentials expression is inlined, so they don't call `hsm_credentials()` or the `_detail` functions at all, and the caller never needs `READ SECRET` on the `hsm` scope (see the governance note above) |
| `PERMISSION_DENIED` calling `hsm_encrypt_detail`/`hsm_decrypt_detail`/`hsm_credentials()` directly | These need their own `EXECUTE` grant, separate from `hsm_encrypt`/`hsm_decrypt` — see the `hsm-power-users` example grant in `sql/create_functions.sql` |
| `UC_INVALID_DEPENDENCIES.SQL_UDF` (calling `hsm_encrypt`/`hsm_decrypt`) | **Confirmed root cause**: a `LANGUAGE SQL` function's body calling *another user-defined SQL function* reliably corrupts Unity Catalog's dependency tracking for that edge — isolated by direct experiment (`hsm_decrypt` calling `hsm_decrypt_detail`, Python, is reliable; `hsm_decrypt` calling `hsm_credentials()`, SQL, is what breaks). **Not fixed by**: restarting a cluster or SQL warehouse (UC function metadata is metastore-level, not compute-scoped); dropping and recreating every function in the chain, in order; reducing UDF-call count (a lone `hsm_decrypt()` call, at 3 nested invocations, is nowhere near Databricks' 5-per-query limit). **Fix, already applied here**: `hsm_encrypt`/`hsm_decrypt` inline the credentials expression directly rather than calling `hsm_credentials()` — if you're seeing this on your own copy of the SQL, check it matches the current `sql/create_functions.sql` (inlined, not a call to `hsm_credentials()`) |

## 6. Open items before a production rollout

- See [`DATABRICKS_UDF_DESIGN.md`](../java/docs/DATABRICKS_UDF_DESIGN.md) §14
  — in particular, whether the RSA-OAEP transport-unwrap needs to stay inside
  a FIPS-140-validated module, still unconfirmed at the time this was built.
