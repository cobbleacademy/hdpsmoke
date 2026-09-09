# Deploying `hsm-databricks-udf` to Databricks

Companion to [`../java/docs/DATABRICKS_UDF_DESIGN.md`](../java/docs/DATABRICKS_UDF_DESIGN.md)
(the design/rationale) and [`sql/create_functions.sql`](sql/create_functions.sql)
(the actual `CREATE FUNCTION` DDL).

**Status:** the package itself is built and verified — its crypto is proven
byte-for-byte compatible with `hsm-core-service`'s real Java implementation in
both directions, including the real credential-passing call path described
below (see `tests/test_live_interop.py`, run for real against a live local
instance). **The Databricks-side deployment steps themselves have not been
run against a real Databricks workspace** — this repo has no Databricks
access.

## How credentials reach the function — and why this changed

An earlier version of this doc had the function body call
`dbutils.secrets.get(...)` directly inside `AS $$ ... $$`, relying on Unity
Catalog's secrets mechanism. **That does not work and was never actually
run against a live workspace before shipping** — confirmed directly against
Databricks' own docs and support KB once it was tried for real: `dbutils` is
only available in the calling notebook/job's *own driver context*, never
inside a UDF or Unity Catalog Python Function body. Calling it from inside a
`CREATE FUNCTION ... LANGUAGE PYTHON` body raises exactly
`NameError: name 'dbutils' is not defined`.

The fix, matching Databricks' own documented workaround for this exact
failure: **the caller fetches the secret where `dbutils` does work (their
own notebook/job) and passes it into the function call as an explicit
argument**, instead of the function resolving its own credentials. Both
functions now take a `credentials_json` argument — a JSON object holding the
same fields as this package's `HSM_*` config (see `config.py`'s
`Config.from_json`):

```python
import json

creds = json.dumps({
    "HSM_SERVICE_BASE_URL": "https://hsm-core-service.internal:8443/api/sensec/hsm/v1",
    "HSM_APP_ID": "databricks-udf",
    "HSM_AUTH_MODE": "STATIC",
    "HSM_PRIVATE_KEY_PEM": dbutils.secrets.get(scope="hsm", key="databricks-udf-private-key"),
    "HSM_BEARER_TOKEN": dbutils.secrets.get(scope="hsm", key="databricks-udf-token"),
})

df = spark.sql(
    "SELECT id, main.hsm.hsm_decrypt(ciphertext_token, :creds) AS account_number "
    "FROM main.payments.customer_accounts",
    args={"creds": creds},
)
```

Always pass `credentials_json` as a **bind parameter** (`args={...}` /
`:creds`), never string-interpolated into SQL text — interpolation would put
the raw private key and token into the query text itself (logged, visible in
query history).

**A real governance tradeoff versus the original design**: Unity Catalog's
definer-rights model (function creator holds the secret scope, callers only
need `EXECUTE`) is what the `dbutils`-in-body approach was meant to get —
callers would never need direct secret access. That's no longer true: since
credentials now travel as a caller-supplied argument, **every caller needs
their own `READ SECRET` grant on the scope holding this app's key/token**, in
addition to `EXECUTE` on the functions. This is a real, known regression
from the original design goal, not an oversight — it's the price of `dbutils`
not being callable inside the function body at all. If tighter control over
who can access the raw credential matters more than this UDF's convenience,
consider a small wrapper notebook/job that is the only principal with
`READ SECRET`, and grant other users access to *that* instead of the raw
scope.

## Why one flow now covers all three compute types for the wheel itself

`CREATE FUNCTION`'s `ENVIRONMENT` clause declares a function's Python
dependencies (PyPI packages, or a wheel path in a Unity Catalog volume) *as
part of the function's own definition* — confirmed directly against
Databricks' `CREATE FUNCTION` docs. This part of the original design still
holds: the wheel + its dependencies are resolved wherever the function
executes, not wherever it was registered from, regardless of compute type.
It's only the *credentials* mechanism above that had to change.

## 0. One-time prerequisites

1. **Register the calling app in `hsm-core-service`** (if not already), with
   at minimum the `dek_issue`/`dek_unwrap` scopes — see
   [`java/docs/APP_ONBOARDING.md`](../java/docs/APP_ONBOARDING.md).
2. **Generate an RSA keypair** for this app (the DEK-transport keypair —
   separate from any JWT-signing key):
   ```bash
   openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out hsm-databricks-key.pem
   openssl pkey -in hsm-databricks-key.pem -pubout -out hsm-databricks-key.pub.pem
   ```
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
     `HsmCryptoClient.Builder` and this package support) and register its
     public half via `POST /admin/apps/keys`' `signing_public_key_pem` field:
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
   Unlike the earlier design, this scope is now read by every *caller*
   (their own notebook/job, to build `credentials_json`), not by the
   function itself — see the governance-tradeoff note above for the access
   implications:
   ```bash
   databricks secrets create-scope hsm
   databricks secrets put-secret hsm databricks-udf-private-key --file hsm-databricks-key.pem

   # STATIC:
   databricks secrets put-secret hsm databricks-udf-token --string-value "$TOKEN"

   # SELF_SIGNED_JWT (instead of the token above):
   databricks secrets put-secret hsm databricks-udf-signing-key --file hsm-databricks-signing-key.pem
   ```
   Grant `READ SECRET` on this scope to whichever principals should be able
   to call `hsm_encrypt`/`hsm_decrypt`.

## 1. Register the functions

Edit the volume path in [`sql/create_functions.sql`](sql/create_functions.sql)
to match your deployment, then run it — from a notebook, the SQL editor, or a
job, on **any** compute type (job/classic, shared, or serverless all work
identically here, since nothing in this step is compute-specific):

```sql
-- contents of sql/create_functions.sql
```

Both functions now take `credentials_json` as their last argument — see
above for what to put in it and why.

## 2. Run it

From a notebook or job (where `dbutils` is available to fetch the secret):

```python
import json

creds = json.dumps({
    "HSM_SERVICE_BASE_URL": "https://hsm-core-service.internal:8443/api/sensec/hsm/v1",
    "HSM_APP_ID": "databricks-udf",
    "HSM_AUTH_MODE": "STATIC",
    "HSM_PRIVATE_KEY_PEM": dbutils.secrets.get(scope="hsm", key="databricks-udf-private-key"),
    "HSM_BEARER_TOKEN": dbutils.secrets.get(scope="hsm", key="databricks-udf-token"),
})

spark.sql(
    "SELECT main.hsm.hsm_encrypt(:pt, :dn, NULL, :creds) AS token",
    args={"pt": "4111-1111-1111-1234", "dn": "customers.account_number", "creds": creds},
).show()
# -> "v1.AbC123..."

spark.sql(
    "SELECT main.hsm.hsm_decrypt(:tok, :creds) AS plaintext",
    args={"tok": "v1.AbC123...", "creds": creds},
).show()
# -> "4111-1111-1111-1234"

# Over a real table:
spark.sql(
    "SELECT id, main.hsm.hsm_decrypt(ciphertext_token, :creds) AS account_number "
    "FROM main.payments.customer_accounts",
    args={"creds": creds},
).show()
```

A SQL warehouse query (no `dbutils` there either) needs `creds` built
upstream — e.g. by a notebook/job that resolves it once and writes it as a
session variable or query parameter the warehouse query then binds; a bare
`SELECT` typed directly into a SQL editor has no `dbutils` equivalent, so
this UDF is best driven from a notebook/job in practice.

## 3. Serverless-specific note: network egress

Python UDFs can reach external HTTPS endpoints on serverless — confirmed
directly against Databricks docs — but if the workspace runs *restricted*
serverless egress, `hsm-core-service`'s domain must be explicitly added:
Settings → Network → Network Policies → allowed internet destinations. This
is a one-time workspace-level change, unrelated to the function registration
above.

## 4. Verifying a deployment actually works

```python
creds = json.dumps({...})  # as above

result = spark.sql(
    "SELECT main.hsm.hsm_decrypt(main.hsm.hsm_encrypt(:pt, :dn, NULL, :creds), :creds) = :pt AS round_trip_ok",
    args={"pt": "test value", "dn": "deployment.verify.column", "creds": creds},
).collect()
assert result[0]["round_trip_ok"]
```

Cross-check against the real `/decrypt` endpoint directly (not through this
package at all) to prove the token these UDFs produce is genuinely
`hsm-core-service`'s own wire format, not just internally self-consistent:

```bash
TOKEN=$(databricks sql query "SELECT main.hsm.hsm_encrypt('cross-check', 'deployment.verify.column2', NULL, '$CREDS_JSON')" | tail -1)
curl -X POST "$BASE/decrypt" -H "Authorization: Bearer $SOME_TOKEN" -H "X-App-ID: databricks-udf" \
  -H "Content-Type: application/json" -d "{\"ciphertext\": \"$TOKEN\"}"
# -> {"plaintext": "cross-check", ...}
```

## 5. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `NameError: name 'dbutils' is not defined` (raised from inside `hsm_encrypt`/`hsm_decrypt`) | The function body is calling `dbutils` directly — confirmed to never work inside a Unity Catalog Python Function; fetch the secret in the calling notebook/job instead and pass it via `credentials_json` (see above) |
| `ConfigError: credentials_json is not valid JSON: ...` | The caller passed something other than a JSON object string as the last argument |
| `ConfigError: HSM_SERVICE_BASE_URL is not set (expected as a credentials_json field)` | A required field is missing from the `credentials_json` object the caller built |
| `SvcClientError: /dek/issue -> 403: ...` | App not registered, wrong scope, or (for a cross-app `dek_name`) no grant — see [`java/docs/ADMIN_OPERATIONS.md`](../java/docs/ADMIN_OPERATIONS.md)'s `GET /admin/edek/{edekId}` support workflow |
| `SvcClientError: /dek/issue -> 422: App '...' has no public_key_pem registered` | Step 0.3 (register the public key) wasn't done for this `HSM_APP_ID` |
| `ConfigError: HSM_AUTH_MODE=SELF_SIGNED_JWT requires HSM_SIGNING_PRIVATE_KEY_PEM ...` | `HSM_AUTH_MODE` is `SELF_SIGNED_JWT` in `credentials_json` but `HSM_SIGNING_PRIVATE_KEY_PEM` (or `HSM_PRIVATE_KEY_PEM` as a fallback) wasn't included |
| `SvcClientError: /dek/issue -> 401: ...` (with `HSM_AUTH_MODE=SELF_SIGNED_JWT`) | No `signing_public_key_pem` registered for this `app_id` (step 0.4), the `aud` claim doesn't match the server's `hsm.jwt.audience`, or the signing key PEM doesn't match what was registered |
| Timeout / connection error | Network egress from this compute type to `hsm-core-service` isn't allowed — see §3 for serverless specifically |
| `ImportError: No module named 'hsm_databricks_udf'` | The `ENVIRONMENT` clause's wheel path is wrong, or the wheel wasn't actually uploaded to that Unity Catalog volume path — see §0.6 |
| `PERMISSION_DENIED` fetching the secret in the calling notebook | The caller's own identity needs `READ SECRET` on the `hsm` scope now (see the governance-tradeoff note above) — `EXECUTE` on the function alone is no longer sufficient |

## 6. Open items before a production rollout

- The governance tradeoff above (callers need direct `READ SECRET` access,
  not just `EXECUTE`) — acceptable for this package's current scope, worth
  revisiting if broader/less-trusted caller access is needed later.
- See [`DATABRICKS_UDF_DESIGN.md`](../java/docs/DATABRICKS_UDF_DESIGN.md) §14
  — in particular, whether the RSA-OAEP transport-unwrap needs to stay inside
  a FIPS-140-validated module, still unconfirmed at the time this was built.
