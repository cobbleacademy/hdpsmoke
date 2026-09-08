# Deploying `hsm-databricks-udf` to Databricks

Companion to [`../java/docs/DATABRICKS_UDF_DESIGN.md`](../java/docs/DATABRICKS_UDF_DESIGN.md)
(the design/rationale) and [`sql/create_functions.sql`](sql/create_functions.sql)
(the actual `CREATE FUNCTION` DDL).

**Status:** the package itself is built and verified — its crypto is proven
byte-for-byte compatible with `hsm-core-service`'s real Java implementation in
both directions (see `tests/test_live_interop.py`, run for real against a live
local instance while this was built). **The steps below have not been run
against a real Databricks workspace** — this repo has no Databricks access.
One specific step is called out below as needing a smoke test before you rely
on it for real: whether `dbutils` is available unqualified inside a Unity
Catalog Python function body.

## Why one flow now covers all three compute types

An earlier draft of this doc had separate instructions per compute type
(cluster-attached libraries for job/classic, an admin-allowlisted volume for
shared, a notebook-scoped `%pip install` for serverless). That was working
around a real limitation instead of using what Databricks actually built for
this: `CREATE FUNCTION`'s `ENVIRONMENT` clause declares a function's Python
dependencies (PyPI packages, or a wheel path in a Unity Catalog volume) *as
part of the function's own definition* — confirmed directly against
Databricks' `CREATE FUNCTION` docs. Combined with `dbutils.secrets.get(...)`
for credentials (Unity Catalog's own governed secrets mechanism, also
confirmed directly, not assumed), the function carries everything it needs
wherever it's invoked from — no separate per-cluster setup, no dependency on
whichever session happened to register it.

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
   chosen auth mode needs, plus the DEK-transport private key either way. The
   person who runs `sql/create_functions.sql` (the function's *creator*)
   needs `READ SECRET` on this scope — callers of the function later need
   only `EXECUTE` on the function itself, never access to the scope (Unity
   Catalog's definer-rights model for secrets used inside a function):
   ```bash
   databricks secrets create-scope hsm
   databricks secrets put-secret hsm databricks-udf-private-key --file hsm-databricks-key.pem

   # STATIC:
   databricks secrets put-secret hsm databricks-udf-token --string-value "$TOKEN"

   # SELF_SIGNED_JWT (instead of the token above):
   databricks secrets put-secret hsm databricks-udf-signing-key --file hsm-databricks-signing-key.pem
   ```

## 1. Register the functions

Edit the `HSM_SERVICE_BASE_URL`/`HSM_APP_ID` values and the volume path in
[`sql/create_functions.sql`](sql/create_functions.sql) to match your
deployment, and pick the bootstrap block matching your chosen auth mode (the
file shows `STATIC` inline in both function bodies, with the
`SELF_SIGNED_JWT` variant given as a commented-out alternative to swap in —
use one or the other, not both, for a given `app_id`). Then run it — from a
notebook, the SQL editor, or a job, on **any** compute type (job/classic,
shared, or serverless all work identically here, since nothing in this step
is compute-specific):

```sql
-- contents of sql/create_functions.sql
```

The `ENVIRONMENT` clause resolves the wheel + its dependencies wherever the
function later executes; the `dbutils.secrets.get(...)` calls inside the
function body resolve credentials the same way, regardless of which
cluster/warehouse a future caller uses.

**Smoke-test this specific step before relying on it**: whether `dbutils` is
available as a bare name inside a Unity Catalog Python function's `AS $$ ...
$$` body (versus needing an explicit import) isn't something this
environment could verify against a real workspace. Run one call and confirm
it works:

```sql
SELECT main.hsm.hsm_encrypt('smoke test', 'deployment.smoke.test');
```

If `dbutils` isn't in scope there, the fallback is unchanged from before:
`config.py` reads `os.environ` regardless of how those variables got set, so
setting `HSM_SERVICE_BASE_URL`/`HSM_APP_ID`/`HSM_AUTH_MODE` plus either
`HSM_BEARER_TOKEN` (`STATIC`) or `HSM_SIGNING_PRIVATE_KEY_PEM`
(`SELF_SIGNED_JWT`), and `HSM_PRIVATE_KEY_PEM` either way, as cluster-level
environment variables (job/classic/shared clusters support this directly;
serverless would need them set via `os.environ[...] = ...` in whatever code
path actually runs first) still works — only the *how credentials get into
the process* changes, not anything in the package itself.

## 2. Run it

```sql
SELECT main.hsm.hsm_encrypt('4111-1111-1111-1234', 'customers.account_number', 'pci');
-- -> "v1.AbC123..."

SELECT main.hsm.hsm_decrypt('v1.AbC123...');
-- -> "4111-1111-1111-1234"

-- Over a real table:
SELECT id, main.hsm.hsm_decrypt(ciphertext_token) AS account_number
FROM main.payments.customer_accounts;
```

## 3. Serverless-specific note: network egress

Python UDFs can reach external HTTPS endpoints on serverless — confirmed
directly against Databricks docs — but if the workspace runs *restricted*
serverless egress, `hsm-core-service`'s domain must be explicitly added:
Settings → Network → Network Policies → allowed internet destinations. This
is a one-time workspace-level change, unrelated to the function registration
above.

## 4. Verifying a deployment actually works

```sql
-- Round trip through the UDFs themselves
SELECT main.hsm.hsm_decrypt(main.hsm.hsm_encrypt('test value', 'deployment.verify.column')) = 'test value' AS round_trip_ok;
-- -> true
```

Cross-check against the real `/decrypt` endpoint directly (not through this
package at all) to prove the token these UDFs produce is genuinely
`hsm-core-service`'s own wire format, not just internally self-consistent:

```bash
TOKEN=$(databricks sql query "SELECT main.hsm.hsm_encrypt('cross-check', 'deployment.verify.column2')" | tail -1)
curl -X POST "$BASE/decrypt" -H "Authorization: Bearer $SOME_TOKEN" -H "X-App-ID: databricks-udf" \
  -H "Content-Type: application/json" -d "{\"ciphertext\": \"$TOKEN\"}"
# -> {"plaintext": "cross-check", ...}
```

## 5. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `ConfigError: HSM_SERVICE_BASE_URL is not set` | The `dbutils.secrets.get(...)` bootstrap in the function body didn't run or `dbutils` wasn't in scope — see §1's smoke test |
| `NameError: name 'dbutils' is not defined` | `dbutils` isn't available unqualified in this function body context — fall back to cluster-level environment variables, see §1 |
| `SvcClientError: /dek/issue -> 403: ...` | App not registered, wrong scope, or (for a cross-app `dek_name`) no grant — see [`java/docs/ADMIN_OPERATIONS.md`](../java/docs/ADMIN_OPERATIONS.md)'s `GET /admin/edek/{edekId}` support workflow |
| `SvcClientError: /dek/issue -> 422: App '...' has no public_key_pem registered` | Step 0.3 (register the public key) wasn't done for this `HSM_APP_ID` |
| `ConfigError: HSM_AUTH_MODE=SELF_SIGNED_JWT requires HSM_SIGNING_PRIVATE_KEY_PEM ...` | `HSM_AUTH_MODE` is set to `SELF_SIGNED_JWT` but the bootstrap block wasn't swapped to set `HSM_SIGNING_PRIVATE_KEY_PEM` — see §0.4/§1 |
| `SvcClientError: /dek/issue -> 401: ...` (with `HSM_AUTH_MODE=SELF_SIGNED_JWT`) | No `signing_public_key_pem` registered for this `app_id` (step 0.4), the `aud` claim doesn't match the server's `hsm.jwt.audience`, or the signing key PEM doesn't match what was registered |
| Timeout / connection error | Network egress from this compute type to `hsm-core-service` isn't allowed — see §3 for serverless specifically |
| `ImportError: No module named 'hsm_databricks_udf'` | The `ENVIRONMENT` clause's wheel path is wrong, or the wheel wasn't actually uploaded to that Unity Catalog volume path — see §0.6 |

## 6. Open items before a production rollout

- The `dbutils`-inside-function-body wiring (§1) — needs a real smoke test.
- See [`DATABRICKS_UDF_DESIGN.md`](../java/docs/DATABRICKS_UDF_DESIGN.md) §14
  — in particular, whether the RSA-OAEP transport-unwrap needs to stay inside
  a FIPS-140-validated module, still unconfirmed at the time this was built.
