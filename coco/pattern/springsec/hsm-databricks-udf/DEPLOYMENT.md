# Deploying `hsm-databricks-udf` to Databricks

Companion to [`../java/docs/DATABRICKS_UDF_DESIGN.md`](../java/docs/DATABRICKS_UDF_DESIGN.md)
(the design/rationale) and [`sql/create_functions.sql`](sql/create_functions.sql)
(the actual `CREATE FUNCTION` DDL). This doc is the concrete "how do I actually
get this running" walkthrough per compute type, plus example queries.

**Status:** the package itself is built and verified — its crypto is proven
byte-for-byte compatible with `hsm-core-service`'s real Java implementation in
both directions (see `tests/test_live_interop.py`, run for real against a live
local instance while this was built). **The Databricks-side steps below have
not been run against a real Databricks workspace** — this repo has no
Databricks access. Treat them as the concrete, ready-to-execute plan, not
something already confirmed working on Databricks itself.

## 0. One-time prerequisites, regardless of compute type

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
4. **Get a bearer token** for this app — a static token today (see
   `config.py`'s note on `SELF_SIGNED_JWT`/mTLS as follow-ups, not yet built).
5. **Build the wheel**:
   ```bash
   cd hsm-databricks-udf
   python -m build   # produces dist/hsm_databricks_udf-0.1.0-py3-none-any.whl
   ```

## 1. Job clusters / classic all-purpose clusters

The straightforward path — full control over cluster config and libraries.

1. **Upload the wheel** to a Unity Catalog volume or DBFS:
   ```
   databricks fs cp dist/hsm_databricks_udf-0.1.0-py3-none-any.whl \
     dbfs:/FileStore/hsm-databricks-udf/hsm_databricks_udf-0.1.0-py3-none-any.whl
   ```
2. **Attach it as a cluster library** — Compute → your cluster → Libraries →
   Install New → select the uploaded wheel.
3. **Set cluster environment variables** — Compute → your cluster → Edit →
   Advanced options → Environment variables:
   ```
   HSM_SERVICE_BASE_URL=https://hsm-core-service.internal:8443/api/sensec/hsm/v1
   HSM_APP_ID=databricks-udf
   HSM_BEARER_TOKEN={{secrets/hsm/databricks-udf-token}}
   HSM_PRIVATE_KEY_PEM={{secrets/hsm/databricks-udf-private-key}}
   ```
   The `{{secrets/scope/key}}` syntax pulls from a Databricks secret scope —
   never paste the token/private key in plaintext. Create the scope first:
   ```bash
   databricks secrets create-scope hsm
   databricks secrets put-secret hsm databricks-udf-token --string-value "$TOKEN"
   databricks secrets put-secret hsm databricks-udf-private-key --file hsm-databricks-key.pem
   ```
4. **Register the functions**: run [`sql/create_functions.sql`](sql/create_functions.sql)
   in a notebook cell or via the SQL editor, attached to this cluster.
5. **Run it**:
   ```sql
   SELECT main.hsm.hsm_encrypt('4111-1111-1111-1234', 'customers.account_number', 'pci');
   -- -> "v1.AbC123..."

   SELECT main.hsm.hsm_decrypt('v1.AbC123...');
   -- -> "4111-1111-1111-1234"

   -- Over a real table:
   SELECT id, main.hsm.hsm_decrypt(ciphertext_token) AS account_number
   FROM main.payments.customer_accounts;
   ```

## 2. Shared clusters (Unity Catalog standard access mode)

Same steps as job clusters (§1), plus one extra, admin-only step:

1. **Request the wheel be allowlisted** for standard-access-mode compute —
   a workspace admin runs:
   ```sql
   -- Or via the UI: Data Governance -> Unity Catalog -> Allowlist
   ALLOW LIBRARY '/Volumes/main/hsm/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl' FOR ALL CLUSTERS;
   ```
   (Custom JARs/wheels on standard-access-mode clusters require this —
   DBR 13.3+, confirmed against current Databricks docs.) Uploading the wheel
   to a Unity Catalog volume rather than DBFS is the recommended path here,
   since volumes are what the allowlist mechanism is built around.
2. Steps 3–5 from §1 are identical — environment variables, `CREATE FUNCTION`,
   and the same `SELECT hsm_decrypt(...)` queries work unchanged, since Unity
   Catalog Functions are the same governed object regardless of which
   compute type executes them.

## 3. Serverless (notebooks, jobs, SQL warehouses)

No cluster to configure — environment variables and libraries work
differently here.

1. **Publish the wheel to a Unity Catalog volume** (serverless doesn't
   support DBFS-attached compute-scoped libraries the way clusters do):
   ```
   databricks fs cp dist/hsm_databricks_udf-0.1.0-py3-none-any.whl \
     dbfs:/Volumes/main/hsm/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl
   ```
2. **Install it notebook-scoped**, in the first cell of the notebook/job that
   registers or calls the functions:
   ```python
   %pip install /Volumes/main/hsm/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl
   dbutils.library.restartPython()
   ```
3. **Set environment variables via secrets, read in-notebook** (serverless has
   no cluster-level environment-variable UI):
   ```python
   import os
   os.environ["HSM_SERVICE_BASE_URL"] = "https://hsm-core-service.internal:8443/api/sensec/hsm/v1"
   os.environ["HSM_APP_ID"] = "databricks-udf"
   os.environ["HSM_BEARER_TOKEN"] = dbutils.secrets.get(scope="hsm", key="databricks-udf-token")
   os.environ["HSM_PRIVATE_KEY_PEM"] = dbutils.secrets.get(scope="hsm", key="databricks-udf-private-key")
   ```
4. **If the workspace runs restricted serverless egress**, allowlist
   `hsm-core-service`'s domain — Settings → Network → Network Policies →
   add the domain to the allowed internet destinations. Confirmed directly
   against Databricks docs: serverless Python UDFs can reach external HTTPS
   endpoints on port 443, but a restricted-egress workspace still needs the
   domain explicitly added.
5. **Register the functions and query**, identical to §1 step 4–5 — run
   `sql/create_functions.sql`, then:
   ```sql
   SELECT main.hsm.hsm_decrypt(ciphertext_token) FROM main.payments.customer_accounts LIMIT 10;
   ```

## 4. Verifying a deployment actually works

Before trusting any of the above in a real workflow, run the same round-trip
this package's own test suite already proves works against the JVM side:

```sql
-- Round trip through the UDFs themselves
SELECT main.hsm.hsm_decrypt(main.hsm.hsm_encrypt('test value', 'deployment.verify.column')) = 'test value' AS round_trip_ok;
-- -> true

-- Cross-check against the real /decrypt endpoint directly (not through this
-- package at all) -- proves the token these UDFs produce is genuinely
-- hsm-core-service's own wire format, not just internally self-consistent:
```
```bash
TOKEN=$(databricks sql query "SELECT main.hsm.hsm_encrypt('cross-check', 'deployment.verify.column2')" | tail -1)
curl -X POST "$BASE/decrypt" -H "Authorization: Bearer $SOME_TOKEN" -H "X-App-ID: databricks-udf" \
  -H "Content-Type: application/json" -d "{\"ciphertext\": \"$TOKEN\"}"
# -> {"plaintext": "cross-check", ...}
```

## 5. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `ConfigError: HSM_SERVICE_BASE_URL is not set` | Environment variable not set on this compute type — see the compute-specific step above |
| `SvcClientError: /dek/issue -> 403: ...` | App not registered, wrong scope, or (for a cross-app `dek_name`) no grant — see [`java/docs/ADMIN_OPERATIONS.md`](../java/docs/ADMIN_OPERATIONS.md)'s `GET /admin/edek/{edekId}` support workflow |
| `SvcClientError: /dek/issue -> 422: App '...' has no public_key_pem registered` | Step 0.3 (register the public key) wasn't done for this `HSM_APP_ID` |
| Timeout / connection error | Network egress from this compute type to `hsm-core-service` isn't allowed — see §3 step 4 for serverless specifically |
| `ImportError: No module named 'hsm_databricks_udf'` | Wheel not installed on this compute, or (serverless) `%pip install` didn't run before the function's first call |

## 6. Open items before a production rollout

See [`DATABRICKS_UDF_DESIGN.md`](../java/docs/DATABRICKS_UDF_DESIGN.md) §14 —
in particular, whether the RSA-OAEP transport-unwrap needs to stay inside a
FIPS-140-validated module, still unconfirmed at the time this was built.
