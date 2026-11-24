# hsm-databricks-udf

`hsm_encrypt`/`hsm_decrypt` as Unity Catalog Python Functions — invocable
from SQL, notebooks, Delta Live Tables, and jobs across job clusters, Unity
Catalog shared clusters, and serverless compute, unlike `hsm-spark-adapter`
(a JVM Spark extension, which is excluded from shared clusters and serverless
by Databricks design).

- **Design/rationale:** [`../java/docs/DATABRICKS_UDF_DESIGN.md`](../java/docs/DATABRICKS_UDF_DESIGN.md)
- **Deployment steps per compute type + example queries:** [`DEPLOYMENT.md`](DEPLOYMENT.md)
- **`CREATE FUNCTION` DDL:** [`sql/create_functions.sql`](sql/create_functions.sql)

It also holds **`hsm-deid`**, the de-identified CSV export for Databricks: reads a
Delta table, de-identifies it with an approved profile, writes one CSV and its
manifest to a volume (package `hsm_databricks_udf.deid`).

- **Design:** [`../java/docs/DEIDENTIFICATION.md`](../java/docs/DEIDENTIFICATION.md) section 17
- **Deployment:** [`DEID_DEPLOYMENT.md`](DEID_DEPLOYMENT.md); job definitions in [`jobs/`](jobs/)

## Layout

```
src/hsm_databricks_udf/
  dek_manager.py    # AES-256-GCM + token pack/unpack, vendored from app/crypto/dek_manager.py
  iv_factory.py      # IV generation, vendored from app/crypto/iv_factory.py
  transport.py        # RSA-OAEP-256 wrap/unwrap, Python port of TransportWrapper.java
  svc_client.py        # HTTP client for hsm-core-service's /dek/issue, /dek/unwrap
  cache.py              # per-worker-process DEK cache, mirrors HsmCryptoClient's model
  config.py              # environment-variable configuration
  udf.py                  # hsm_encrypt / hsm_decrypt entry points
  deid/                   # hsm-deid: profiles, validator, methods, the Spark job (DEIDENTIFICATION.md §17)
jobs/                     # Databricks job definitions for hsm-deid (serverless, dedicated, init-key STATIC and JWT)
  test/                   # a first test on a workspace: table, profiles, job (DEID_DEPLOYMENT.md §11)
```

## Running the tests

```bash
pip install -e ".[dev]"  # or: pip install -e . && pip install pytest
pytest tests/ -v
```

The `hsm-deid` Spark and Postgres tests are skipped there. To run every test with
them, in Docker (Spark 4.0.1 and Postgres 16):

```bash
tests/deid/spark/run-tests.sh
```

`tests/test_dek_manager.py`, `tests/test_transport.py`, and
`tests/test_owner_app_id_wiring.py` run with no network dependency — safe for
CI. `tests/test_live_interop.py` is opt-in (skipped unless
`HSM_LIVE_TEST_BASE_URL` is set) and proves this package's crypto is
byte-for-byte compatible with the real Java `hsm-core-service` implementation,
in both directions:

```bash
cd ../java && mvn -q -pl hsm-core-service -am package -DskipTests
java -jar hsm-core-service/target/hsm-core-service.jar &
HSM_LIVE_TEST_BASE_URL=http://localhost:3005/api/sensec/hsm/v1 pytest tests/test_live_interop.py -v
```

## A note on why `owner_app_id` appears everywhere in this package

`hsm-core-service`'s `/dek/issue` and `/dek/unwrap` responses report the
DEK's true, permanent owner (`owner_app_id`) alongside the wrapped key. This
package always uses that value — never its own configured `HSM_APP_ID` — as
the AES-256-GCM AAD. That distinction matters the moment a grant-authorized
cross-app `dek_name` reuse is in play: using the wrong identity here silently
produces a ciphertext nothing can ever decrypt again. This was a real,
confirmed bug in `hsm-core-service` itself (fixed the same round this package
was built — see `EncryptionService.ResolvedDek`'s javadoc on the Java side)
before it could have been quietly baked into this package too.
