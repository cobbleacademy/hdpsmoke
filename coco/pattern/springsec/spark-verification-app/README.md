# spark-verification-app

Local, no-cluster verification of `hsm-spark-adapter`'s `hsm_encrypt`/`hsm_decrypt`
Spark SQL functions against a real `hsm-core-service` instance. No
`spark-submit`, no `start-master.sh`/`start-worker.sh` -- a plain
`master("local[*]")` `SparkSession` runs entirely in-process in one JVM.

This project is deliberately **independent of the `java/` Maven reactor**
(no `<parent>`, no relation to `hsm_bouncy/java/pom.xml`). That's not
incidental -- the reactor's root `pom.xml` imports `netty-bom` and forces a
specific patched Netty version project-wide (for CVEs `azure-sdk-bom` would
otherwise pull in). That override also silently applies to `spark-sql` if
it's ever resolved *through* that reactor (e.g. `mvn compile exec:java` run inside
`java/hsm-spark-adapter`, or an IDE run configuration pointed at that
module), downgrading Spark's own required Netty version and breaking
`SparkContext` at startup (`NoClassDefFoundError: KQueueIoHandler`). A
genuinely separate project like this one doesn't inherit that override, so
Spark resolves its own correct Netty version normally. See
`java/docs/SPARK_ADAPTER.md`'s "Testing locally without a cluster" section
for the full writeup.

## Setup

Two jars must be present alongside this `pom.xml` before building (or point
the two Maven properties below at wherever you keep them instead):

1. **`hsm-spark-adapter-1.0.0.jar`** -- build it from the real reactor:
   ```bash
   cd ../java && mvn -pl hsm-spark-adapter -am package -DskipTests
   cp hsm-spark-adapter/target/hsm-spark-adapter-1.0.0.jar ../spark-verification-app/
   ```

2. **`bc-fips-2.1.1.jar`** -- deliberately excluded from the shaded jar above
   (BC-FIPS validates its own module integrity via a self-checksum that
   breaks if it's repackaged into any uber-jar -- a documented BC-FIPS
   constraint, not a bug in the shading config; see `SPARK_ADAPTER.md`).
   Building step 1 above resolves it into your local Maven repo as a normal
   transitive dependency, so it's already sitting at:
   ```bash
   cp ~/.m2/repository/org/bouncycastle/bc-fips/2.1.1/bc-fips-2.1.1.jar .
   ```

If you'd rather not copy the jars into this directory, override their
locations instead:
```bash
mvn ... -Dhsm.spark.adapter.jar=/path/to/hsm-spark-adapter-1.0.0.jar \
        -Dhsm.bc.fips.jar=/path/to/bc-fips-2.1.1.jar
```

## Configuration

Two ways to supply connection/credential details -- pick one.

### Option A: environment variables (default)

Set these as environment variables before running -- they map directly onto
the same `spark.hsm.*` Spark conf keys a real deployment uses (see
`SPARK_ADAPTER.md`'s Configuration table for the authoritative reference).

| Env var | Required when | Value |
|---|---|---|
| `HSM_BASE_URL` | always | hsm-core-service base URL, e.g. `http://localhost:3005` (must be `https://...` for `MTLS`) |
| `HSM_APP_ID` | always | e.g. `payments-svc` |
| `HSM_AUTH_MODE` | always | one of `STATIC` \| `AZURE_AD` \| `SELF_SIGNED_JWT` \| `MTLS` |
| `HSM_PRIVATE_KEY_PATH` | always | file path to the **DEK-transport** private key PEM (matches `app_registrations.public_key_pem`/`encryption_public_key_pem`) |
| `HSM_API_V1_PREFIX` | optional | defaults to `/api/sensec/hsm/v1` (matches `HsmSparkConfig`'s own default) -- only set this if the target hsm-core-service's `hsm.service.api-v1-prefix` was itself overridden away from that default |
| `HSM_STATIC_TOKEN` | `HSM_AUTH_MODE=STATIC` | bearer token |
| `HSM_AZURE_TOKEN_SCOPE` | `HSM_AUTH_MODE=AZURE_AD` | e.g. `api://hsm-core-service/.default` -- must end in `/.default` |
| `HSM_SIGNING_KEY_PATH` | `HSM_AUTH_MODE=SELF_SIGNED_JWT` | file path to the **signing** private key PEM (matches `app_registrations.signing_public_key_pem`) -- a *different* key from `HSM_PRIVATE_KEY_PATH` unless you're on the legacy one-keypair fallback |
| `HSM_MTLS_CERT_PATH` | `HSM_AUTH_MODE=MTLS` | file path to the client certificate PEM |
| `HSM_MTLS_KEY_PATH` | `HSM_AUTH_MODE=MTLS` | file path to the matching private key PEM |

`HSM_PRIVATE_KEY_PATH` and `HSM_SIGNING_KEY_PATH` are **file paths**, not
inline PEM content -- write your key material to files first if you only
have it as a string (e.g. from a bulk-client job yml's inline
`private-key-pem: |` block).

### Option B: a conf file

Set **one** environment variable, `HSM_CONF_FILE`, pointing at a file in the
same `spark-defaults.conf` format `spark-submit --properties-file` (or
`$SPARK_HOME/conf/spark-defaults.conf`) uses -- `key value` or `key=value`
per line, `#` comments and blank lines ignored. Copy `hsm-spark.conf.example`
in this directory, fill in real values, keep only the block matching your
`spark.hsm.authMode`:

```
spark.sql.extensions com.hsm.spark.HsmUdfExtension
spark.hsm.baseUrl http://localhost:3005
spark.hsm.appId payments-svc
spark.hsm.authMode SELF_SIGNED_JWT
spark.hsm.privateKeyPath /path/to/dek-transport-key.pem
spark.hsm.signingKeyPath /path/to/signing-key.pem
```

Then:
```bash
export HSM_CONF_FILE=/path/to/your.conf
mvn compile exec:java
```

Every `spark.*` key in the file is applied directly, unmodified -- **the
same file works as-is with a real `spark-submit --properties-file` later**,
no translation needed. When `HSM_CONF_FILE` is set, the individual
`HSM_*` env vars from Option A are ignored entirely.

## Running

```bash
cd spark-verification-app

export HSM_BASE_URL="http://localhost:3005"
export HSM_APP_ID="payments-svc"
export HSM_AUTH_MODE="SELF_SIGNED_JWT"
export HSM_PRIVATE_KEY_PATH="/path/to/dek-transport-key.pem"
export HSM_SIGNING_KEY_PATH="/path/to/signing-key.pem"

mvn compile exec:java
```

## Running from IntelliJ IDEA

1. **File → Open** and pick this directory's `pom.xml` (not the `java/`
   reactor's) -- IntelliJ imports it as its own, separate Maven project.
   Copy the two required jars (see Setup above) into this directory
   *before* opening, or re-import the Maven project (the little "m" refresh
   icon in the Maven tool window) after copying them, so the `system`-scope
   dependencies resolve.
2. Open `LocalSparkSessionManualVerification.java`, click the green ▶ gutter
   icon next to `public static void main` and choose **Run**. This creates
   a run configuration named `LocalSparkSessionManualVerification` (it will
   fail the first time -- that's expected, no env vars are set yet).
3. **Run → Edit Configurations…**, select that configuration.
4. Find the **Environment variables** field. If it's not visible, click
   **Modify options** (top-right of the dialog) and enable "Environment
   variables" from that menu first.
5. Click the small expand icon at the right edge of the Environment
   variables field to open the multi-line editor. Add entries with **+**, or
   paste a block of `KEY=VALUE` lines directly (IntelliJ splits them
   automatically). Simplest: just one entry --
   ```
   HSM_CONF_FILE=/path/to/your.conf
   ```
   (see Option B above) -- or the five-or-so individual `HSM_*` variables
   from Option A if you'd rather not use a conf file.
6. **Apply**, then **Run** (▶) or **Debug** (🐞) as usual.

## What it does, and what success looks like

`LocalSparkSessionManualVerification.main()`:

1. Builds a real `SparkSession` (`local[*]`, `spark.sql.extensions=com.hsm.spark.HsmUdfExtension`)
   -- the exact same automatic-registration mechanism a real cluster
   deployment uses.
2. Runs `SELECT hsm_encrypt('hello from local verification', 'verification.column', NULL) AS ciphertext`
   and prints the result. `hsm_encrypt` takes **3 arguments**: `(plaintext,
   dek_name, data_classification)` -- `dek_name` must be a literal per
   column (see `HsmEncryptUdf`'s own javadoc), `data_classification` may be
   `NULL`.
3. Runs `SELECT hsm_decrypt('<the ciphertext above>') AS plaintext` --
   `hsm_decrypt` takes **1 argument**, the packed `v1...` token
   `hsm_encrypt` produced.
4. Asserts the round-tripped plaintext matches the original and throws if
   not.

Successful output ends with:
```
+---------------------------+
|plaintext                  |
+---------------------------+
|hello from local verification|
+---------------------------+

Round-trip verified: encrypt -> decrypt returned the original plaintext.
```

### Running your own queries

Once you've confirmed the round trip works, edit the `spark.sql(...)` calls
in `LocalSparkSessionManualVerification.java` directly to try other
queries -- e.g. against a real DataFrame instead of a single literal `SELECT`:

```java
Dataset<Row> df = spark.read().option("header", true).csv("/path/to/some.csv");
df.selectExpr("hsm_encrypt(ssn, 'customers.ssn', 'pii') AS ssn_ciphertext").show(false);
```

Or drop into a genuine interactive loop instead of editing the file each
time -- add `spark-shell`-style interactivity isn't wired up here (this is a
plain `main()`, not a REPL), so for exploratory querying, either keep
editing this file and re-running `mvn compile exec:java`, or point a real
`spark-shell`/`pyspark` session at the two jars above with the same
`spark.hsm.*` confs via `--jars`/`--conf` instead.

## Common errors

| Symptom | Cause |
|---|---|
| `NoClassDefFoundError: ...KQueueIoHandler` | You're resolving `spark-sql` through the `java/` reactor somehow (e.g. `mvn compile exec:java` run from inside `java/hsm-spark-adapter` instead of from here) -- confirm you're running `mvn compile exec:java` from *this* directory, with *this* `pom.xml`. |
| `FipsOperationError: Module checksum failed` | `bc-fips-2.1.1.jar` is missing from this directory (or `hsm.bc.fips.jar` points at the wrong file), or you're using a jar that still bundles `bc-fips` inside it -- rebuild `hsm-spark-adapter` from current source (the exclusion is already in `pom.xml`) and re-copy. |
| `WRONG_NUM_ARGS.WITHOUT_SUGGESTION ... hsm_encrypt requires 3 parameters` | `hsm_encrypt` takes `(plaintext, dek_name, data_classification)`, not just plaintext -- see above. |
| `Invalid audience` (401 from hsm-core-service) | `HSM_AUTH_MODE`-specific -- e.g. for `SELF_SIGNED_JWT`, confirm `hsm.jwt.audience` on the target core-service still includes the literal `hsm-core-service` default (or whatever `spark.hsm.selfSignedAudience` resolves to) if it's been widened to a multi-value list for `AZURE_AD`. |
| 404 on every call | `spark.hsm.apiV1Prefix` (`HSM_API_V1_PREFIX`) doesn't match the target hsm-core-service's actual `hsm.service.api-v1-prefix` -- the two are configured independently on each side, never auto-synced. |
