# hsm-spark-adapter: registering hsm_encrypt/hsm_decrypt as Spark SQL functions

`hsm-spark-adapter` registers `hsm_encrypt`/`hsm_decrypt` as Spark SQL
functions backed by `hsm-crypto-client`'s `HsmCryptoClient`, so a Spark job
can call them directly in SQL instead of driving `hsm-bulk-client`'s
config-file batch jobs. Companion to `AUTHORIZATION.md` (the two
authentication mechanisms `HsmCryptoClient` supports) and
`TIER3_POC_BUILD.md` (`hsm-crypto-client` itself).

## Status

Pinned to `spark-sql_2.13:4.2.0` (this module's `pom.xml`), matching the
target cluster's Spark 4.x line. **Actually run** -- not just compiled --
against a real local Spark 4.2.0 session under JDK 21: both
`HsmUdfRegistration.registerAll(spark)` and `spark.sql.extensions=
com.hsm.spark.HsmUdfExtension` register `hsm_encrypt`/`hsm_decrypt`
correctly, and `SELECT hsm_decrypt(...)` executes through real SQL parsing,
Catalyst analysis, and whole-stage codegen (not just interpreted eval) all
the way into `HsmDecryptUdf.call()`. Not yet run against a real *cluster*,
or with real `spark.hsm.*` config pointed at a live hsm-core-service --
`HsmCryptoClient` itself (the layer both UDFs delegate to) was already
separately live-verified end-to-end against a real hsm-core-service during
the `hsm-crypto-client` module's own build (see `TIER3_POC_BUILD.md`), so
the remaining gap is specifically "does a real cluster's classpath/config
delivery work," not "does the encrypt/decrypt logic work."

JDK note: this sandbox's only "modern" JDK was originally believed to be a
Homebrew JDK 26 build, which fails for a completely different reason than
expected -- not the Hadoop/`Subject.getSubject` issue that blocked Spark
3.5.1 (confirmed fixed in Hadoop 3.5.0, the version Spark 4.2.0 bundles),
but `spark-unsafe`'s `Platform.<clinit>` reflecting into
`jdk.internal.ref.Cleaner`, an internal class JDK 26 no longer has --
Spark 4.2.0 simply predates a JDK this new. Turned out a Homebrew
`openjdk@21` install already existed in this sandbox all along (missed by
an earlier search that only checked `/Library/Java/JavaVirtualMachines`,
not Homebrew's keg-only `/usr/local/Cellar` layout) -- Spark 4.2.0 runs
cleanly under it, matching Spark's own documented JDK 17/21 support range
(confirmed via the jar's own manifest: `Java-Version: 17`).

### Two real bugs the live run caught that `mvn compile` never could

Both were in `HsmUdfExtension` (the `spark.sql.extensions` automatic path)
-- `HsmUdfRegistration` (the stable public-API path) was correct from the
start and needed no fixes. Neither bug is a Spark-version difference; both
were wrong from this module's first commit, on every Spark version, and
were invisible until something actually ran the code:

1. **Wrong instantiation shape.** `HsmUdfExtension` originally took
   `SparkSessionExtensions` as a constructor argument and did registration
   inside the constructor body -- compiles fine, looks idiomatic, but
   `SparkSession`'s reflection code (`applyExtensions`, confirmed
   byte-for-byte identical between Spark 3.5.1 and 4.2.0 by disassembling
   both jars) calls `Class.getConstructor()` -- zero args -- then casts the
   result to `scala.Function1` and calls `.apply(extensions)` on it. A
   constructor that takes an argument is simply never found, and Spark
   silently skips the extension with a warning rather than failing loudly
   -- easy to miss without actually running it. Fixed by implementing
   `org.apache.spark.sql.SparkSessionExtensionsProvider` (a
   `Function1<SparkSessionExtensions, BoxedUnit>` marker interface Spark
   itself ships for exactly this purpose) with a no-arg constructor
   instead.
2. **Wrong `Column`-to-`Expression` conversion.** Converting the UDF
   invocation's result `Column` back to a Catalyst `Expression` via
   `ExpressionUtils.expression(...)` compiles fine and even looks
   symmetric with `ExpressionUtils.column(...)` (used for the argument
   direction), but produces an `Unevaluable ColumnNodeExpression` wrapper
   that throws `[INTERNAL_ERROR] Cannot generate code for expression` the
   instant Spark tries to codegen it. That method is meant for use inside
   Spark's own Column-processing pipeline, where a later Analyzer rule
   resolves the wrapper away -- code injected directly via
   `SparkSessionExtensions.injectFunction` runs after that rule already had
   its chance, so the wrapper never gets resolved and codegen fails. Fixed
   by calling `org.apache.spark.sql.classic.ColumnNodeToExpressionConverter`
   directly, which performs that resolution immediately.

Neither failure mode was visible from `javap`-comparing method signatures
across Spark versions -- both are runtime *reflection contract* and
*Analyzer timing* issues, categories no amount of signature-diffing catches.
The lesson generalizes: for `HsmUdfExtension` specifically, "compiles clean"
was never sufficient evidence of correctness, on any Spark version -- only
an actual run is.

### Spark 3.x vs 4.x: this module targets 4.x, and the two are not source-compatible

Spark 4.x's Classic/Connect API unification changed `Column` from a thin
wrapper directly around a Catalyst `Expression` to one built on a new
`ColumnNode` abstraction, which breaks `HsmUdfExtension`'s bridging code
across the 3.x/4.x line in two concrete ways -- found by `javap`-inspecting
a real `spark-sql_2.13:4.2.0` jar (the same rigor used for the initial 3.5.1
build) after the target cluster turned out to run 4.x, not 3.5.1 as first
assumed:

- **No `spark-sql_2.12` build exists for Spark 4.x at all** -- Maven Central
  publishes only `_2.13` artifacts from 4.0.0 onward, so a 4.x cluster needs
  `scala.binary.version=2.13` (this module's current pin), not just a
  `spark.version` bump.
- **`Column(Expression)` no longer exists.** Spark 4.x's only `Column`
  constructors are `Column(ColumnNode)`, `Column(String)`, and
  `Column(String, Option<Object>)`. The fix: Spark ships the exact bridge
  needed in `org.apache.spark.sql.classic.ExpressionUtils` --
  `column(Expression): Column` and `expression(Column): Expression` --
  which `HsmUdfExtension` now uses in place of the old `new Column(expr)` /
  `.expr()` calls. Same internal-API risk category as `injectFunction`
  itself, just a different class.
- **`SparkSessionExtensions.injectFunction`'s `Seq` parameter type changed**
  from `scala.collection.Seq` (3.5.1) to `scala.collection.immutable.Seq`
  (4.2.0) -- a real, distinct type under Scala 2.13's collections redesign,
  not a cosmetic rename. `HsmUdfExtension`'s `Function1` builder imports the
  immutable variant accordingly.
- **Unaffected**: `ExpressionInfo`'s constructors, `functions.udf(UDF1/UDF3,
  DataType)`, `UserDefinedFunction.apply(Column...)`, and -- notably -- all
  of `UDFRegistration`'s `register(...)` overloads that
  `HsmUdfRegistration` relies on. If your cluster's exact Spark 4.x point
  release turns out to differ from 4.2.0 in ways that matter, prefer
  falling back to `HsmUdfRegistration` (the stable-API, per-application
  path) over re-deriving the `classic.ExpressionUtils` bridge -- it carries
  none of this internal-API exposure.

## Why two registration functions, not one big design

- **`HsmUdfRegistration.registerAll(SparkSession)`** -- explicit, per-application,
  built entirely on Spark's stable public `UDFRegistration` API
  (`spark.udf().register(...)`). Zero Catalyst-internals risk; works on any
  reasonably modern Spark version without re-verification. Call it once per
  application, right after creating the `SparkSession`.
- **`HsmUdfExtension`** -- automatic for every application on a cluster
  configured with `spark.sql.extensions=com.hsm.spark.HsmUdfExtension`. No
  per-application code at all, but reaches into
  `SparkSessionExtensions.injectFunction`, `ExpressionInfo`'s constructor
  shape, and (Spark 4.x) `org.apache.spark.sql.classic.ExpressionUtils` --
  Catalyst-internal APIs that are not guaranteed stable across Spark
  releases the way `UDF1`/`UDF3` are, and are not even source-compatible
  across the Spark 3.x/4.x line (see "Spark 3.x vs 4.x" above). Re-verify
  (`javap` the target cluster's actual `spark-sql` jar, or just try a
  compile against it) if the cluster runs a materially different Spark
  version than this module's `spark.version`/`scala.binary.version` pin.

Both register the exact same `HsmEncryptUdf`/`HsmDecryptUdf` classes --
the two paths differ only in *when and how* registration happens, never in
the functions' own behavior.

## Function signatures

```sql
hsm_encrypt(plaintext STRING, dek_name STRING, data_classification STRING) -> STRING
hsm_decrypt(ciphertext STRING) -> STRING
```

`dek_name` is a **per-call SQL argument**, not baked into the function --
one registration serves every column, each column supplies its own
`dek_name` literal:

```sql
SELECT
  hsm_encrypt(ssn, 'customers.ssn', 'pii')                AS ssn_encrypted,
  hsm_encrypt(account_number, 'customers.account', 'pii') AS account_encrypted,
  hsm_encrypt(email, 'customers.email', NULL)               AS email_encrypted
FROM customers;

SELECT
  hsm_decrypt(ssn_encrypted)     AS ssn,
  hsm_decrypt(account_encrypted) AS account_number
FROM customers_encrypted;
```

`hsm_decrypt` needs no `dek_name` at all -- the packed token already carries
`edek_id`, so which DEK to use is self-describing, the same reason
`HsmCryptoClient.decrypt(String)` itself takes one argument.

**`dek_name` must be a literal constant per column, not a per-row
expression.** A value that varies per row defeats the DEK-reuse cache
entirely, degrading straight back to a fresh `/dek/issue` call per row --
see "Capacity planning" below for why that matters at Spark's typical row
counts.

## Client lifecycle

One `HsmCryptoClient` per **executor JVM**, built lazily on that executor's
first UDF invocation -- not at executor boot, and not one per UDF call.
`HsmEncryptUdf` and `HsmDecryptUdf` share the same lazily-built instance
(`HsmCryptoClientHolder`), so registering both in one application doesn't
open two separate connections/caches. The client -- and its `dek_name`
cache -- stays warm for the whole application's lifetime, reused across
every job (every `SELECT`/action) in that application, not just within one
query.

Never explicitly closed: Spark UDFs have no clean per-application shutdown
hook a simple UDF can reliably attach cleanup to. The executor JVM's own
teardown reclaims it -- deliberate, not an oversight.

## Identity model: one `appId` per application

`spark.hsm.appId` (and the rest of the connection config) is read once per
executor, from **that application's own** Spark conf -- not baked into
`HsmUdfExtension` at construction time. That's what lets registration stay
cluster-wide (every application gets the functions automatically) while
identity stays per-application: each application still sets its own
`--conf spark.hsm.appId=...` at submit time, and every job within that one
application shares that one identity -- the same convention
`hsm-bulk-client`'s own single-`app-id`-per-run config already uses. If an
application genuinely needs to act as more than one identity, that's a
materially different design (a per-executor map of clients keyed by
`appId`, `appId` becoming a required UDF argument, and a config *mapping*
of several `(appId -> key path)` pairs instead of one) -- not built here;
submit a separate application per identity instead.

## Configuration

Two categories, delivered two different ways -- **do not** mix them:

| Config | Spark conf key | Delivery |
|---|---|---|
| hsm-core-service base URL | `spark.hsm.baseUrl` | plain `--conf`, non-secret |
| API path prefix (default `/api/sensec/hsm/v1`) | `spark.hsm.apiV1Prefix` | plain `--conf`, non-secret |
| App identity | `spark.hsm.appId` | plain `--conf`, non-secret |
| Auth mode: `STATIC` \| `AZURE_AD` \| `SELF_SIGNED_JWT` | `spark.hsm.authMode` | plain `--conf`, non-secret |
| Static bearer token (authMode=STATIC) | `spark.hsm.staticToken` | plain `--conf` -- fine for demo tokens only; a real static value here is itself credential-shaped |
| Azure AD token scope (authMode=AZURE_AD) | `spark.hsm.azureTokenScope` | plain `--conf`, non-secret |
| Self-signed JWT audience (authMode=SELF_SIGNED_JWT) | `spark.hsm.selfSignedAudience` | plain `--conf`, non-secret |
| **DEK-transport private key** (always required) | `spark.hsm.privateKeyPath` | **file path**, Secret-mounted identically on every executor |
| **Signing private key** (authMode=SELF_SIGNED_JWT only) | `spark.hsm.signingKeyPath` | **file path**, Secret-mounted identically on every executor |

The two key paths are read from local disk at lazy-init time -- they never
travel through Spark's own config propagation or UDF closure serialization.
Putting raw key PEM into a `--conf` value (or a UDF constructor field) would
put it on that path instead, which is exactly what this design avoids: see
`AUTHORIZATION.md`'s mTLS section for the same class of concern in a
different mechanism.

## Deploying into an existing cluster

`mvn package` produces one self-contained, shaded jar (`maven-shade-plugin`,
~37MB -- most of that size is `netty-tcnative-boringssl-static`'s per-platform
native binaries, pulled in transitively for every OS/architecture, not this
module's own code) with `hsm-crypto-client` and its own dependencies
(`bc-fips`, `nimbus-jose-jwt`, `jackson-databind`, `azure-identity`) bundled
in; everything `spark-sql`-scoped stays out since that dependency is
`provided`. Netty is relocated inside the shaded jar
(`com.hsm.spark.shaded.io.netty`) to avoid colliding with Spark's own
runtime Netty -- a real collision, not a hypothetical one: azure-identity
pulls Netty 4.1.x while this module's Spark 4.2.0 pin needs 4.2.x, and
mixing both unrelocated on one classpath broke `SparkContext`'s own RPC
layer at startup with a `NoClassDefFoundError` (`KQueueIoHandler` not
found) during this module's own live-verification run -- confirmed via a
real local Spark session. Re-verified with the relocation in place: the
shaded jar alone (no other dependency jars needed alongside it) registers
and executes `hsm_encrypt`/`hsm_decrypt` correctly against a real Spark
4.2.0 session. That one jar needs to reach every node's classpath, and
`spark.sql.extensions` (if using the automatic path) needs to be set
cluster-wide. Mechanics differ by platform:

- **Standalone / vanilla YARN:** `spark.jars=/path/to/hsm-spark-adapter.jar`
  and `spark.sql.extensions=com.hsm.spark.HsmUdfExtension` in
  `spark-defaults.conf` on every node, or per-submission via
  `spark-submit --jars ... --conf spark.sql.extensions=...` if you'd rather
  opt in per job (skips `HsmUdfExtension` entirely -- just call
  `HsmUdfRegistration.registerAll(spark)` in the job instead).
- **Spark on Kubernetes (raw `spark-submit --master k8s://...` or the
  Kubernetes Spark Operator):** bake the JAR into the driver/executor
  container image, or reference it via `spark.jars`/the Operator's
  `SparkApplication` CRD `spec.deps.jars`; set `spark.sql.extensions` in the
  same CRD's `sparkConf`. Deliver `spark.hsm.privateKeyPath` (and
  `signingKeyPath`) via a Kubernetes Secret volume mounted at the same path
  on every executor pod.
- **Databricks / EMR / other managed platforms:** use the platform's own
  cluster-scoped library/init-script mechanism to install the JAR, and its
  cluster-config UI (or bootstrap action, for EMR) to set
  `spark.sql.extensions`. Use the platform's own secret-scope/secret-manager
  integration for the key files, not a plain `--conf`.

## Capacity planning

Because the DEK cache is per-executor, not per-application: encrypting one
column across `N` executors costs `O(N)` `/dek/issue` calls total for that
`dek_name`, not `O(1)` -- still a massive reduction from `O(rows)` (what an
unnamed `hsm_encrypt(plaintext)` call, with no `dek_name`, would cost), but
not quite as tight as a single-process batch job's `O(1)` per name. If
several separate applications run concurrently against the same cluster,
multiply again by the number of concurrently-running applications. Size
hsm-core-service's expected request volume with that multiplier in mind,
not just one application's executor count.
