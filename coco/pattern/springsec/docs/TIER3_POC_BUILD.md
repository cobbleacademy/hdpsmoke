# Tier 3 Bulk PoC: what was built and measured

Companion to `BULK_OPERATIONS.md`'s Tier 3 design section -- this doc records what
was actually built for the scoped PoC round (Phase 1 + Phase 2 + a benchmark) and
the real numbers from running it, not just the design.

## What's built

- **New module**: `java/hsm-bulk-service` -- `POST /dek/issue`, `POST /dek/unwrap`.
  Scaffolded the same way `cek-rotation-service` was (no shared library module in
  this repo, so the handful of stateless crypto/security/auth classes it needs are
  duplicated from `hsm-core-service`, not imported).
- **New migration**: `V6__add_public_key_to_app_registrations.sql` -- adds
  `public_key_pem` to `app_registrations` (nullable, no backfill).
- **New**: `com.hsm.bulk.crypto.TransportWrapper` -- RSA-OAEP-256 wrap/unwrap of a
  raw DEK for transport to the calling app (CLNT), via BC-FIPS `Cipher.WRAP_MODE`/
  `UNWRAP_MODE` (BC-FIPS approved-mode rejects plain `ENCRYPT_MODE`/`DECRYPT_MODE`
  for RSA key-wrapping -- a real gotcha hit and fixed during this build).
- **Tests**: `DekIssueServiceTest`, `DekUnwrapServiceTest` -- persisted `EdekRecord`
  shape, transport-wrap round-trip, and the same owner/grant enforcement
  `DecryptionService.decrypt` uses (owner app / explicit `AppDecryptGrant` /
  `governance` authority).
- **Benchmark**: `com.hsm.bulk.bench.BulkVsBatchBenchmark` (runnable `main()`, not
  a JUnit test) -- plays CLNT locally: calls `/dek/issue`, RSA-unwraps the
  transport-wrapped DEK, runs `DekManager.encrypt` locally, and times that against
  calling `hsm-core-service`'s `/encrypt/batch` for the same plaintexts.
- **New module**: `java/hsm-bulk-client` -- the real, standalone client (not a
  benchmark script) that drives SVC (`hsm-bulk-service`) against actual data.
  Genuinely two separate deployables, not merged: this module never receives
  inbound requests, so it carries none of SVC's server-side machinery (no
  `JwtAppIdAuthenticationFilter`, no Spring Security, no JPA) -- just an HTTP
  client (`SvcClient`, same `java.net.http.HttpClient` pattern the benchmark
  proved) plus the same duplicated `DekManager`/`IvFactory`/`TransportWrapper`
  trio, now under `com.hsm.client.crypto`. Two jobs:
  - **BULK DB** (`DbBulkJob`) -- config-driven source/target table + column
    mapping (independently addressable JDBC URL/schema/table per side), one DEK
    per *column value* (not per row) so a compromised DEK only ever exposes one
    field, keyset-paginated (not `OFFSET`) reads, plain generic JDBC (no
    vendor-specific SQL). `encrypt()`/`decrypt()` share the same config shape --
    direction just flips which side is ciphertext vs. plaintext. `columns` only
    ever covers the sensitive columns being encrypted/decrypted -- any other
    target-table column (non-sensitive business data you still want copied
    over) needs an explicit `passthrough-columns: [name, email, ...]` entry;
    nothing outside `key-column` + `columns` + `passthrough-columns` is ever
    read from source or written to target. Deliberately explicit rather than
    "copy every source column not in `columns`" -- auto-discovery would
    silently copy a newly-added sensitive column into a "secure" target table
    in plaintext if someone forgot to add it to `columns`.

    `DekManager.decrypt()` always hands back the plaintext as raw UTF-8 bytes
    -- there's no way to recover a sensitive column's original SQL type from
    the ciphertext alone. On a DECRYPT job, if a `columns` entry's target is
    anything other than `VARCHAR`/`TEXT` (e.g. a `DOB` column typed `DATE`),
    set `target-type` on that mapping: `DATE`, `TIMESTAMP`, `NUMERIC`
    (DECIMAL/NUMERIC-family), or `INTEGER` (INT/BIGINT/SMALLINT-family).
    Omitted/`STRING` (default) matches the pre-existing behavior and is
    correct for `VARCHAR`/`TEXT` targets. `target-type` is ignored on ENCRYPT
    jobs -- the target there is always the `ciphertext_token` `VARCHAR`/`TEXT`
    column, regardless of the source column's type. `Date.valueOf`/
    `Timestamp.valueOf` are used directly (not via `LocalDate`/
    `LocalDateTime.parse`) because they accept exactly the format
    `java.sql.Date`/`Timestamp.toString()` produce -- what ENCRYPT already
    stringified the original value with -- so this is a true round trip.
  - **BULK File** (`FileBulkJob`) -- config-driven source/target root, each
    independently local disk or ADLS (`FileStore` interface, `LocalFileStore`/
    `AdlsFileStore` -- mixed pairs like ADLS source to local target fall out of
    the interface for free, not a special case). One DEK per whole file; each
    chunk still gets its own `DekManager.encrypt` call (a fresh random IV every
    time via `IvFactory`, so no new crypto design was needed for "one DEK, many
    chunks"). Chunks are stitched into a single output file via **length-prefixed
    binary framing** (`[edek_id][chunk length][iv+tag+ciphertext]` repeated to
    EOF) -- deliberately not newline-delimited, since raw AEAD ciphertext can
    contain a literal `0x0A` byte and would corrupt that framing.

## Deployment

`hsm-bulk-service` (SVC) now has real Docker/Helm artifacts:
`java/docker/Dockerfile.hsm-bulk-service` (same distroless multi-stage pattern
as `hsm-core-service`) and `helm/hsm-bulk-service/` (same chart structure --
Deployment, Service, ConfigMap, Secret, ServiceAccount/Workload Identity, HPA,
PDB, NetworkPolicy, corporate-CA initContainer). Two real differences from
`hsm-core-service`'s chart: `replicaCount: 1` and `podDisruptionBudget.enabled:
false` by default (this is a scoped, onboarding/de-boarding-window service, not
always-on shared traffic -- a `minAvailable: 1` PDB on a single replica would
block all voluntary node drains, an anti-pattern); and `probes.type: tcp` by
default, since SVC has no health endpoint yet (only authenticated
`POST /dek/issue`/`POST /dek/unwrap`) -- a TCP check still catches a dead/hung
process without needing an unauthenticated route added just for probing.

Adding `hsm-bulk-service`/`hsm-bulk-client` to the parent POM's `<modules>`
broke the *existing* `Dockerfile.hsm-core-service`/`Dockerfile.cek-rotation`
builds -- Maven's reactor needs every module's `pom.xml` physically present in
the build context to resolve the parent, even with `-pl X -am` limiting what
actually gets compiled. Fixed by adding `COPY` lines for the two new modules'
`pom.xml` (not their `src`) to both existing Dockerfiles.

`hsm-bulk-client` stays a plain runnable jar by design -- no Docker/K8s for it.
`java/hsm-bulk-client/scripts/build.sh` and `scripts/run.sh <config.yml>`
wrap the exact commands used throughout this doc's verification runs.
`config-examples/{db,file}-{encrypt,decrypt}-example.yml` are the real
verified job shapes with credentials/keys redacted.

**A real, unrelated security fix surfaced along the way**: `hsm-core-service`
historically expected credentials embedded directly in `DATABASE_URL`
(`postgresql://user:pass@host/db`) -- but Hikari and Hibernate both print the
JDBC URL at startup unconditionally (`"Database JDBC URL [...]"`,
`"HikariPool-1 - Added connection ... url=..."`), which ships to Splunk via
this service's own log pipeline. An embedded password would land in plaintext
in every pod's logs. Fixed by adding separate `DATABASE_USERNAME`/
`DATABASE_PASSWORD` properties (matching `hsm-bulk-service`'s -- and
`application-demo.yml`'s -- existing pattern), so `DATABASE_URL` never needs
credentials embedded in it. Verified: `hsm-core-service` still starts and
passes its health check with the new fields; `55/55` tests still pass.

## Deliberately out of scope this round

Admin key-provisioning endpoint (Phase 3), `hsm-bulk-client` audit-completion
callback, and a real health endpoint on `hsm-bulk-service` -- see
`BULK_OPERATIONS.md` for the first. Key registration for this PoC is a direct
DB write (or a small setup script), not a built admin feature.

## Running it

Both services run locally against `MockKekClient` (no real Azure Key Vault) and
share the same H2 file via `AUTO_SERVER=TRUE` (H2 file mode is normally
single-JVM-exclusive). See `BulkVsBatchBenchmark`'s class Javadoc for the exact
commands. One real gotcha hit and documented there: H2 rejects
`AUTO_SERVER=TRUE` combined with `DB_CLOSE_ON_EXIT=FALSE` ("Feature not
supported") -- drop the latter when sharing a file this way.

`MockKekClient` in `hsm-bulk-service` deliberately uses the *same* hardcoded key
bytes and version string (`demo-v1`) as `hsm-core-service`'s own `MockKekClient`
-- there's no real Key Vault for two independent mock instances to actually
share, so this is the only way a `/dek/issue`-derived EDEK can be unwrapped by
`hsm-core-service`'s real `/decrypt` in this PoC. Against a real Key Vault
(`demo-mode: false`, `skip-akv: false`), both services' `AzureKeyVaultKekClient`
genuinely share the same HSM-backed key and this alignment isn't needed.

### `demo-mode` vs `skip-akv`

`hsm.demo-mode` (`DEMO_MODE`) swaps **both** the `KekClient` and the
`JwtValidator` to their mock forms together -- fine for `BulkVsBatchBenchmark`,
but it means there's no way to test real JWT/scope validation
(`app_registrations.allowed_scopes`) without also requiring a reachable Key
Vault/Managed HSM. `hsm.skip-akv` (`SKIP_AKV`) is a second, independent lever
that only ever affects the `KekClient` -- the exact same `demo-mode`/`skip-akv`
relationship and flag names `hsm-core-service` already has (`HsmProperties`,
`CryptoBeansConfig`). Set `DEMO_MODE=false` + `SKIP_AKV=true` to exercise real
`RsaJwtValidator` + scope enforcement against a real `app_registrations` row
while still using `MockKekClient` for the DEK wrap/unwrap itself:

```bash
DEMO_MODE=false SKIP_AKV=true ...   # real JWTs, real scopes, no Key Vault call
```

(This flag was originally named `mock-kek`/`MOCK_KEK` -- renamed to
`demo-mode`/`DEMO_MODE` to match `hsm-core-service` exactly, since the old name
implied it only scoped to the KEK client when it also governs JWT validation.)

With `DEMO_MODE=true` (default), a caller must use one of `MockJwtValidator`'s
three fixed demo tokens (`demo-token-payments-svc`, `demo-token-reporting-app`,
`demo-token-ops-admin`), and `svc.app-id` in the caller's config must match the
token's baked-in `app_id` claim exactly. Either way, `dek_issue`/`dek_unwrap`
authorities still come from `app_registrations.allowed_scopes` in the DB, not
from the token -- `AppRegistryService` caches scopes in-memory with no
eviction, so a scope grant added via direct SQL needs a service restart to
take effect.

## DEK naming & reuse

A later round on top of the Phase 1/2 PoC above: `dek_name` lets a caller ask
for the *same* DEK across many calls -- one DEK per logical column
(`"customers.ssn"`) instead of one per value -- cutting real HSM/Key-Vault
operations from O(rows) to O(distinct names), which is what was actually
driving the benchmark's RSA-overhead cost above.

- **Schema** (`V7__add_dek_name_to_edek_records.sql`): `edek_records` gains
  `dek_name` (permanent, kept through rotation for history) and
  `current_dek_name` (nulled out on rotation) -- a *shadow-column* pair, not a
  Postgres partial unique index (`CREATE UNIQUE INDEX ... WHERE ...`), because
  H2 (demo mode) rejects that syntax outright (verified directly against H2
  2.4.240). A plain unique index on `(app_id, current_dek_name)` gets the same
  effect on both databases, since ANSI SQL never treats one `NULL` as equal to
  another in a unique index.
- **Reuse logic**, mirrored in both `EncryptionService.resolveDek()`
  (`hsm-core-service`, `/encrypt` + `/encrypt/batch`) and
  `DekIssueService.issueOne()` (`hsm-bulk-service`, `/dek/issue`): a request
  with `dek_name`/`name` set looks up `(app_id, current_dek_name)`; a hit
  reuses that DEK (unwrap + reuse, `EncryptResponse.reused`/
  `DekIssueResultItem.reused` reports which happened); a miss mints fresh and
  tags the new row with the name. `hsm-core-service` also reads/primes
  `DekCache` on the reuse path -- the same cache `DecryptionService` already
  used, now warmed by encrypt too. `hsm-bulk-service` has no `DekCache` (this
  module was deliberately scoped down without one), so a reuse there still
  costs one real KEK/HSM unwrap -- the win is fewer *issued* DEKs and fewer
  `EdekRecord` rows, not fewer HSM calls per lookup.
- **Fingerprint**: left `null` on named rows. The existing per-token
  fingerprint cross-check in `DecryptionService.decrypt()` assumes one
  `edek_id` has exactly one `iv`/`tag` pair ever -- true under DEK-per-record,
  false under reuse (many different `iv`/`tag` pairs legitimately share one
  `edek_id`). The check already gates on non-null, so this needed no change on
  the decrypt side at all.
- **Classification immutability**: one `dek_name` is bound to exactly one
  `data_classification`. Enforcement is three-way, not a strict equality
  check: an explicit non-blank conflict is rejected (`422`); a blank
  incoming value on an already-classified name is a no-op (informational
  field stays as-is); a blank *existing* value backfills from a later
  non-blank call (nothing to conflict with yet).
- **A real bug this surfaced**: sending `dek_name: ""` (empty string, not
  omitted) stored `""` in `current_dek_name`, not `null` -- and since the
  unique index treats `""` as a real value, the *second* unnamed `/encrypt`
  call from the same app collided and 500'd. Fixed by normalizing
  blank-to-`null` in both `EncryptRequest`'s and `DekIssueItem`'s compact
  constructors, so it's impossible to construct either DTO with a blank (as
  opposed to genuinely absent) name.
- **Rotation** (`hsm-core-service`): `NamedDekRotationScheduler` mirrors
  `KekRotationScheduler`'s exact shape -- same `@PostConstruct` + `CronTrigger`
  pattern, same demo-mode guard. `RotationService.rotateNamedDeks(maxAgeHours)`
  sweeps `current`-status rows with a non-null `current_dek_name` older than
  the threshold (default 720h / 30 days, `hsm.named-dek-rotation.*`), mints a
  fresh DEK per row, and retires the old one (`rotation_status=rotated`,
  `current_dek_name` cleared, `dek_name` kept for history). Deliberately
  **time-based, not usage-count-based**: `hsm-core-service` has no visibility
  into how many individual values a caller actually encrypts with a DEK it
  handed out (that happens entirely client-side), so a lookup counter could
  never be a trustworthy usage measure -- bounding the *age* a name stays
  current sidesteps that gap instead of trying to solve it. One real ordering
  gotcha: the old row's `UPDATE` (clearing `current_dek_name`) must be
  `saveAndFlush`'d *before* the new row's `INSERT`, since Hibernate's default
  flush order is by operation type (inserts before updates), not registration
  order -- inserting the fresh row first would transiently violate the unique
  index within the same transaction.
- **`hsm-bulk-client` DEK-name config**: `ClientProperties.Db.ColumnMapping`
  gained `dekName` -- set on a column's `columns` entry and `DbBulkJob.encrypt()`
  resolves that name's DEK **once for the whole job run** (not per row, not
  even per sub-batch) via a new `issueNamedColumnDeks()` step before the
  pagination loop starts. `decrypt()` separately dedups by `edek_id` (not by
  the existing per-row correlation key) before calling `/dek/unwrap` *and*
  before the local RSA-OAEP unwrap -- the RSA step, not the HTTP round trip,
  was the dominant cost in the benchmark above, so deduping only the network
  call would have missed the bigger win.
- **Bearer-token expiry on long-running jobs**: `SvcClient` previously sent a
  single static config token for the whole job -- fine for demo-mode's
  never-expiring fixed strings, but a real Azure AD JWT (~1h TTL) would expire
  mid-run on anything longer than that. Replaced with a `TokenProvider`
  abstraction: `StaticTokenProvider` (today's behavior, unchanged, selected by
  default `svc.auth-mode: STATIC`) or `AzureAdTokenProvider`
  (`svc.auth-mode: AZURE_AD`), which calls `TokenCredential.getToken()` fresh
  before *every* request. That call is a cache hit on nearly every request
  (Azure Identity SDK caches internally and only re-acquires when the cached
  token is near expiry) -- no manual TTL tracking needed. Same Workload
  Identity cascade (`WorkloadIdentityCredential` → `ManagedIdentityCredential`
  → `DefaultAzureCredential`) already used by `AzureKeyVaultKekClient` and
  `AdlsFileStore`, kept as its own copy per this repo's no-shared-library
  convention. No client secret anywhere in this chain.

## Performance, resume, and File-job parity (later round)

Prompted by a real report: a 2M-row, 4-column `db encrypt` job took 4 hours.
Diagnosis (not guesswork -- walked through the actual math against the reported
config) found the DEK-issuance path was already using `dek-name` correctly, so
the real cost was elsewhere: `DriverManagerDataSource` (unpooled -- every JDBC
operation pays full connection-setup cost) and `subChunkByItemCap` artificially
capping insert batches to `dek-batch-max-items` (100) even when there was no
`/dek/issue` HTTP call left to cap for a fully-named run. Both fixed; a third
lever (parallelism) and a fourth (crash/resume) came out of the same
conversation, then ported to the File job for parity, since the same "one
4-hour job, one crash" problem applies to a large file tree just as much as a
large table.

- **Connection pooling + batch-insert rewrite** (`DbBulkJob`): `HikariDataSource`
  replaces `DriverManagerDataSource`, sized to `parallelism + 2` so every
  concurrent worker (see below) gets its own connection without starving.
  `reWriteBatchedInserts=true` auto-appended to Postgres JDBC URLs -- without it,
  the driver's batch protocol is still N separate bind/execute messages per
  flush, not a true multi-row `INSERT ... VALUES (...),(...),(...)`.
- **`subChunkByItemCap` fix**: `columnsPerRow=0` (every column in a batch is
  named, so no `/dek/issue` call happens for it at all) now returns the whole
  page as one chunk instead of splitting it down to `dek-batch-max-items` rows
  for no reason -- there's no HTTP item cap to respect when no HTTP call is
  being made.
- **`client.db.parallelism`** (default `1`, unchanged sequential behavior):
  `>1` partitions the key range into that many pieces up front -- a bounded,
  *one-time* set of `ORDER BY key_column OFFSET x LIMIT 1` boundary lookups
  (`parallelism - 1` of them total), fundamentally different from using
  `OFFSET` as the *per-page* pagination mechanism, which this design still
  avoids. Each partition runs the identical fetch-issue-encrypt-insert pipeline
  concurrently, on its own key range.
- **`client.db.checkpoint`** (`enabled`/`resume`/`job-id`/`table-name`, default
  `enabled: false` -- zero overhead unless opted in): a batch's data insert and
  its checkpoint update commit in **one transaction**
  (`TransactionTemplate`/`DataSourceTransactionManager` over the target
  `DataSource`), so a resumed run can never skip rows that were never durably
  written, nor re-process rows that were. Deliberately *not* built on top of
  the earlier key-uniqueness discussion (an `ON CONFLICT` upsert on the actual
  target table would need that) -- the checkpoint table owns its own
  guaranteed-unique `job_id` key and is upserted independently, so resume works
  correctly without requiring the source/target table's own key to be
  provably unique. Under `parallelism > 1`, each partition gets its own
  derived checkpoint (`job-id-p0`, `job-id-p1`, ...), so a parallel run resumes
  per-partition, not as one unit.
- **`hsm-bulk-service` gains its own `NamedDekRotationScheduler`**: closes a
  real, pre-existing gap -- DEKs issued via `POST /dek/issue` with a name
  (`DbBulkJob`'s per-column `dek-name`, and the new File-job `dek-name` below)
  had **no rotation at all** until now, unlike `hsm-core-service`'s own
  `/encrypt` `dek_name` path. Mirrors `hsm-core-service`'s scheduler and
  `RotationService.rotateNamedDek()` exactly, including the same
  `saveAndFlush`-before-`save` ordering fix for the same Hibernate
  flush-order reason. New `HsmBulkProperties.NamedDekRotation`
  (`hsm.named-dek-rotation.*`, same defaults: `enabled: true`, `720` max-age
  hours, daily `03:00` cron).
- **`client.file.dek-name`** (`FileBulkJob`): mirrors `DbBulkJob`'s per-column
  `dek-name`, but scoped to the *whole job*, not a column -- because a file
  job has no per-column concept, and (per an explicit discussion) the natural
  reuse boundary for a file tree is "one business purpose, one DEK," matching
  how source trees are actually organized in practice (all-customer-files,
  all-product-files, etc., as separate jobs). Unlike `DbBulkJob`'s per-run-only
  scoping, this name is meant to persist **across every future run** of that
  job, which is exactly why `hsm-bulk-service`'s new rotation scheduler above
  had to exist first -- an unrotated, indefinitely-reused DEK would otherwise
  protect an unbounded amount of data across a job's entire lifetime.
- **`client.file.parallelism`**: simpler than the DB case -- files are
  independent, unordered work items, so partitioning is just an even,
  contiguous split of the file list into that many groups, no boundary-key
  math needed at all.
- **`client.file.checkpoint`**: same `enabled`/`resume`/`job-id` shape as the
  DB side, but backed by a single batched **manifest file** in the target
  `FileStore` (a newline-delimited list of completed relative paths) instead
  of a database table -- deliberately not a marker-per-file (would double
  write/blob-create operations, a real added cost on ADLS specifically, which
  bills and rate-limits per operation) and not a new external DB dependency
  (keeps this job storage-agnostic, same as everything else in this module).
  `FileStore` has no append primitive, so each flush rewrites the manifest in
  full; `checkpoint.flush-interval` (default `1000`) controls how many
  completions happen between flushes, and is deliberately its own setting, not
  tied to `files-per-batch` (an unrelated HTTP-batching-size knob) -- see the
  measured regression below for why that coupling mattered.
- **`FileBulkJob.decryptSlice` dedup fix**: found while verifying the
  `dek-name` addition above -- decrypt wasn't deduplicating by `edek_id` the
  way `DbBulkJob`'s decrypt already does, so a job where many files share one
  named DEK would have made one redundant `/dek/unwrap` call (and one
  redundant RSA-OAEP unwrap) per file instead of per distinct id. Fixed to
  dedupe per-batch, mirroring `DbBulkJob` exactly.

## Verified

1. `mvn -pl hsm-bulk-service -am test` -- 9/9 passing (7 original + 2 new:
   named-DEK reuse shares `edek_id`, classification conflict rejected).
2. `mvn -pl hsm-core-service -am test` -- 59/59 passing (54 original + 5 new:
   `NamedDekIntegrationTest` -- reuse, independent mints, classification
   conflict/backfill, and a full rotation round trip via `RotationService`
   directly).
3. `mvn clean install` across the full reactor (`hsm-core-service`,
   `cek-rotation-service`, `hsm-bulk-service`, `hsm-bulk-client`) -- exit 0,
   all 4 module jars built, 69/69 tests passing.
4. **Token-format compatibility** (the hard requirement): a DEK issued via
   `/dek/issue`, unwrapped and used locally to build a `ciphertext_token` via
   `DekManager.packToken`, decrypted correctly through `hsm-core-service`'s real,
   unmodified `/decrypt` endpoint. Confirmed live, not just by test.
5. **Benchmark, live run, 200 records, both services local/mocked**:

   | Path | Total time | Throughput |
   |---|---|---|
   | Batch (`/encrypt/batch`) | 658 ms | 304.0 items/sec |
   | Bulk (`/dek/issue` + local AES-GCM) | 1373 ms | 145.7 items/sec |

   Bulk was **slower** in this run, not faster -- likely dominated by RSA-2048
   OAEP unwrap per item on the CLNT side (asymmetric crypto is meaningfully more
   expensive per-op than the AES-GCM it's protecting), run single-threaded in the
   same process as the benchmark's HTTP client. This doesn't contradict Tier 3's
   actual motivating claim (reducing `hsm-core-service`'s own shared-process
   footprint under concurrent multi-tenant load, not raw single-caller
   throughput) -- but it does mean the "faster for bulk" intuition doesn't hold
   without qualification, and RSA transport-wrap cost is a real, non-trivial
   overhead specific to this design that a production pilot needs to size
   (batching the RSA operations, or evaluating ECIES's cheaper decrypt path
   instead of RSA-OAEP, are the two obvious levers).
6. **`hsm-bulk-client`, live run, both jobs, against real `hsm-core-service` +
   `hsm-bulk-service`** (shared H2 file, `demo-mode: true`):
   - **BULK DB**: seeded a 3-row `customers` table (`ssn`, `account_number`),
     ran `db encrypt` -- `customers_encrypted`'s two `ciphertext_token` columns
     populated correctly (6 separate DEKs issued, one per column value). Spot
     checked one row's tokens directly through `hsm-core-service`'s real
     `/decrypt`: `111-22-3333` and `ACCT-0001` came back exactly. Ran
     `db decrypt` back into a third table and diffed all 3 rows against the
     original -- byte-for-byte identical.
   - **BULK File**: a nested `level1/level2/` tree with a 15 KB text file and a
     500 KB binary file, 64 KiB chunk size (so the binary file spans 8 chunks).
     `file encrypt` reproduced the exact source structure/filenames under the
     target root; output sizes matched the length-prefixed framing overhead
     exactly (`+48` bytes for the 1-chunk text file, `+272` bytes for the
     8-chunk binary file -- `16`-byte header plus `32` bytes of `[4-byte
     length][12-byte iv][16-byte tag]` framing per chunk, confirmed by hand).
     `file decrypt` back into a third directory, `diff`/`cmp` against the
     originals -- both files byte-for-byte identical, including the 500 KB
     binary file across all 8 chunks.
   - Hit one real gotcha along the way, unrelated to the crypto: BC-FIPS in
     approved-only mode rejects plain RSA `ENCRYPT_MODE`/`DECRYPT_MODE`
     (`"Cipher available for WRAP_MODE and UNWRAP_MODE only"`) -- fixed by using
     `Cipher.WRAP_MODE`/`UNWRAP_MODE` with a `SecretKeySpec` wrapping the DEK
     bytes, which is also the more semantically correct operation (wrapping a
     key, not encrypting arbitrary data) and needed no design change, just a
     different `Cipher` API.
7. **Performance/resume round, all against real Postgres (throwaway Docker
   containers, not H2) and real local files -- not simulated**:
   - **Partition-boundary math**: a deliberately uneven 9,973-row table split
     into 4 partitions computed as `2494/2493/2493/2493` -- sums to exactly
     9,973. Verified twice: once reading each partition sequentially, once
     running all 4 concurrently through the real Hikari pool (genuine parallel
     threads, not simulated) -- both runs saw every id `1..9973` exactly once,
     no gaps, no duplicates.
   - **Checkpoint transactional coupling**: 12 checks against real Postgres,
     including a simulated crash (an exception thrown mid-transaction, after
     both the data insert and the checkpoint write had been issued but before
     commit) -- confirmed neither the data row nor the checkpoint update
     landed. A real successful batch landed both together; a second batch
     correctly upserted (not duplicated) the checkpoint row.
   - **`hsm-bulk-service`'s new rotation scheduler**: 13 checks, booting the
     real Spring application and calling `RotationService` directly against
     real Postgres. Confirmed the full cycle -- old row retires
     (`rotation_status=rotated`, `current_dek_name` cleared, wrapped form left
     untouched and still decryptable), a fresh row takes over reuse under the
     same name -- and, importantly, that a freshly-rotated-in row is **not**
     re-swept under a realistic 24-hour cutoff (only under the artificial
     `maxAgeHours=0` used to force the first rotation deterministically),
     confirming the age-based cutoff logic actually discriminates, not just
     that the sweep runs.
   - **File-job crash/resume, 60,000 real files, real kill -9**: killed the
     process mid-run at 11,167 files actually written but only 687 flushed to
     the manifest (the expected gap between "written" and "flushed" at any
     given flush interval). Resumed with `resume: true` -- finished at exactly
     60,000/60,000 files and 60,000/60,000 manifest entries, zero duplicate
     manifest entries. No data loss, no corruption from the crash.
   - **File-job `dek-name` + decrypt dedup, same 60,000 files**: `dek_issued`
     audit events showed exactly 1 (all 60,000 files sharing one name, one
     `/dek/issue` call for the whole job) instead of 60,000. After the decrypt
     dedup fix, `dek_unwrapped` events showed 6,000 (one per 10-file batch --
     `files-per-batch`), not 60,000. Decrypted output diffed byte-for-byte
     identical against the original 60,000 source files.
   - **`checkpoint.flush-interval` fix, measured, same 60,000 files**: before
     decoupling the flush cadence from `files-per-batch` (effectively 10),
     resuming from a crash took **58 seconds** (6,000 manifest rewrites, each
     rewriting a growing list). After decoupling with the new default of
     `1000`, the full 60,000-file job (not just the resumed portion) took
     **10.9 seconds** (60 rewrites) -- re-verified with another real crash/resume
     cycle at the new interval: killed mid-run, resumed, landed at exactly
     60,000/60,000 files and manifest entries again, zero duplicates.
   - `mvn test` across the full reactor -- 69/69 passing, unchanged, both
     before and after every fix in this round.
