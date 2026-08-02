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

## Verified

1. `mvn -pl hsm-bulk-service -am test` -- 7/7 passing.
2. `mvn -pl hsm-core-service -am test` -- 55/55 still passing with V6 applied
   (schema-additive, no regressions).
3. **Token-format compatibility** (the hard requirement): a DEK issued via
   `/dek/issue`, unwrapped and used locally to build a `ciphertext_token` via
   `DekManager.packToken`, decrypted correctly through `hsm-core-service`'s real,
   unmodified `/decrypt` endpoint. Confirmed live, not just by test.
4. **Benchmark, live run, 200 records, both services local/mocked**:

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
5. **`hsm-bulk-client`, live run, both jobs, against real `hsm-core-service` +
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
