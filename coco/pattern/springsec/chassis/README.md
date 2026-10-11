# HSM chassis

Shared libraries every HSM service is meant to be built on, so cross-cutting
concerns live in one place instead of one copy per service. Services adopt them
**one at a time**; nothing here changes a service until that service adds the
dependency. The formats that must not change while that happens are in
[`../docs/COMPATIBILITY_CONTRACT.md`](../docs/COMPATIBILITY_CONTRACT.md).

| Module | What it is | Depends on |
|---|---|---|
| [`hsm-crypto-core`](hsm-crypto-core) | The one implementation of the shared crypto formats: `FipsBootstrap`, `DekManager` (v1 token), `IvFactory`, `TransportWrapper` (RSA-OAEP DEK transport), `DekCacheCodec` (Redis DEK cache). Plain Java, no Spring — safe on a Spark executor. | `bc-fips` only |
| [`hsm-chassis-core`](hsm-chassis-core) | Spring Boot auto-configuration for every Spring service, web or headless: BC-FIPS registered before any bean exists, a startup check that refuses to start outside approved-only mode, the `bc_fips_native` status line, a `fips` health component (when actuator is present), and a cross-pod `JobLock` (when there is a `DataSource`). | `hsm-crypto-core`, Spring Boot |
| [`test-vectors`](test-vectors) | `crypto-v1.json`: language-neutral vectors every Java and Python implementation runs. Generated from the spec by `generate_crypto_v1.py`; never edited by hand. | — |

Planned: `hsm-chassis-web` (request-ID filter, error format, OpenAPI defaults) and
a Helm library chart.

## Adoption status

| Service | On the chassis | Notes |
|---|---|---|
| hsm-cache-key-rotator | ✓ `hsm-chassis-core` | Phase 3. Own `FipsBootstrap` and cache codec removed; no `JobLock` (no database; single pod via `replicaCount: 1` + `Recreate`). |
| hsm-crypto-client (→ file-service, bulk-client, Spark adapter) | — | next: `hsm-crypto-core` |
| hsm-file-service | — | with `hsm-chassis-web` |
| hsm-core-service (+ bulk release) | — | last; run as v1/v2 in parallel |

Images: a module on the chassis needs the chassis **sources** copied in its
Dockerfile (`COPY chassis/<module>/src …`), not just the poms every Dockerfile
already copies. The CI `docker` stage builds every image, so a missing copy fails
there.

## Adopting `hsm-chassis-core` in a Spring service

1. Add the dependency:
   ```xml
   <dependency>
       <groupId>com.hsm</groupId>
       <artifactId>hsm-chassis-core</artifactId>
       <version>1.0.0</version>
   </dependency>
   ```
2. Delete the service's own `FipsBootstrap` class and the
   `static { FipsBootstrap.register(); }` block on its application class — the
   chassis registers BC-FIPS earlier than that block ran, including under
   `@SpringBootTest`.
3. Replace the service's own copies of `DekManager`, `IvFactory`,
   `TransportWrapper` with `com.hsm.crypto.*` from `hsm-crypto-core`, and its
   `TestFipsSupport` test helper with nothing (the library registers itself).
4. If the service has its own `JobLock`/`JobLockConfig`, delete them and inject
   `com.hsm.chassis.lock.JobLock`. **Keep the lock key strings unchanged**
   (contract C13) so old and new builds still exclude each other.
5. Run `scripts/ci/build.sh java`: the vectors, the API baselines and the service's
   own tests must all still pass.

Every chassis bean is `@ConditionalOnMissingBean`: a service that needs different
behaviour defines its own bean and the chassis steps aside.

### Properties

| Property | Default | Effect |
|---|---|---|
| `hsm.chassis.fips.enforce` | `true` | `false` turns the startup check into an ERROR log line. Local experiments only — never in a deployed environment. |
