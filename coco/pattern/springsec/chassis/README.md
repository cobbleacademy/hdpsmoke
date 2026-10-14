# HSM chassis

Shared libraries every HSM service is meant to be built on, so cross-cutting
concerns live in one place instead of one copy per service. Services adopt them
**one at a time**; nothing here changes a service until that service adds the
dependency. The formats that must not change while that happens are in
[`../docs/COMPATIBILITY_CONTRACT.md`](../docs/COMPATIBILITY_CONTRACT.md).

| Module | What it is | Depends on |
|---|---|---|
| [`hsm-crypto-core`](hsm-crypto-core) | The one implementation of the shared crypto formats: `FipsBootstrap`, `DekManager` (v1 token), `IvFactory`, `TransportWrapper` (RSA-OAEP DEK transport), `DekCacheCodec` (Redis DEK cache). Plain Java, no Spring — safe on a Spark executor. | `bc-fips` only |
| [`hsm-chassis-core`](hsm-chassis-core) | Spring Boot auto-configuration for every Spring service, web or headless: BC-FIPS registered before any bean exists, a startup check that refuses to start outside approved-only mode, the `bc_fips_native` status line, a `fips` health component (when actuator is present), a cross-pod `JobLock` (when there is a `DataSource`), and the shared logback configuration (`hsm/chassis/logback/base.xml`: application logs as text or JSON, the `audit.json` stream passed through). | `hsm-crypto-core`, Spring Boot |
| [`hsm-chassis-web`](hsm-chassis-web) | Servlet-web auto-configuration on top of `hsm-chassis-core`: a configurable request-id filter (header, MDC key, accepted fallback headers; unsafe ids replaced) registered first in the chain, and the relative `<api-prefix>/docs` Swagger UI redirect. Error formats stay per service (contract C10). | `hsm-chassis-core`, Spring Web MVC |
| [`hsm-chassis-audit`](hsm-chassis-audit) | One audit implementation (OBS-2 option B): the `AuditLog` API adding `request_id` (from the request-id MDC key) and `service` to every event, the stdout `audit.json` sink (each service's timestamp format kept), optional bounded Splunk HEC delivery, and pluggable `AuditSink` beans. Additive to what services emitted before. | Spring Boot, Jackson |
| [`test-vectors`](test-vectors) | `crypto-v1.json`: language-neutral vectors every Java and Python implementation runs. Generated from the spec by `generate_crypto_v1.py`; never edited by hand. | — |

Not planned: a shared error-response format (needs a client migration first) and a
Helm library chart (Phase 5 skipped: core and bulk keep separate charts).

## Adoption status

| Service | On the chassis | Notes |
|---|---|---|
| hsm-cache-key-rotator | ✓ `hsm-chassis-core` | Phase 3. Own `FipsBootstrap` and cache codec removed; no `JobLock` (no database; single pod via `replicaCount: 1` + `Recreate`). |
| hsm-crypto-client (→ file-service, bulk-client, Spark adapter) | ✓ `hsm-crypto-core` | Phase 4. Own `DekManager`, `IvFactory`, `TransportWrapper`, `FipsBootstrap` removed; consumers import `com.hsm.crypto.*`. The Spark adapter jar bundles `hsm-crypto-core` and still excludes bc-fips (provided on the cluster). |
| hsm-file-service | ✓ `hsm-chassis-web`, `hsm-chassis-audit` (+ core) | Phase 6. Own `BcFipsStatusLogger`, `RequestIdFilter`, `ApiDocsEntryController` and FIPS static block removed; request-id contract kept (`X-Request-Id`, MDC `requestId`). Gains a `fips` health component (not in the liveness/readiness probes). |
| hsm-core-service (+ bulk release) | ✓ `hsm-chassis-web`, `hsm-chassis-audit` (+ core, crypto-core) | Phase 7. Own `DekManager`, `IvFactory`, `TransportWrapper`, `FipsBootstrap` (and its two static blocks), `JobLock` + `JobLockConfig`, `CorrelationIdFilter`, `ApiDocsEntryController` and Redis cache codec removed. Contracts kept: `X-Correlation-Id`/`correlationId`, lock keys, admin error texts, API shapes (`ApiContractBaselineTest`). Rollout: RUNBOOK "Rolling out a new hsm-core-service build (v1/v2)". |

Logging (OBS-1): core, file-service and the rotator use the chassis logback configuration
(their `logback-spring.xml` is a single include). hsm-bulk-client is not on the chassis and
keeps Spring Boot's default text logging.

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
5. Make the service's `src/main/resources/logback-spring.xml` the chassis include only:
   ```xml
   <configuration>
       <include resource="hsm/chassis/logback/base.xml"/>
   </configuration>
   ```
   and give its chart a `logging.format` value mapped to `HSM_LOG_FORMAT`.
6. Run `scripts/ci/build.sh java`: the vectors, the API baselines and the service's
   own tests must all still pass.

Every chassis bean is `@ConditionalOnMissingBean`: a service that needs different
behaviour defines its own bean and the chassis steps aside.

### Properties

| Property | Default | Effect |
|---|---|---|
| `hsm.chassis.web.request-id.header` | `X-Request-Id` | Header the request id is read from and echoed on |
| `hsm.chassis.web.request-id.mdc-key` | `requestId` | MDC key every log line of the request carries |
| `hsm.chassis.web.request-id.also-accept` | (none) | Comma-separated fallback headers to take an id from when the primary is absent |
| `hsm.chassis.web.api-prefix` | (none) | Prefix for the `/docs` redirect; without it (or with Swagger UI off) no redirect is registered |
| `hsm.chassis.audit.service` | `spring.application.name` | `service` field on every audit event (core sets its per-release Splunk source) |
| `hsm.chassis.audit.timestamp-format` | `iso-instant` | `seconds` keeps hsm-core-service's whole-second audit timestamps |
| `hsm.chassis.audit.hec.enabled` | `false` | Splunk HEC delivery (`url`, `token`, `index`, `source`, `sourcetype`, `batch-size`, `flush-interval-seconds`, `max-queue-size`, `shutdown-flush-seconds`). hsm-core-service keeps its own `hsm.splunk.*` / `SPLUNK_*` settings |
| `hsm.chassis.logging.format` (env `HSM_LOG_FORMAT`) | `text` | Application-log format. `text`: Spring Boot's default line, unchanged. `json`: one JSON object per line via Boot's structured logging (logstash format) with `timestamp`, `message`, `logger`, `thread`, `level`, `request_id` (from `hsm.chassis.web.request-id.mdc-key`), `service` (= `hsm.chassis.audit.service`), `host` (`HOSTNAME`, the pod name), `stack_trace` (max 4000 chars); banner off. Any other value: warning, then `text`. A service's own `logging.structured.*` settings win. The `audit.json` lines are identical in both |
| `hsm.chassis.fips.enforce` | `true` | `false` turns the startup check into an ERROR log line. Local experiments only — never in a deployed environment. |
