# Operational Runbook

Incident response procedures. Where a fact depends on your specific Azure
subscription, on-call structure, or SLA commitments, it's marked `TODO` —
fill in before treating this as a real on-call document, not after.

## Total lockout: no app_id can authenticate or decrypt anything

**There is currently no break-glass path around this.** Read this section
before an incident, not during one — knowing that up front changes how you
triage.

### Diagnose which layer is broken

Every app failing the same way points at one shared layer, not a
per-app problem. Check in this order:

1. **JWT validation** — is `GET /admin/health` reachable at all (it's
   public, no auth)? If yes, the app is up; the problem is auth-specific.
   Check `JWT_JWKS_URL`/`JWT_ISSUER`/`JWT_AUDIENCE` config and whether the
   JWKS endpoint itself is reachable from the pod. A single bad token
   produces `invalid_token: ...` in the audit log for that app only; *every*
   app failing the same way in the same window means the JWKS/issuer
   config itself is broken, not any individual app's credentials.
2. **`app_registrations` table** — query it directly
   (`SELECT app_id, active FROM ${access_schema}.app_registrations`). If
   rows are missing or all `active=false`, every app gets
   `unknown_or_inactive_app`.
3. **`hsm.security.access-rules`** — if a recent config/Helm-values change
   touched this, check whether a rule's `authorities` list was accidentally
   left empty or misspelled — `SecurityConfig` denies by default for
   anything that doesn't match a caller's actual authority strings exactly.

### Recovery paths per cause

| Cause | Fix | Requires |
|---|---|---|
| JWKS/issuer misconfigured | Config fix + redeploy | Access to Helm values / ConfigMap, a deploy pipeline |
| JWKS endpoint itself down (IdP outage) | Wait it out, or see "reduce exposure" below | Nothing you control directly |
| `app_registrations` wiped/corrupted | Restore from DB backup (see `DISASTER_RECOVERY.md`), or re-run the onboarding migrations for known apps | DB restore access |
| `hsm.security.access-rules` misconfigured | Config fix + redeploy | Same as JWKS fix |

Notice the chicken-and-egg problem: fixing `app_registrations` via
`/admin/apps/status` requires an authenticated ops-admin call — which is
exactly what's broken if this *is* the lockout. In that specific case, the
DB fix has to happen directly against the database, not through the API.

### Reduce how often you'd ever need this

- Cache the JWKS response locally with a generous grace TTL so a transient
  IdP blip doesn't cause instant total lockout (`RsaJwtValidator` currently
  re-fetches on a fixed schedule — confirm the TTL is generous enough for
  your IdP's actual reliability, or extend it).
- Treat `app_registrations` and `hsm_access` as Tier-0 backup targets (see
  `DISASTER_RECOVERY.md`) so "wiped" is a fast restore, not a real incident.
- `TODO`: decide whether a genuine break-glass tool is worth building — an
  out-of-band CLI, run by a human with direct Azure RBAC on the HSM and DB,
  entirely outside this service's HTTP API, with dual-control approval
  (e.g. Azure PIM just-in-time) and audit logging to a store independent of
  this service's own DB. This is a real design decision needing security
  sign-off on scope before building, not something to build unilaterally —
  a break-glass tool is itself a high-value attack target.

## KEK rotation stuck or partially completed

`RotationService.rotateKek` pages through stale records
(`PAGE_SIZE`-sized batches) and re-wraps each under the new KEK version.

- Check `records_queued` in the response / `kek_rotation_completed` audit
  event against the actual count of records still on the old
  `kek_version` (`SELECT count(*) FROM edek_records WHERE kek_version != '<new>' AND rotation_status = 'current'`).
- Rotation is safe to re-run — it only re-wraps records not already on the
  current version, so a second `POST /admin/rotate-kek` call after a
  partial failure picks up exactly where the first left off, it does not
  double-rotate already-current records.
- If it's failing entirely (not just partial), check Managed HSM
  reachability/throttling first — re-wrap is HSM-call-heavy for a large
  batch.
- `POST /admin/rotate-kek` (or `rekey-kek`, `rekey-kek/revert`) returning
  **409** means another rotation is still running — scheduled or manual, on
  any pod. Wait for its `kek_rotation_completed` / `kek_rekey_completed` /
  `kek_rekey_reverted` audit event and retry. See "Rotation jobs: one runner
  across all pods" below.

## Rotation jobs: one runner across all pods

Every replica of `hsm-core-service` — and of `hsm-bulk-service`, which is the
same image deployed as a second release against the same database —
registers `KekRotationScheduler` and `NamedDekRotationScheduler`. To stop
every pod sweeping the same rows at the same cron tick, `RotationService`
runs each job under a **Postgres session advisory lock**
(`PostgresAdvisoryJobLock`):

| Lock key | Guards |
|---|---|
| `hsm:kek-wrap` | `rotateKek`, `rekey`, `revertRekey` — scheduled and admin-API runs alike, since all three rewrite the same rows' KEK wrapping |
| `hsm:named-dek-rotation` | `rotateNamedDeks` |

**What to expect per tick:** exactly one pod logs `*_job_triggered` →
`*_job_completed`; every other pod logs `*_job_skipped
reason=already_running`, and a `*_skipped` audit event records it. The
admin endpoints answer **409 Conflict** instead of queuing.

**Ownership by release:** the `hsm-bulk-service` chart sets
`kekRotationEnabled` / `namedDekRotationEnabled` to `"false"`, so rotation
work stays on the primary release's pods. The lock is the backstop if both
ever have them enabled (including v1/v2 running side by side).

**Requirements:**
- `DATABASE_URL` must be a **direct** Postgres connection (port 5432). A
  transaction-mode PgBouncer would run the unlock on a different server
  session than the lock.
- Append `tcpKeepAlive=true` to `DATABASE_URL`. A pod that crashes drops its
  connection and Postgres frees the lock immediately; a pod that *hangs* with
  its TCP connection still open is only detected through keepalive.

**Is a rotation lock stuck?** If every tick logs `already_running` but no pod
logs `*_job_triggered` for that job, look for the session holding it:

```sql
-- pg_locks splits a 64-bit advisory key into classid (high 32 bits) and objid (low 32 bits).
SELECT k.key, l.pid, a.client_addr, a.application_name, a.state, a.backend_start, a.state_change
FROM pg_locks l
JOIN pg_stat_activity a ON a.pid = l.pid
JOIN (VALUES ('hsm:kek-wrap'), ('hsm:named-dek-rotation')) AS k(key)
  ON (l.classid::bigint << 32) | l.objid::bigint = hashtextextended(k.key, 0)
WHERE l.locktype = 'advisory' AND l.objsubid = 1;
```

If the holder's pod is genuinely gone or hung, restart that pod; only if that
isn't possible, `SELECT pg_terminate_backend(<pid>)` releases the lock.
Rotation is safe to re-run after an interrupted sweep (see above).

**Deploying a change to `RotationService` or the lock:** old pods (without the
lock) and new pods run side by side during a rolling update. Deploy outside
the rotation windows (default KEK `0 2 1 * *`, named-DEK `0 3 * * *`).

## Named-DEK rotation partially failed

`rotateNamedDeks` rotates each due row in its own transaction. A row that
fails (HSM error, constraint violation, …) is rolled back and **stays
current** — it is retried on the next nightly run — and the sweep carries on
with the remaining rows.

- Per failed row: a `named_dek_rotation_row_failed` audit event (`edek_id`,
  `error_type`) plus an ERROR log line with the full exception.
- Per run: `named_dek_rotation_completed` with `records_rotated`,
  `records_failed`, `candidates` and `status` = `success` /
  `partial_failure` / `aborted`.
- `status=aborted`: 10 rows failed in a row, so the sweep stopped early
  (`named_dek_rotation_aborted` in the logs). That pattern is systemic —
  check Managed HSM reachability/throttling and DB health first. The
  remaining rows are picked up on the next run once the cause is fixed.

## CEK rotation service down

`hsm-cache-key-rotator` rotates the Redis cache-encryption key every
`ROTATION_INTERVAL_HOURS` (default 4h). If it's down:

- **Not an emergency.** Pods hold their current CEK indefinitely with no
  errors — `CekHotReloadScheduler`'s poll loop simply finds nothing changed
  each cycle. Decrypt continues working normally (cache hits and HSM
  fallback both unaffected).
- On recovery, rotation resumes immediately (rotation fires right away on
  service restart, not just on the next scheduled interval) — no manual
  catch-up step needed.
- If you want defense-in-depth restored sooner (shorter exposure window on
  the current CEK) rather than waiting for the service to come back, that's
  the only reason to treat this as urgent — see `CACHING_AND_ROTATION.md`
  for why rotation cadence is a security dial, not a performance one.
- The rotator must only ever run as **one** pod — two would flip the same
  alpha/beta Key Vault slots against each other. The chart pins
  `replicaCount: 1` and uses `strategy: Recreate`, so a deploy stops the old
  pod before starting the new one. Expect a few seconds with no rotator
  during a rollout; that's intended, and harmless per the first bullet.

## Redis (DEK cache) down

- **Not an emergency, and not a backup target.** `RedisDekCache` swallows
  Redis errors and falls through to HSM unwrap on every decrypt
  (`DekCache.get` returns `null` on any exception rather than throwing).
  Expect elevated HSM call volume and latency, not failures.
- If Redis is down long enough that HSM throughput becomes the bottleneck,
  that's a capacity/scaling conversation, not a data-loss one.

## Closing out `dek_issue`/`dek_unwrap` access after a bulk window (once Tier 3 is built)

**Not built yet — see `BULK_OPERATIONS.md`'s Tier 3 proposal.** Documented
ahead of time so the procedure exists the moment the capability does,
rather than being improvised during a real onboarding window.

`dek_issue`/`dek_unwrap` are narrow-window scopes by design — granted for
an app's onboarding or de-boarding migration, never meant to stay standing
afterward (see Tier 3's "What this is NOT": onboarding/de-boarding only,
never steady-state traffic). The moment a bulk window closes:

- **Revoke via the admin endpoint, not direct SQL** — see
  `ADMIN_OPERATIONS.md`'s "Prefer the admin API over direct SQL": a raw
  update leaves the app's cached scopes stale for up to the cache TTL, and
  leaves no audit record of the revocation.
- If `hsm-bulk-service` is deployed per onboarding window (per the
  Development Plan's "on/off is a deployment operation" design), also
  scale it to zero / undeploy once the window closes. This is
  belt-and-suspenders, not a substitute for revoking the scope — an app
  with the scope but no reachable service, and an app with a reachable
  service but no scope, should each independently fail closed.
- Confirm via the audit log (the revocation event) and a direct read of
  `app_registrations.allowed_scopes` that the change actually took effect
  before treating the window as closed.

## Tracing a slow or failing single request (later round)

Every request gets one correlation ID (hsm-chassis-web's `RequestIdFilter`,
configured for hsm-core-service as `X-Correlation-Id` / MDC `correlationId`) —
reused from an incoming `X-Correlation-Id` (or, failing that, `X-Request-Id`)
header if the caller supplied a valid one, otherwise a fresh UUID. It's echoed back on the `X-Correlation-Id` response header and
placed in MDC, so it appears on every plain log line for that request
(`logging.pattern.level` in `application.yml`) without grepping timestamps
across concurrent traffic to reconstruct one request's story.

To trace a specific slow/failing request end to end:

1. Get its correlation ID — from the caller (they received it on the
   response header even if the call failed), or from the audit log entry if
   you only have an `app_id`/`edek_id`/timestamp to start from (the audit
   stream doesn't carry the correlation ID today — cross-reference by
   timestamp/app_id instead).
2. `grep "correlationId=<id>"` the service log. For `/encrypt` and
   `/decrypt`, you'll see, in order:
   - `encrypt_request_received` / the start of `decrypt(...)` — request
     accepted, caller/classification logged.
   - `resolve_dek_started` / `resolve_dek_completed duration_ms=<n>` —
     encrypt only; how long DEK resolution (cache hit, or KEK unwrap on a
     miss) took. Not AOP-covered — `resolveDek` is a private method called
     via self-invocation, which Spring AOP proxies cannot intercept, so
     it's timed manually instead.
   - `component_call_started` / `component_call_completed component=<..>
     method=<..> duration_ms=<n> status=success|error` — one pair per call
     into `PbacClient.check`, `KekClient.wrapDek`/`unwrapDek`, or
     `EdekRecordRepository.save` (`ComponentTimingAspect`). These are the
     four collaborators Spring AOP can actually intercept here: each sits
     behind an interface implemented by a distinct Spring bean, called from
     a *different* bean than the one invoking it — proxy-based AOP only
     sees calls that cross a bean boundary. `DekManager` (a `static`
     utility on a non-bean `final` class) is never covered this way for the
     same underlying reason as `resolveDek` above.
   - `encrypt_request_completed .../..._completed total_duration_ms=<n>` —
     full request wall-clock time.
   - A slow request shows up as a large gap between two adjacent lines,
     pointing at exactly which collaborator (PBAC check, KEK
     wrap/unwrap, EDEK save, or DEK resolution) is the bottleneck, rather
     than only knowing the request overall was slow.
3. For `/encrypt/batch` and `/decrypt/batch`, the same log lines repeat once
   per item, but on `batch-executor-N` threads (see `BatchExecutorConfig` in
   `BULK_OPERATIONS.md`) rather than the original `nio-*-exec-N` request
   thread — the correlation ID is deliberately propagated onto those pooled
   worker threads (`MdcPropagatingCallable`) since MDC is thread-local and
   would otherwise be lost the instant work leaves the original request
   thread, making a batch item's logs impossible to correlate back to its
   parent request.
4. Every `/encrypt` and `/decrypt` response also carries `status`, `code`,
   `message`, and `correlation_id` fields directly in the JSON body (a
   caller doesn't have to read a response header to get the ID back) —
   e.g. `{"status": "success", "code": "ENCRYPT_SUCCESS", "message":
   "Encryption completed successfully", "correlation_id": "..."}` alongside
   `ciphertext`. Note: `ciphertext_token` was the field's original name (see
   the additive-envelope round); a later, explicit follow-up decision
   renamed it to `ciphertext` across the whole system (core, bulk, client,
   demo UI, diagrams) — a deliberate breaking wire change, not additive.
   **Later still (minimal/full split)**: `edek_id`, `owner_app_id`,
   `algorithm`, `encoding` (encrypt only — decrypt's `encoding` stays
   default, it's functionally needed to interpret `plaintext`), and
   `kek_version` are no longer in the response by default — they're gated
   behind the `X-Response-Detail: full` request header (absent/anything
   else = minimal: just `ciphertext`/`plaintext`, `reused`, and the
   envelope fields above). The individual binary fields
   (`iv_b64`/`ciphertext_b64`/`tag_b64`) that used to sit alongside
   `ciphertext` for backward compat with a pre-token contract are gone
   entirely now, not gated — this service never had a real external
   consumer, so there was nothing to stay compatible with. See
   `ResponseViews`/`ResponseDetailBodyAdvice` (`com.hsm.core.web`) for the
   mechanism, and `EncryptResponse`/`DecryptResponse` (`com.hsm.core.dto`)
   for the current full field list and which view each field belongs to.
   The demo UI always sends `X-Response-Detail: full` (its field-breakdown
   panel explains every field it gets back) — a fresh `curl` without that
   header is the fastest way to see what a real caller gets by default.
   Error responses (4xx/5xx) go through a separate, unchanged
   `{"detail": "..."}` shape (`GlobalExceptionHandler`) — the caller can
   already always find the correlation ID for a failed call on the
   `X-Correlation-Id` response header regardless.

### Request ids across file-service, core-service and the Istio gateway

- **One id per request, across both services.** hsm-file-service takes `X-Request-Id`
  (MDC `requestId`) and forwards it on its calls to hsm-core-service (or the bulk
  release) as `X-Correlation-Id`, so core's **log lines** for that call carry the same
  id: `[correlationId=<id>]`, e.g. on `component_call_completed … method=unwrapDek`.
  core's **audit** events (`dek_unwrapped`) do **not** carry a correlation id — match
  them by time/app instead (see step 1 above).
- **Only cache misses reach core.** file-service keeps unwrapped DEKs in memory for
  `dekCache.ttl` (default 15 min, swept every 60 s). The first download of a file calls
  `/dek/unwrap` and shows up in core with that request's id; later downloads within the
  TTL are served from the cache and make **no** call to core — so there is no core log
  line for their ids. Don't shorten the TTL for tracing: it is the revocation lag and
  the HSM-call/latency dial. file-service's own log and audit line has every
  request's `request_id`.
- **Blank `[correlationId=]` on core lines is normal outside requests:** the CEK poll,
  rotation jobs, Azure SDK and Splunk-batcher threads aren't serving a request.
- **The Istio ingress gateway replaces external request ids.** Envoy treats requests
  arriving from outside the mesh as untrusted: it drops any incoming `x-request-id`
  and generates its own UUID before the request reaches file-service. A caller who
  sends `X-Request-Id: my-id` through the gateway gets back `x-request-id: <uuid>`,
  and that UUID — not `my-id` — is what file-service and core log. This is Envoy's
  default, not a service bug. (Verified 2026-10-12.)
  - **To trace a gateway request:** use the `x-request-id` the response returned and
    grep that in file-service and core.
  - **To test with your own id:** call from inside the mesh as the BFF identity (e.g.
    a temporary pod with the BFF's ServiceAccount and the sidecar injected). Inside
    the mesh Envoy keeps the caller's id; real BFF → file-service traffic is in-mesh.
  - **Keeping external ids** needs `preserve_external_request_id: true` on the
    gateway (an EnvoyFilter on the gateway's HTTP connection manager). It affects
    **every** service behind that gateway — a gateway-owner decision, tracked as
    GW-1 in [`ACTION_ITEMS.md`](ACTION_ITEMS.md).
- **`X-End-User` is caller-supplied, audit only.** Envoy passes it through unchanged,
  and file-service records whatever value arrives in its audit line (`end_user`). It
  is never used for authorization, by design — but the audit value is only as
  trustworthy as whoever set it. From the BFF (the only intended caller) that's fine;
  if outside callers can reach `/files/…` through the external gateway, they can put
  any name there. Tracked as SEC-1 in [`ACTION_ITEMS.md`](ACTION_ITEMS.md).

### Audit events: correlation and Splunk HEC delivery

- Every hsm-core-service audit event carries `service` (which release emitted it) and,
  inside a request, `request_id` (that request's correlation id); file-service's
  `file_access` carries `request_id`, `service` and `host`. To follow a file download
  into core's audit trail: take file-service's `request_id` and search core's audit
  events for the same `request_id` (only DEK-cache misses produce a core record).
- Direct Splunk HEC delivery (`SPLUNK_ENABLED`, off everywhere as of 2026-10-12) is
  best-effort; stdout (`audit.json`) is always the complete record. If enabled:
  - `splunk_hec_events_dropped count=… max_queue_size=…` — the in-memory queue hit its
    bound (`SPLUNK_MAX_QUEUE_SIZE`, default 10000) and the oldest events were dropped
    from HEC delivery (still on stdout). Usually Splunk unreachable or too slow.
  - `splunk_hec_delivery_failed …` — a batch failed and is retried next flush.
  - `splunk_hec_shutdown_undelivered count=…` — events still queued when the
    shutdown budget (`SPLUNK_SHUTDOWN_FLUSH_SECONDS`, default 10) ran out.

## HTTP client and Netty stack (all Java services)

**Current setup:**
- **Azure SDK:** every module that uses it (core, hsm-cache-key-rotator, crypto-client,
  file-store, and through them bulk-client, file-service and the Spark adapter)
  uses the **JDK HTTP client** (`azure-core-http-jdk-httpclient`).
  `azure-core-http-netty` is excluded from every `com.azure` dependency, which
  keeps reactor-netty off the classpath entirely.
- **Netty:** present only where Lettuce needs it (core's Redis DEK cache,
  hsm-cache-key-rotator's Redis operations), at the single version the parent pom's
  `netty-bom` import sets (4.2.x).

| Symptom | Cause | Action |
|---|---|---|
| `NoClassDefFoundError: io/netty/channel/MultiThreadIoEventLoopGroup` on the first Key Vault, Entra ID, Storage or Redis call | Mixed Netty lines on the classpath: something built for Netty 4.2 (reactor-netty 1.3, Lettuce 7) running on Netty 4.1. This was the state of every service until the Netty 4.2 / JDK-HttpClient change. | Check that the parent pom's `netty-bom` is the 4.2 line and that no module re-added `azure-core-http-netty` |

**Guard:** `HttpStackTest` in core, hsm-cache-key-rotator, crypto-client and file-store
fails the build if any of these break:
- the Azure SDK's default client is the JDK one;
- a real call through it links and fails only to connect;
- reactor-netty and `azure-core-http-netty` are absent;
- core and hsm-cache-key-rotator only: Lettuce links, and exactly one Netty version is
  present.

**Bumping Netty for a CVE:** change the `netty-bom` version in `java/pom.xml`
only, never a single Netty artifact.

## Rolling out a new hsm-core-service build (v1/v2)

For a build that changes core-service internals but no contract -- e.g. the chassis
adoption (Phase 7): crypto, FIPS, locking and the correlation-id filter moved to
shared libraries; API shapes, token/EDEK/cache formats and lock keys unchanged.
Old (v1) and new (v2) can run side by side against the same database, Key Vault and
Redis, because everything they share is pinned (COMPATIBILITY_CONTRACT.md).

**What makes side-by-side safe:**
- **Rotation jobs:** v1 and v2 use the same advisory-lock keys (`hsm:kek-wrap`,
  `hsm:named-dek-rotation`), so only one pod of either version runs a sweep.
- **Schema:** the build adds no Flyway migration, so v2's startup Flyway run is a
  no-op validation. (A build that *does* add one needs the add-only rule and a single
  migrating release -- not this procedure.)
- **Data:** tokens, EDEKs and cache entries written by either version are read by
  the other -- pinned by the shared test vectors.

**Procedure** (core first; repeat for the bulk release with its own values):
1. Note the current image tag (rollback point).
2. Install v2 as a **second release** of the same chart (e.g. `hsm-core-service-next`;
   "v2" is only a release label -- the API path stays `/api/sensec/hsm/v1`), with the
   same values file as v1 plus this overlay:
   ```yaml
   image:
     tag: "<new tag>"
   config:
     splunkSource: "hsm-core-service-next"   # tell v1 and v2 apart in Splunk
   serviceAccount:
     create: false                           # REQUIRED: reuse v1's ServiceAccount --
     name: "<v1 ServiceAccount name>"        # Azure Workload Identity trusts that name only
   ```
   Without the `serviceAccount` override the chart creates a new ServiceAccount named
   after the release, which Entra ID has no federated credential for: every Key Vault
   / Managed HSM call from v2 would fail. The release gets its own Service, ConfigMaps,
   HPA, PDB and NetworkPolicy (same rules as v1). Callers keep their URL; your Istio
   VirtualService for that host splits traffic between the v1 and v2 Services.
3. Check v2 pods before any traffic: first log line `Picked up JDK_JAVA_OPTIONS: …
   approved_only=true`; `bc_fips_native … approved_only=true`; no `FIPS check failed`;
   `job_lock_mode=postgres_advisory`.
4. Shift traffic with your Istio VirtualService weights (v1/v2 Services):
   0 → 5% → 25% → 50% → 100%, watching at each step: 4xx/5xx rate, decrypt latency,
   `NoSuchAlgorithm`/`FIPS` errors, and that `correlation_id`s look normal. Optional
   before 5%: **mirror** only `/decrypt`, `/decrypt/batch` and `/dek/unwrap` to v2 --
   never `/encrypt`, `/dek/issue` or admin calls (they write rows).
5. At 100% for the agreed period (suggested 2 weeks with no errors), upgrade the
   original release to the new tag and remove v2 -- or keep v2 and retire v1.
6. **Rollback at any step:** set the weight back to v1 (seconds). v1 keeps running
   throughout; nothing it relies on changed.

**Expected, intended differences in v2:** an incoming `X-Correlation-Id` with
characters outside `[A-Za-z0-9._:-]` (or > 128 chars) is replaced by a UUID instead of
being echoed; a caller sending only `X-Request-Id` gets that id adopted as the
correlation id.

## BC-FIPS approved-only mode (all Java services)

BC-FIPS's approved-only mode must hold on **every thread**, not just the one that
registered the provider. `CryptoServicesRegistrar.setApprovedOnlyMode(true)` — what
every service's `FipsBootstrap` used to rely on — only affects the calling thread:
request threads, schedulers and Spark tasks stayed in general mode and could use
non-approved algorithms. JVM-wide mode comes only from the system property
`org.bouncycastle.fips.approved_only=true`, read once when BC-FIPS initializes.

- **On the chassis** (`hsm-crypto-core` users: cache-key-rotator, file-service,
  bulk-client, Spark adapter): `FipsBootstrap.register()` sets the property before
  BC-FIPS initializes. Spring services on `hsm-chassis-core` verify it from a fresh
  thread at startup and **refuse to start** if it isn't in effect; the
  `bc_fips_native … approved_only=` line and the `fips` health component report the
  JVM-wide value.
- **If a service refuses to start** with `FIPS check failed: BC-FIPS is not in
  approved-only mode JVM-wide`: something initialized BC-FIPS before the property was
  set. Add `-Dorg.bouncycastle.fips.approved_only=true` to the JVM options (e.g.
  `JDK_JAVA_OPTIONS`), which guarantees it regardless of class-loading order.
- **hsm-core-service / hsm-bulk-service** are on the chassis too (Phase 7), so they
  also log `bc_fips_native … approved_only=` and refuse to start if it isn't
  JVM-wide. Their charts additionally pass the JVM flag: `bcFips.approvedOnly: true`
  (default) adds `-Dorg.bouncycastle.fips.approved_only=true` to `JDK_JAVA_OPTIONS`
  (first log line: `NOTE: Picked up JDK_JAVA_OPTIONS: …`). Keep it on: it guarantees
  the mode regardless of class-loading order, and it is what protected a pre-chassis
  image. `--set bcFips.approvedOnly=false` removes it.

## Spark adapter: FIPS flag and redeploying the jar

**Open item** -- tracked as SPARK-1 / SPARK-2 in [`ACTION_ITEMS.md`](ACTION_ITEMS.md).

`hsm-spark-adapter-1.0.0.jar` is a **shaded jar**: at build time Maven copies
`hsm-crypto-client` (and, since Phase 4, `hsm-crypto-core`) *into* it, and a cluster
runs only what is inside that file. Library changes in this repository reach a
cluster only when the jar is rebuilt and redeployed. A jar built before the chassis
work keeps working -- the token, transport and cache formats are unchanged and pinned
by the test vectors -- but it lacks the JVM-wide FIPS fix: **approved-only mode holds
only on the thread that registered BC-FIPS, not on executor task threads** (see
"BC-FIPS approved-only mode" above).

Not to be confused with the Python Databricks UDF (`hsm-databricks-udf`), which needs
none of this.

### Option 1 -- FIPS flag only, no rebuild (SPARK-1)

Closes the FIPS gap even with the old jar.

1. Add to the driver and executor JVM options:
   ```
   spark.driver.extraJavaOptions    -Dorg.bouncycastle.fips.approved_only=true
   spark.executor.extraJavaOptions  -Dorg.bouncycastle.fips.approved_only=true
   ```
   - `spark-submit` / `spark-defaults.conf`: the two lines above.
   - Databricks: Cluster → Edit → Advanced options → Spark → Spark config.
   - **If a key already has a value, append the flag after a space -- don't replace it**,
     e.g. `-Dsome.existing=x -Dorg.bouncycastle.fips.approved_only=true`.
2. Restart the cluster (or resubmit the job) -- JVM options apply to new JVMs only.
3. Verify (Scala notebook cell or `spark-shell`):
   ```scala
   spark.range(8).repartition(8).mapPartitions(_ =>
     Iterator(System.getProperty("org.bouncycastle.fips.approved_only"))).collect()
   // expect: every element "true"
   ```
   Then one `hsm_encrypt` → `hsm_decrypt` round trip, and `hsm_decrypt` on an
   existing (pre-change) value.
4. Rollback: remove the flag, restart.

### Option 2 -- rebuild and redeploy the jar (SPARK-2)

Brings the cluster onto the shared `hsm-crypto-core` code. Keep the Option 1 flag.

1. Build from `main`:
   ```bash
   cd java && mvn -B -pl hsm-spark-adapter -am package -DskipTests
   cp hsm-spark-adapter/target/hsm-spark-adapter-1.0.0.jar \
      hsm-spark-adapter-1.0.0-$(git rev-parse --short HEAD).jar
   ```
   The version is still `1.0.0`: the commit suffix gives the new file a distinct name.
2. Upload it **next to** the old jar, wherever you keep it today (DBFS / Volume /
   workspace files, HDFS / S3 / ADLS, or the container image). **Don't overwrite the old
   jar** -- it is the rollback, and managed platforms cache libraries by path, so an
   overwrite in place may not even take effect.
3. Point the cluster at the new file -- change only the adapter path:
   ```
   spark.jars  <path>/hsm-spark-adapter-1.0.0-<sha>.jar,<path>/bc-fips-2.1.1.jar
   ```
   - Databricks: Cluster → Libraries: install the new jar, uninstall the old one.
   - **`bc-fips-2.1.1.jar` stays exactly as it is, as its own separate jar** -- BC-FIPS
     checks its own integrity and fails (`Module checksum failed`) if bundled.
   - Unchanged: `spark.sql.extensions=com.hsm.spark.HsmUdfExtension`, every `spark.hsm.*`
     setting, and the Option 1 flag.
4. Restart the cluster (or resubmit the job).
5. Verify:
   ```scala
   Class.forName("com.hsm.crypto.FipsBootstrap")             // throws on the OLD jar, succeeds on the new one
   com.hsm.crypto.FipsBootstrap.isApprovedOnlyJvmWide()       // driver: true
   spark.range(8).repartition(8).mapPartitions(_ =>
     Iterator(com.hsm.crypto.FipsBootstrap.isApprovedOnlyJvmWide())).collect()   // executors: all true
   ```
   - `hsm_encrypt` → `hsm_decrypt` round trip returns the original values.
   - `hsm_decrypt` on values encrypted **before** the change returns the originals.
   - Executor logs: no `ClassNotFoundException`, `NoSuchAlgorithm`, `Module checksum failed`.
6. Rollback: point `spark.jars` / the cluster library back at the old jar, restart.

## BC-FIPS native libraries (all Java services)

Applies to `hsm-core-service` (including the `hsm-bulk-service` release),
`hsm-cache-key-rotator`, `hsm-bulk-client` and `hsm-file-service`. The
`hsm-core-service-loadtest` image doesn't load BC-FIPS.

**How it works:**
- BC-FIPS 2.x unpacks small native libraries from its jar at startup and loads
  them: a CPU-feature probe, plus AES, SHA and DRBG acceleration.
- Every image sets `-Dorg.bouncycastle.native.loader.install_dir=/opt/bc-native`.
- Every chart has `bcFips.nativeMode`:
  - `native` (default): the chart mounts a disk-backed `emptyDir` at
    `/opt/bc-native`, so `/tmp` never needs to be executable;
  - `java`: pure Java; the chart sets `-Dorg.bouncycastle.native.cpu_variant=java`,
    and nothing is unpacked or executed from disk.

| Symptom | Cause | Action |
|---|---|---|
| Pod exits at startup: `UnsatisfiedLinkError … /tmp/bc-fips-jni_…/libbc-probe.so: failed to map segment` | Pre-fix image. BC-FIPS unpacked into a `noexec` `/tmp` | Upgrade the image |
| Same error, but the path is `/opt/bc-native/…` | `/opt/bc-native` is writable but `noexec` (policy applied to the volume) | Allow exec on that volume, or set `bcFips.nativeMode: java` |
| Starts normally, but native acceleration is off | `/opt/bc-native` not writable, e.g. a chart without the `bc-native` volume. BC-FIPS falls back to pure Java; it does **not** fail. | Expected with older charts. Upgrade the chart to get native mode back. |

**Checking the mode:**
- `hsm-file-service` logs it at startup:
  `bc_fips_native enabled=true variant=avx aes_gcm_native=true`.
- The other services don't log it. The behaviour above, verified in
  containers, is the reference.

`java` mode uses the same approved algorithms, without the native acceleration.
Confirm with your FIPS compliance owner whether your validated configuration
requires one mode.

## hsm-file-service: triage by error code

Consumers run this service from the image and chart only, so triage starts
from the `error_code` in the response body and the matching `request_id` in
the pod log (`file_request_failed` / `file_request_rejected` /
`file_stream_aborted`) and in the `file_access` audit line. The full code
table is in `FILE_SERVICE.md`, "Error codes".

To reproduce a reported failure, fetch the same path yourself with
port-forward and curl, or through Swagger UI where developer docs are on
(`FILE_SERVICE.md`, "Testing a file quickly"):

```bash
kubectl -n <ns> port-forward deploy/hsm-file-service 8080:8080
curl -sS -D - -o out.bin http://localhost:8080/api/sensec/file/v1/files/<path>
```

| Symptom | Likely cause | Action |
|---|---|---|
| Every request `FS-502-KEY-UNAVAILABLE` right after install | No cross-app grant, or wrong `app_id` / public key | `GET /admin/grants`; compare `config.core.appId` with `app_registrations` |
| `FS-502-KEY-UNAVAILABLE` on one file | Key shredded, or the file was written by an app not covered by the grant | `GET /admin/edek/{edek_id}` (edek_id is in the log detail) |
| `FS-503-CORE-UNAVAILABLE` | hsm-core-service down or unreachable (NetworkPolicy egress, mesh) | Core's own health first; then `networkPolicy.coreServiceNamespace` |
| `FS-502-STORAGE` | Storage RBAC, private endpoint, throttling | Pod identity role assignment; storage metrics |
| `FS-422-INTEGRITY` | Stored file altered, truncated by a failed copy, or not written by a real job | **Treat as a security event.** Preserve the blob, check storage write logs, re-run the producing job for that file |
| `FS-412-FILE-ID-MISMATCH` | File replaced, restored from an old copy, or the BFF's record is stale | **Security-relevant.** Compare the served `X-HSM-File-Id` with the bulk result files |
| Downloads cut off, `outcome=aborted` in audit | Integrity failure after streaming began (large file) | Same as `FS-422-INTEGRITY`; the log carries the reason |
| Sidecar `403 RBAC: access denied` | Caller isn't the BFF principal in `istio.authorizationPolicy.bffPrincipals` | Fix the principal (`cluster.local/ns/<ns>/sa/<sa>`) |
| Pod exits at startup: `UnsatisfiedLinkError … libbc-probe.so: failed to map segment` | BC-FIPS unpack directory is `noexec` | See "BC-FIPS native libraries (all Java services)" above |
| Pods OOM-killed | Buffered burst above the memory budget | Lower `config.delivery.maxBufferedRequests` or raise `resources.limits.memory`; write UI files with 1 MiB chunks |

**Revoking access urgently.** Remove the grant (`DELETE /admin/grants`) or
deactivate the service's app_id. Cached keys keep working for up to the
cache TTL (15 min) plus 60 s. For immediate effect, also restart the
Deployment (`kubectl rollout restart`); shutdown zeroes the cache.

## `TODO`: fill in before this is a real on-call doc

- [ ] Escalation path / on-call rotation for each failure mode above
- [ ] Who has Azure RBAC to query/restore the DB directly during a lockout
- [ ] Whether the break-glass tool described above gets built, and by when
- [ ] Actual JWKS cache TTL vs. your IdP's observed reliability
