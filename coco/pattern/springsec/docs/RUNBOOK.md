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

## CEK rotation service down

`cek-rotation-service` rotates the Redis cache-encryption key every
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

Every request gets one correlation ID (`CorrelationIdFilter`) — reused from
an incoming `X-Correlation-Id` header if the caller supplied one, otherwise
a fresh UUID. It's echoed back on the `X-Correlation-Id` response header and
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

## `TODO`: fill in before this is a real on-call doc

- [ ] Escalation path / on-call rotation for each failure mode above
- [ ] Who has Azure RBAC to query/restore the DB directly during a lockout
- [ ] Whether the break-glass tool described above gets built, and by when
- [ ] Actual JWKS cache TTL vs. your IdP's observed reliability
