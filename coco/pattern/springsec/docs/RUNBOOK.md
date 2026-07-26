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

## `TODO`: fill in before this is a real on-call doc

- [ ] Escalation path / on-call rotation for each failure mode above
- [ ] Who has Azure RBAC to query/restore the DB directly during a lockout
- [ ] Whether the break-glass tool described above gets built, and by when
- [ ] Actual JWKS cache TTL vs. your IdP's observed reliability
