# Action items

Open follow-ups from the chassis migration (phases 0–7) and the work around it, so
nothing lives only in someone's memory. Update the status here when an item moves;
close it with the date and the commit or change that closed it.

Reference points in git: `pre-chassis` (last commit before the chassis work),
`deployed-phase7` (code running in the target environment as of 2026-10-12).

## Open

| ID | Item | Why | Owner | Priority | Status | Details |
|---|---|---|---|---|---|---|
| SPARK-1 | Add `-Dorg.bouncycastle.fips.approved_only=true` to `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions` on every cluster that loads `hsm-spark-adapter` | The jar on the clusters predates the JVM-wide FIPS fix: executor task threads are **not** in approved-only mode. The flag closes this with no rebuild | Ops | High | Open | RUNBOOK "Spark adapter: FIPS flag and redeploying the jar", Option 1 |
| SPARK-2 | Rebuild `hsm-spark-adapter` from `main` and redeploy it (new file name, old jar kept for rollback, `bc-fips-2.1.1.jar` unchanged) | The running jar contains pre-chassis copies of `hsm-crypto-client`; library changes reach a cluster only through a rebuilt shaded jar | Dev + Ops | Medium | Open | Same RUNBOOK section, Option 2 |
| CORE-1 | Finish the core/bulk switchover: upgrade the **original** core and bulk releases to the validated image tag, move VirtualService weight back to them, `helm uninstall` the `-next` releases | v2 is validated (2026-10-12) but still runs under the temporary `-next` release names | Ops | Medium | Open | RUNBOOK "Rolling out a new hsm-core-service build (v1/v2)", step 5 |
| CORE-2 | Unknown paths on core/bulk return 404, not 500 + ERROR stack trace | `NoHandlerFoundException` (any path outside the API prefix, and likely every unknown path when demo mode is off) reached the catch-all handler: 500 and ~90 log lines per scanner hit or mistyped URL. COMPATIBILITY_CONTRACT already says `404` for an unknown path | Dev | Low | Merged to main 2026-10-14; deploy pending (core/bulk image) | OBS-1_LOGGING_EVALUATION.md finding 7 |
| REL-1 | Tag `chassis-complete` on the deployed commit once CORE-1 is done | Milestone marker | Dev | Low | Open | — |
| REPO-2 | Remove the inactive legacy Python service (`app/`, `scheduler/`, `migrations/`, `tests/`) in its own commit | Not active; easy to mistake for live code. Git history keeps it | Dev | Low | Open | — |
| OPS-1 | Update any Splunk search/alert that filters on logger `com.hsm.fileservice.config.BcFipsStatusLogger` | file-service's `bc_fips_native` line now comes from `com.hsm.chassis.fips.FipsStatusLogger` (message unchanged + `approved_only`) | Ops | Low | Open | chassis README, adoption status |
| OBS-1 | Shared application logging setup (one JSON format, standard field names, text/JSON switch per release) | Services format logs differently; Splunk searches must be checked before turning JSON on | Dev, then Ops + Splunk owner | Medium | Step 1 (chassis logback, switch default `text`, unused logstash encoder removed) merged to main 2026-10-14, deploy pending; deploy shows no change. Step 2 (JSON per release) waits on Splunk Q1–Q4 | [OBS-1_LOGGING_EVALUATION.md](OBS-1_LOGGING_EVALUATION.md); RUNBOOK "Application log format" |
| GW-1 | Decide whether the Istio ingress gateway should keep caller-supplied request ids (`preserve_external_request_id: true` via EnvoyFilter) | The gateway replaces any external `X-Request-Id` with its own UUID, so outside callers can't trace with their own ids. Requests are still traceable end to end with the returned UUID. Gateway-wide setting: affects every service behind it | Gateway owner | Low | Decision needed | RUNBOOK "Request ids across file-service, core-service and the Istio gateway" |
| SEC-1 | Review the external gateway route to hsm-file-service: should outside callers reach `/files/…` at all (FILE_SERVICE.md describes a docs/try-it-out route), and should the gateway or BFF strip/overwrite `X-End-User`? | `X-End-User` is recorded in file-service's audit trail as sent. Through the external gateway any caller that gets in can set it, making the audit `end_user` spoofable. Not an authorization issue (never used for access decisions) | Security + gateway owner | Medium | Review needed | Same RUNBOOK section; FILE_SERVICE.md |
| POL-1 | Decide whether scheduled KEK rotation should be on | `KEK_ROTATION_ENABLED=false` in core v1/v2 and bulk: KEK rotation happens only via `POST /admin/rotate-kek`. Turning it on is a values change; the advisory lock already makes it safe | Security / Owner | Low | Decision needed | RUNBOOK "Rotation jobs: one runner across all pods" |

## Closed

| ID | Item | Closed | By |
|---|---|---|---|
| — | Rotation jobs ran on every pod (named-DEK runs aborting, KEK rotation multiplied) | 2026-10-10 | Advisory lock, `9527877` / `4a31ac0` |
| — | BC-FIPS approved-only was per-thread in core/bulk/rotator/file-service/bulk-client | 2026-10-11 | `e1c810e` (code), `9e5f996` (core/bulk chart flag) |
| — | Service images could not be built after the chassis modules were added | 2026-10-11 | `b743eba` (+ CI `docker` stage) |
| — | Chassis phases 0–4, 6, 7 adopted and deployed (phase 5 skipped by decision) | 2026-10-12 | `deployed-phase7` |
| FEAT-1 | Request-id forwarding file-service → core (`X-Correlation-Id`) | 2026-10-12 | `64b6487`; verified in target: gateway UUID seen on bulk-service `method=unwrapDek` log line |
| OBS-2 | Shared audit logging: step 1 (`request_id`/`service`/`host` + bounded HEC) and step 2 (`hsm-chassis-audit`, both services) | 2026-10-12 | `28bf1ec`, `c3cecdb`; both deployed and verified in target. Option C (field renames) only on request of the Splunk owners |
| REPO-1 | Git remote / push / CI wiring | 2026-10-12 | Not pursued here — owner's decision |
