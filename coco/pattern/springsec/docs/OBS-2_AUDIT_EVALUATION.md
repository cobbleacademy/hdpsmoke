# OBS-2: shared audit logging — evaluation

| | |
|---|---|
| **Item** | OBS-2 in [`ACTION_ITEMS.md`](ACTION_ITEMS.md) |
| **Status** | Decided 2026-10-12: recommendation accepted — step 1 (A + HEC fixes) implemented on branch `feature/obs-2-step1-audit-context`; then B; C only on request |
| **Date** | 2026-10-12 |
| **Scope** | Audit events only (the `audit.json` stream). Plain application logging is OBS-1 |

## 1. Summary

- **Two producers, two formats.** hsm-core-service emits 28 event types through
  `AuditLogger`; hsm-file-service emits one (`file_access`) through
  `AccessAuditLogger`. Both write one JSON line to the `audit.json` logger (stdout), but
  their envelopes differ: timestamp precision, field names for the same idea
  (`status` vs `outcome`, `end_user_id` vs `end_user`, `reason` vs `error_code`), and
  which context fields exist at all. Nothing else in the system produces audit events.
- **Core's audit events carry no request or correlation id.** A file-service request
  can be followed into core's *log lines* (`[correlationId=…]`), but not into core's
  *audit record* for the same call. This is the gap that started OBS-2.
- **Core's direct Splunk HEC sender has two robustness problems** (when
  `SPLUNK_ENABLED=true`): an **unbounded** in-memory queue with **unbounded** retry,
  and a fixed delivery ceiling of **one batch of 50 per 5 s (~10 events/s per pod)**.
  Above that rate the backlog grows without limit; during a Splunk outage it grows
  until the pod runs out of memory. On shutdown only one final batch is sent.
- **Recommendation:** (1) add the missing context fields to both services — additive,
  no Splunk search breaks; (2) fix the HEC sender (bounded queue, drain per tick, drop
  counter); (3) then move both services onto one small shared module,
  `hsm-chassis-audit`, that emits **today's fields plus a common envelope** — still
  additive. Renaming existing fields to one vocabulary (option C) only if the Splunk
  owners want it, with a dual-emit period.

## 2. Current state

### 2.1 Producers and delivery paths

| Service | Class | Events | stdout (`audit.json`) | Direct to Splunk HEC | In-memory UI buffer |
|---|---|---|---|---|---|
| hsm-core-service (+ bulk release) | `audit/AuditLogger` | 28 types (+ lock `*_skipped`) | ✓ always | ✓ when `SPLUNK_ENABLED=true` (`SplunkHecBatcher`) | ✓ last 200 (`RecentEventsBuffer`, demo UI) |
| hsm-file-service | `audit/AccessAuditLogger` | `file_access` | ✓ always | ✗ | ✗ |
| cache-key-rotator, bulk-client, Spark adapter, Databricks UDF | — | none | — | — | — |

Both `logback-spring.xml` files route logger `audit.json` to a console appender with
pattern `%msg%n`, so each line is exactly the JSON the service built. Whether Splunk
receives the **stdout** copy depends on the cluster's log shipper; if HEC is also
enabled for core, Splunk may receive core events **twice** (see question Q1).

### 2.2 Envelope comparison

| Concept | hsm-core-service (stdout line) | core via HEC | hsm-file-service |
|---|---|---|---|
| Event name | `event` **and** `event_type` | `event.event_type` | `event` (always `file_access`) |
| Time | `timestamp` `yyyy-MM-ddTHH:mm:ssZ` (**seconds**) | HEC `time` (epoch, ms) + `event._epoch` | `timestamp` ISO-8601 with **nanoseconds** |
| Level / logger | `level: info`, `logger: audit` | — | — |
| Host | `host` (pod hostname) | HEC `host` | — (only from log-shipper metadata) |
| Service | — (only via HEC `source`) | HEC `source` (`hsm-core-service` / `hsm-bulk-service`) | — |
| Request / correlation id | **— (missing)** | **— (missing)** | `request_id` |
| Outcome | `status` (`success` / `failure` / `skipped` …) | same | `outcome` (`success` / `error` …) |
| Failure detail | `reason` (some events) | same | `error_code` (`FS-…`) |
| End user | `end_user_id` (encrypt/decrypt) | same | `end_user` (`X-End-User`, unverified) |
| Caller identity | `app_id`, `sub`, `caller_ip` | same | `caller` (Istio SPIFFE id) |
| Duration | — | — | `duration_ms` |

### 2.3 Event catalog

**hsm-core-service** (field names as emitted; every event also has `status`):

| Group | Events | Typical fields |
|---|---|---|
| Data plane | `encrypt`, `decrypt`, `batch_encrypt`, `batch_decrypt`, `dek_issued`, `dek_unwrapped` | `app_id`, `sub`, `caller_ip`, `edek_id`, `owner_app_id`, `kek_version`, `dek_name`, `data_classification`, `end_user_id`, `reused`, `item_count`/`success_count`/`failure_count`, `reason` |
| Auth | `auth_failure`, `access_denied` | `app_id`, `caller_ip`, `reason`, `token_app_id`, `method`, `path`, `required_authorities` |
| Governance | `classification_check`, `dek_name_reservation_check` | `app_id`, `dek_name`, `data_classification`, `reserved_for`, `context` |
| Admin | `app_keys_updated`, `app_mtls_cert_updated`, `app_status_changed`, `grant_added`/`removed`, `dek_grant_added`/`removed`, `classification_grant_added`/`removed`, `kek_registry_entry_added`/`removed`, `edek_metadata_viewed` | `app_id`, `sub`, `target_app_id`, `grantee_app_id`, `owner_app_id`, `scope`, … |
| Key lifecycle | `kek_rotation_completed`, `kek_rekey_completed`, `kek_rekey_reverted`, `named_dek_rotation_completed`, `named_dek_rotation_row_failed`, `<operation>_skipped` | `kek_name`, `new_kek_version`, `records_*`, `triggered_by`, `edek_id`, `error_type`, `lock` |

**hsm-file-service:** `file_access` — `request_id`, `outcome`, `error_code`, `mode`,
`path`, `end_user`, `caller`, `file_id`, `format_version`, `bytes`, `duration_ms`.

## 3. Findings

| # | Finding | Impact | Severity |
|---|---|---|---|
| F1 | Core audit events carry no request/correlation id | A file-service request can't be joined to core's `dek_unwrapped` audit record by id; incident timelines rely on timestamps and `app_id` | Medium |
| F2 | HEC sender: unbounded queue + unbounded retry | During a Splunk outage, memory grows until the pod is OOM-killed — an audit outage becomes a service outage | **High if `SPLUNK_ENABLED=true`** |
| F3 | HEC sender: one batch (50) per 5 s tick ≈ 10 events/s per pod | At sustained higher rates (every encrypt/decrypt is an event) the backlog — and delivery lag — grow without limit even when Splunk is healthy | **High if enabled** |
| F4 | On shutdown only one final batch is flushed | Queued events beyond 50 are lost from HEC at every pod restart (the stdout copy remains) | Medium if enabled |
| F5 | Envelopes differ (time precision, `status`/`outcome`, `end_user_id`/`end_user`, `reason`/`error_code`, host/service presence) | Cross-service searches need per-service field logic | Low |
| F6 | file-service events don't name the service or host themselves | Relies on log-shipper metadata to tell which service/pod emitted them | Low |
| F7 | `end_user` / `end_user_id` are caller-supplied and unverified | Fine for audit context; must not be read as authenticated identity (see SEC-1) | Info |
| F8 | COMPATIBILITY_CONTRACT.md listed cache-key-rotator as an audit producer | It emits none — doc correction (made with this evaluation) | Info |

## 4. What Splunk depends on — unknown, to confirm

This repo doesn't contain the Splunk side. Before changing anything visible:
saved searches, dashboards and alerts that use `event_type`/`event`, `status`/`outcome`,
`source`, `index=hsm_audit`, or specific field names; and whether ingestion is via
**stdout collection**, **HEC**, or both (Q1). **Additive changes (new fields) cannot
break these; renames or removals can.**

## 5. Options

### A — Minimal, additive context (both services)

- Core `AuditLogger` adds `correlation_id` (from MDC `correlationId`) and `service`
  (`spring.application.name`) to every event.
- file-service `AccessAuditLogger` adds `service` and `host`.
- No field renamed or removed.

*Effort:* ~1 day incl. tests. *Risk:* very low (additive). *Closes:* F1, F6.

### B — Shared module `hsm-chassis-audit` (one implementation, additive envelope)

- One `AuditLogger` API (`log(eventType, fields…)`), auto-configured by the chassis.
- **Common envelope added to every event:** `event_type`, `timestamp` (ISO-8601, ms),
  `service`, `host`, `request_id` (from the chassis request-id MDC key, whatever the
  service calls it), plus the service's existing event-specific fields **unchanged**.
- Sinks: stdout `audit.json` (default); optional HEC sink with a **bounded queue**,
  drain-until-empty per tick, a dropped-events counter/metric and a bounded shutdown
  flush.
- Each service keeps its event names and today's fields (core keeps `event` and
  `event_type`; file-service keeps `event: file_access`), so existing searches keep
  working; new searches can use the common envelope across services.

*Effort:* ~3–5 days incl. migration of both services and tests. *Risk:* low (additive),
medium for the HEC sink change. *Closes:* F1–F6.

### C — One schema for all audit events (renames)

- Converge on one vocabulary (e.g. `outcome` everywhere, `end_user` everywhere,
  one timestamp format), drop duplicates (`event` vs `event_type`).
- **Breaking** for Splunk: needs a dual-emit period (old and new fields side by side),
  dashboard/alert migration, then removal.

*Effort:* ~1–2 weeks including the Splunk side. *Risk:* medium–high. *Closes:* F1–F6
plus a single query model.

## 6. Recommendation

1. **Now — option A** plus the **HEC fixes** (F2–F4) inside core: bounded queue (e.g.
   10 000 events, oldest dropped, counted and logged), flush until the queue is empty
   each tick (batches of `batch-size`), bounded shutdown flush (e.g. up to 10 s).
   Additive and contained; can ship with the next core release.
2. **Next — option B**, extracting what step 1 produced into `hsm-chassis-audit` and
   moving file-service onto it (gaining optional HEC). Still additive.
3. **Only if the Splunk owners want it — option C**, with a dual-emit period.

If HEC is **not** enabled anywhere (Q1), F2–F4 are latent, not live; step 1 still
fixes them before anyone turns HEC on.

## 7. Compatibility rules for any change

- Additive only until option C is explicitly agreed: never rename, remove or change
  the type of an existing field or event name (contract C12).
- Never put plaintext, DEKs, tokens, or exception messages that may carry secrets in an
  audit event — exception **types** only, as `named_dek_rotation_row_failed` does.
- Every new field gets a test asserting its presence on the emitted JSON line.
- `audit.json` stays one JSON object per line on stdout; Splunk HEC stays optional.

## 8. Decisions needed

| # | Question | Why it matters |
|---|---|---|
| Q1 | Is `SPLUNK_ENABLED=true` for core/bulk in any environment, and is stdout also shipped to Splunk? | Decides whether F2–F4 are live (and whether core events arrive twice) |
| Q2 | Who owns the Splunk searches/dashboards/alerts on `index=hsm_audit`, and can they list the fields they use? | Needed before option C; confirms option A/B are safe |
| Q3 | Field name for the id: `request_id` everywhere (file-service's name) or `correlation_id` in core? | Recommendation: `request_id` in the common envelope (B); core may also keep `correlation_id` for continuity with its log lines |
| Q4 | Go with step 1 (A + HEC fixes) now? | Smallest change that closes the original gap and the robustness risk |

### Answers (2026-10-12)

- **Q1:** `SPLUNK_ENABLED=false` for all services → F2–F4 are latent (HEC is never used
  today); they are fixed anyway in step 1 so HEC is safe to turn on later. Audit events
  reach Splunk only through stdout collection.
- **Q3:** `request_id` — core's audit events get `request_id` (its correlation id), the
  same name file-service already uses; no rename needed for option B.
- **Q4:** yes — step 1 (option A + HEC fixes), then option B.
- **Q2:** open (Splunk search owners) — only needed before option C.

