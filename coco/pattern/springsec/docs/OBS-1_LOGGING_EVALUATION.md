# OBS-1: shared application logging — evaluation

| | |
|---|---|
| **Item** | OBS-1 in [`ACTION_ITEMS.md`](ACTION_ITEMS.md) |
| **Status** | Option C chosen 2026-10-14 (D1–D4, D6 as recommended). Step 1 merged to main, deploy pending; step 2 waits on Q1–Q4 |
| **Date** | 2026-10-14 |
| **Scope** | Plain application logs (`log.info/warn/error(...)` → the `CONSOLE` appender). The audit stream (`audit.json`) is OBS-2 and does **not** change |

## 1. Summary

- **Every Spring service writes Spring Boot's default text line.** The only differences
  are the request-id tag (`[correlationId=…]` in core/bulk, `[requestId=…]` in
  file-service, none in the rotator and bulk-client) and that core and file-service each
  keep their own copy of `logback-spring.xml`.
- **Text lines are hard to use in Splunk:** fields such as level, logger and request id
  need regex extraction, the request id has two names, and **multi-line events
  (stack traces) arrive as many separate lines**. In a local run of core, one
  unexpected-exception error was **~90 lines** of stdout.
- **Spring Boot 4.1 already has JSON console logging built in**
  (`StructuredLogEncoder`, format `logstash`). Proven locally on the current core jar:
  configuration only, no Java code. The request id can be renamed to `request_id` and
  the field names aligned with the audit envelope (`timestamp`, `level`, `logger`,
  `service`, `host`), all through `logging.structured.json.*` properties.
- **`logstash-logback-encoder` is declared in core, the rotator and bulk-client but used
  nowhere** (no class or XML references it). The built-in encoder replaces it, and core's
  Jackson exclusion workaround for it can go too.
- **Recommendation:** option C (section 5). One shared logging include in the chassis
  with a **text/JSON switch per release, defaulting to today's text**, so deploying it
  changes nothing. Then turn JSON on one release at a time (bulk or core v2 first),
  after confirming the Splunk searches in section 4.

## 2. Current state

### 2.1 Per service

| Service | Logging config | Console line | Request id in line | Multi-line events |
|---|---|---|---|---|
| hsm-core-service (+ bulk release) | own `logback-spring.xml` (CONSOLE text + `audit.json` passthrough) | Boot default text | `[correlationId=<id>]` (`logging.pattern.level`) | stack traces; Hibernate's startup "Database info" block |
| hsm-file-service | own `logback-spring.xml` (copy of core's) | Boot default text | `[requestId=<id>]` | stack traces |
| hsm-cache-key-rotator | none (Boot programmatic default) | Boot default text | — (no requests) | stack traces |
| hsm-bulk-client | none (Boot programmatic default) | Boot default text | — | stack traces |
| hsm-spark-adapter / Databricks UDF | the Spark/Databricks cluster's logging | out of scope | — | — |

Both `logback-spring.xml` files are identical apart from a comment: CONSOLE with
`${CONSOLE_LOG_PATTERN}`, and logger `audit.json` → `%msg%n`, `additivity=false`.

### 2.2 What the lines look like today (captured locally, current core jar, demo mode)

```text
2026-10-14T07:23:52.061-05:00  INFO [correlationId=obs1-demo-001] 11104 --- [hsm-core-service] [nio-3913-exec-2] com.hsm.core.service.EncryptionService   : encrypt_request_received app_id=ops-admin caller_sub=demo-user-3 caller_ip=0:0:0:0:0:0:0:1 classification=null
2026-10-14T07:23:52.199-05:00  INFO [correlationId=obs1-demo-001] 11104 --- [hsm-core-service] [nio-3913-exec-2] com.hsm.core.service.EncryptionService   : resolve_dek_completed app_id=ops-admin dek_name=null reused=false duration_ms=137
{"event":"encrypt","event_type":"encrypt",…,"request_id":"obs1-demo-001","service":"hsm-core-service",…}      ← audit line, already JSON
2026-10-14T07:23:52.263-05:00 ERROR [correlationId=obs1-demo-002] 11104 --- [hsm-core-service] [nio-3913-exec-4] c.hsm.core.web.GlobalExceptionHandler    : unhandled_exception path=uri=/actuator/health error=No endpoint GET /actuator/health.

org.springframework.web.servlet.NoHandlerFoundException: No endpoint GET /actuator/health.
	at org.springframework.web.servlet.DispatcherServlet.noHandlerFound(DispatcherServlet.java:1177)
	at …                                                       ← ~88 more lines, each a separate stdout line
```

Same run: **175 stdout lines** as text versus **75** with JSON (section 2.3; that count still includes the 8-line banner). The
difference is almost entirely stack-trace and Hibernate continuation lines.

### 2.3 The same requests with Boot's built-in JSON (proposed field set, captured locally)

```json
{"timestamp":"2026-10-14T07:26:38.327686-05:00","message":"encrypt_request_received app_id=payments-svc caller_sub=demo-user-1 …","logger":"com.hsm.core.service.EncryptionService","thread":"http-nio-3913-exec-2","level":"INFO","request_id":"obs1-demo-001","service":"hsm-core-service","host":"<pod name>"}
{"event":"encrypt","event_type":"encrypt","level":"info","logger":"audit",…,"request_id":"obs1-demo-001",…}   ← audit line, unchanged
{"timestamp":"…","message":"unhandled_exception path=uri=/actuator/health error=…","logger":"com.hsm.core.web.GlobalExceptionHandler","thread":"http-nio-3913-exec-4","level":"ERROR","request_id":"obs1-demo-002","stack_trace":"org.springframework…\n\tat …","service":"hsm-core-service","host":"<pod name>"}
```

- `message` keeps today's text exactly, so `kubectl logs … | grep cek_rotated` and the
  RUNBOOK log checks still work.
- The whole stack trace is **one event** in `stack_trace`.
- The audit lines are byte-for-byte unchanged (same `audit.json` → `%msg%n` route).

## 3. Findings

1. **Two names for the same request id** in application logs (`correlationId`,
   `requestId`), and a third in audit (`request_id`). Joining a request's application
   lines across file-service and core needs two different text matches.
2. **Stack traces split into many Splunk events** unless the Splunk sourcetype has
   line-merging rules for this format (unknown, question Q2).
3. **No pod or service field** in the line itself; Splunk only has them if the log
   shipper adds Kubernetes metadata.
4. **Duplicate config:** two copies of `logback-spring.xml`; the rotator and
   bulk-client use Boot's default with no `audit.json` route (they emit no audit, so no
   impact today).
5. **Unused dependency:** `logstash-logback-encoder` 7.4 in core, the rotator and
   bulk-client, plus a Jackson exclusion and re-declaration in core's `pom.xml` that
   exists only because of it.
6. **Built-in support is enough.** Verified on the current core jar with a scratch
   config: `StructuredLogEncoder` (logstash format), MDC included automatically,
   renames, `exclude`, `add`, a stack-trace length cap, banner off, and a
   property-driven text/JSON switch inside `logback-spring.xml`. No new library needed.
7. **Side finding (not OBS-1):** a request to an unmapped path such as
   `/actuator/health` in core returns **500** and logs an **ERROR with a full stack
   trace** (`NoHandlerFoundException` reaches the catch-all handler; only
   `NoResourceFoundException` is mapped to 404). Every scanner or mistyped URL adds an
   ERROR and ~90 lines today. Tracked as CORE-2 (section 8, D6).

## 4. What Splunk depends on — unknown, to confirm

| # | Question | Why it matters |
|---|---|---|
| Q1 | Do any Splunk **alerts, dashboards or saved searches** match application-log text, e.g. `"cek_rotated new_version="`, `"correlationId="`, `" ERROR "`, `"kek_rotation_completed"`? | Free-text matches on the message still work in JSON (`message` keeps the text). Matches on the line **layout** (`" ERROR ["`, `"correlationId="`, logger column) would stop matching |
| Q2 | Which **sourcetype** do container stdout lines get, and does it already extract JSON (`KV_MODE=json` / `INDEXED_EXTRACTIONS=json`)? | The audit JSON lines share the same stream and are already searchable by `request_id`, so JSON extraction is probably already on. Confirm before relying on it |
| Q3 | Does that sourcetype take the **event time** from the line or from the log shipper? | JSON lines start with `{"timestamp":…`, not a timestamp. If Splunk parses the time from the line start (`TIME_PREFIX`), it needs adjusting; if it uses the shipper's time, nothing changes |
| Q4 | Line-length / truncation limits (Splunk `TRUNCATE`, default 10,000 bytes; containerd splits lines over 16 KB) | One JSON event includes the whole stack trace. Capping it (`stacktrace.max-length`, e.g. 4000) keeps every event under both limits |

## 5. Options

### A — Leave as is
No work, no risk. Request-id joins in application logs stay manual; stack traces stay
split.

### B — Text only: one request-id tag
Change both `logging.pattern.level` values to `[request_id=%X{…}]`. Tiny, but it
**breaks** any search on `correlationId=` / `requestId=` and fixes nothing else. Not
recommended.

### C — Shared include in the chassis, text/JSON switch per release (recommended)

1. `hsm-chassis-core` ships one logback include, for example
   `hsm/chassis/logback/base.xml`: Boot's defaults, CONSOLE chosen by
   `hsm.chassis.logging.format` (`text` default | `json`), and the `audit.json` →
   `%msg%n` route. Each service's `logback-spring.xml` shrinks to a 3-line include;
   the rotator gets the same file. (As built: bulk-client is not on the chassis and keeps
   Boot's default text logging, D4.)
2. With `json`, the chassis sets the field defaults (each overridable):

   | Property | Value |
   |---|---|
   | `logging.structured.format.console` | `logstash` |
   | `logging.structured.json.rename` | MDC request-id key → `request_id`; `@timestamp` → `timestamp`; `logger_name` → `logger`; `thread_name` → `thread` |
   | `logging.structured.json.exclude` | `@version`, `level_value` |
   | `logging.structured.json.add` | `service` (= `hsm.chassis.audit.service`), `host` (= `${HOSTNAME}`, the pod name) |
   | `logging.structured.json.stacktrace.max-length` | `4000` |
   | `spring.main.banner-mode` | `off` (the ASCII banner is 8 non-JSON lines) |

   The request-id key is read from `hsm.chassis.web.request-id.mdc-key`, as
   `hsm-chassis-audit` already does, so core and file-service need no extra setting.
3. Remove the unused `logstash-logback-encoder` from the three POMs (and core's
   Jackson workaround for it).
4. Helm: one value per chart, `logging.format: text` (default), mapped to an env var.
   Turning JSON on or off is a values change and a restart, no rebuild.

**Not changed by C:** HTTP headers (`X-Correlation-Id`, `X-Request-Id`, contract C11),
MDC keys, the `message` text, the audit stream, Splunk HEC.

## 6. Recommendation

Option C, in two steps, the same pattern as OBS-2:

| Step | What | Visible change on deploy |
|---|---|---|
| **1** | Chassis include + switch (default `text`), the services' `logback-spring.xml` reduced to the include, the rotator gets it (bulk-client stays on Boot's default, D4), unused dependency removed, chart value added. Tests: text output identical to today; JSON output has the agreed fields; audit lines unchanged | **None.** Text lines identical, verified by a test that compares the line layout |
| **2** | After Q1–Q4 are answered: set `logging.format: json` on **one release** (bulk or core v2), check Splunk, then the others one by one. bulk-client stays text (people read its output directly) unless wanted | Application lines become JSON for that release only. Rollback: the value back to `text` |

Effort: step 1 is about a day including tests and Dockerfile checks. Step 2 is values
changes plus your Splunk checks.

## 7. Compatibility rules for any change

- The audit stream (`audit.json`, `%msg%n`) and its fields do not change (C12).
- The default stays `text` and identical to today's line until a release opts in.
- `message` text is never reworded as part of OBS-1.
- Request-id headers and MDC keys do not change (C11); only the JSON **field name** is
  `request_id`.
- No secrets, tokens or key material in log lines (unchanged rule; JSON does not add
  any MDC beyond what the services already put there).

## 8. Decisions needed

| # | Decision | Recommendation |
|---|---|---|
| D1 | Go with option C? | Yes |
| D2 | Field names: align with the audit envelope (`timestamp`, `level`, `logger`, `request_id`, `service`, `host`) or keep Boot's logstash names (`@timestamp`, `logger_name`, …)? | Align with audit |
| D3 | Which release turns JSON on first? | bulk-service or core v2 |
| D4 | bulk-client: JSON too, or text only? | Text (human-read output) |
| D5 | Answers to Q1–Q4 (Splunk owner) | Needed before step 2, not before step 1 |
| D6 | Side finding: track the unmapped-path 500 as **CORE-2**? | Yes, small fix in `GlobalExceptionHandler` (map `NoHandlerFoundException` to 404, no stack trace). Separate from OBS-1 |

## Appendix: how this was verified (local, scratch only)

- Ran the current `hsm-core-service.jar` (Spring Boot 4.1.0) in demo mode on H2, sent
  one `/encrypt` with `X-Correlation-Id: obs1-demo-001` and one unmapped path with
  `obs1-demo-002`.
- `--logging.structured.format.console=logstash` alone has **no effect** on core,
  because its own `logback-spring.xml` defines CONSOLE with a text pattern. The switch
  must therefore live in the logback config (option C step 1).
- A scratch `logback-spring.xml` with
  `<springProperty name="HSM_LOG_FORMAT" source="hsm.chassis.logging.format" defaultValue="text"/>`
  and `<include file="console-${HSM_LOG_FORMAT}.xml"/>` (each file including Boot's
  `console-appender.xml` or `structured-console-appender.xml`) switched correctly
  between the two, with the audit lines unchanged in both.
- All renames, `exclude`, `add`, `stacktrace.max-length` and `banner-mode=off` in the
  table in section 5 were applied together and produced the lines in section 2.3, with
  no non-JSON lines left on stdout.
