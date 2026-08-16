# Live Demo Guide

This walks through running and presenting the Centralized Encryption Service
without any real Azure, Postgres, or Splunk infrastructure. `DEMO_MODE=true`
swaps those for in-memory equivalents (see `app/demo/`) — every other line of
service code (routers, auth, crypto, rotation, grants) runs unmodified.

## 1. Launch

```bash
./run_demo.sh
```

This creates a virtualenv, installs dependencies, copies `.env.demo` → `.env`,
and starts the service on port 3005. Open **http://localhost:3005/** — it
redirects to the UI's real path, **http://localhost:3005/api/sensec/hsm/**,
served as static files by the same FastAPI app, under the same root the API
itself uses. API calls go to `/api/sensec/hsm/v1` — this is the service's
real external root, and now the UI lives under that same root too, not at
bare `/`, so an Istio (or equivalent) route that forwards a path prefix
without rewriting reaches both correctly.

## 2. What's Simulated vs Real

| Component | In this demo | In production |
|---|---|---|
| Master key (KEK) | In-process AES-256 key (`app/demo/mock_kek_client.py`) | Azure Key Vault Managed HSM, FIPS 140-2 L3 |
| Caller auth | Fixed demo tokens (`app/demo/mock_jwt_validator.py`) | RS256 JWT signed by Azure AD / your IdP |
| EDEK store | SQLite file (`demo_hsm.db`) | PostgreSQL |
| Audit destination | In-memory ring buffer, polled by the UI | Same buffer + Splunk HEC |
| CEK slot rotation | Not running — no Redis cache in demo mode | Separate `cek-rotation-service` pod (Rotation SPN); rotates every 4 h and immediately on pod recovery |
| Redis DEK cache | Disabled (`DEK_CACHE_ENABLED=false`) | Azure Cache for Redis (TLS); CEK-encrypted entries keyed as `dek:{slot}:{kv_version}:{edek_id}`; 60 s TTL |

Everything else — AES-256-GCM encryption, random IV per call, envelope
encryption, scope enforcement, cross-app grant model, app-status blocking,
and key rotation logic — is the real production code path.

**Container note:** `DEMO_MODE`'s SQLite store is not safe under multiple
`uvicorn` workers — two workers both running schema creation against the same
file at boot can race. This only affects demo mode; production runs against
Postgres via `DATABASE_URL` and doesn't hit this. If you ever run demo mode
inside the Docker image, override the entrypoint to a single worker.

## 3. Demo Apps & Default Grant

| App | Scopes | Notes |
|---|---|---|
| `payments-svc` | `encrypt`, `decrypt` | Typical producer — encrypts its own data, reads it back |
| `reporting-app` | `decrypt` only | Has **no** encrypt scope, and a grant is seeded so it can decrypt `payments-svc`'s data — see step 5 |
| `ops-admin` | `encrypt`, `decrypt`, `rotate`, `grant`, `manage_apps` | Full admin — rotates the KEK, manages cross-app grants, and can block/restore other apps |

One grant is seeded at startup: **`reporting-app` may decrypt anything
`payments-svc` encrypts.** This is what makes step 5 below succeed rather
than fail.

## 4. Suggested Walkthrough (8–10 minutes)

1. **Pick an app.** Switch the dropdown between the three apps. Point out
   the scope chips changing — this is the least-privilege model from the
   architecture doc, visible live.

2. **Encrypt something.** As `payments-svc`, type a value (a fake card
   number works well), optionally set a Data Classification, click Encrypt.
   The result renders as labeled fields, not raw JSON — each one has an
   inline explainer (what `tag_b64` actually proves, why `iv_b64` is random
   per call, etc.). Re-run the exact same plaintext and point out the
   ciphertext is different every time — that's the random IV defeating
   pattern analysis.

3. **Decrypt it back.** The form auto-fills from the last encrypt result.
   Click Decrypt — `owner_app_id` and `decrypted_as` both show `payments-svc`,
   since the same app is reading its own data.

4. **Show the EDEK store.** Scroll to the "Latest EDEK Records" panel — the
   row that was just created shows the owner, KEK version, algorithm,
   encoding, and a truncated wrapped-key preview. Point out apps never see
   this table directly — only the `edek_id` reference.

5. **Show cross-app access via a grant — not a block.** Switch to
   `reporting-app`, paste in the *same* decrypt fields from `payments-svc`'s
   encrypt result, click Decrypt. It **succeeds** — `owner_app_id` shows
   `payments-svc`, `decrypted_as` shows `reporting-app`. This is the seeded
   grant from step 3, made visible: cross-app reads are possible, but only
   because someone explicitly authorized it, not by default.

6. **Show the default-deny side of the same mechanism.** Open the "Cross-App
   Decrypt Grants" panel as `ops-admin` and Revoke the
   `reporting-app → payments-svc` grant. Switch back to `reporting-app` and
   retry the same decrypt — now it's `403 Access denied`. Re-add the grant
   to restore the demo to its starting state.

7. **Show scope enforcement on rotation.** Switch to `payments-svc` and try
   "Rotate Master Key" — disabled, no `rotate` scope. Switch to `ops-admin`
   — enabled, and rotation succeeds. Check the "Simulated HSM State" panel:
   a second key version now exists, marked current.

8. **Prove zero-downtime rotation.** Decrypt the very first ciphertext from
   step 2 again — it still works, even though the master key changed
   underneath it. The EDEK record's `kek_version` column has silently moved
   to the new version.

9. **Block an app, mid-incident — without losing its data.** As `ops-admin`,
   call `POST /admin/apps/status` with `{"app_id": "payments-svc",
   "active": false}` (no UI panel for this yet — curl or the API docs at
   `/docs`). Confirm `payments-svc` can no longer encrypt or decrypt
   anything. Then decrypt `payments-svc`'s existing data as `ops-admin` —
   it still works, because the grant check never depended on the owner
   being active. Restore with `"active": true` when done.

10. **Point at the audit trail.** Every action above — including the block
    in step 9 and the denied attempt in step 6 — is already in the table at
    the bottom, with timestamp, app, status, and a reason code on failures.
    This is the same feed that ships to Splunk in production.

11. **Show what a real consumer's database actually looks like.** Scroll to
    "Consumer Application Table" — this simulates `payments-svc`'s *own*
    schema, not this service's. Create an account with a fake card number.
    The table shows `customer_name`/`email` as plain columns next to a single
    `ciphertext` column — this service never sees this table at all;
    the consumer calls `/encrypt` once, receives one opaque token string, and
    stores it in one `VARCHAR` column. To read back the sensitive value the
    consumer passes the token straight to `/decrypt` — no field reconstruction,
    no juggling. Click Reveal with "Reveal as" set to each of the three apps
    in turn — the exact same grant/deny behaviour from steps 5 and 6 applies
    here too, because revealing a business field is just `/decrypt` under the
    hood. See §6 below for what to consider when designing this table for real.

## 5. Resetting Between Demos

```bash
rm -f demo_hsm.db   # wipes the EDEK store, app registry, grant seed, and consumer table
```
Restart the service and it reseeds the three demo apps and the default grant
automatically.

## 6. Planning Your Own Consumer Table

The demo's consumer table (`app/demo/consumer_store.py`) is deliberately
minimal — it exists to make one point clearly: **this service never stores
ciphertext.** The calling app does, in its own schema. Before building that
schema for real, plan for these:

### One token column per sensitive field

The `/encrypt` response returns a single `ciphertext` — an opaque,
versioned string that bundles every field the service needs to decrypt later:

```
v1.<base64url( version | edek_id | iv | tag | ciphertext )>
```

The client stores this one string and echoes it back verbatim to `/decrypt`.
There are no separate `edek_id`, `iv_b64`, `ciphertext_b64`, `tag_b64`
columns to manage; there is no way to accidentally mix fields from different
encrypt responses.

**Recommended column type by field length:**

| Sensitive field | Typical token length | Column type |
|---|---|---|
| SSN, ZIP, DOB, short codes | ~75–90 chars | `VARCHAR(128)` |
| Account numbers, phone | ~84–100 chars | `VARCHAR(128)` |
| Name, email, address | ~100–420 chars | `VARCHAR(512)` |
| Medical notes / free text | scales with content | `TEXT` |

`VARCHAR(512)` covers every common PII field with room to spare and is a
safe default if you don't want to think about it per-column.

**Decide: one token per row, or one per sensitive field?** If a row has
multiple sensitive fields (`ssn` and `account_number`), encrypting them
together in one call means any decrypt reveals both at once — no way to
grant access to one without the other. Encrypting separately costs one extra
API call and one extra `VARCHAR` column per field, but keeps each field
independently revocable and rotatable. Pick based on whether the two fields
will ever need different access rules.

### Is per-field envelope encryption worth the column cost?

The overhead is **one `VARCHAR` column per sensitive field** — no other
columns, no separate UUID or IV storage. The relative cost:

| Field | Plaintext size | Token overhead | Relative cost |
|---|---|---|---|
| 9-digit SSN | 9 bytes | ~75 chars | ~8× field size |
| 16-digit card number | 16 bytes | ~84 chars | ~5× field size |
| 2 KB clinical note | 2 048 bytes | ~2 800 chars | <40% overhead |

**Worth it:**
- Regulatory mandate (PCI-DSS, HIPAA, data residency).
- Fields that need independent revocability — if `account_status` and
  `routing_number` might ever need different access rules, separate envelopes
  are the only mechanism that can express that.
- Audit requirements that prove field-level access.

**Probably not worth it:**
- Fields with no independent access-control requirement — database TDE covers
  the "stolen disk" threat for free with no application overhead.
- Fields always viewed together on the same row — bundle them into one
  encrypt call and pay the fixed cost once, not once per field.

**The rule of thumb:** group by *access-control boundary*, not by column
count.

### Don't duplicate metadata you don't need

`owner_app_id`, `algorithm`, and `encoding` are all recoverable server-side
via the embedded `edek_id`. Storing them locally too risks drift from the
source of truth. The token is the only thing you need to store.

### Deleting the token row is effective crypto-shredding

Removing the `ciphertext` value makes that data practically
unrecoverable immediately, even though the wrapped DEK still exists
server-side — useful for "right to be forgotten" without coordinating a
delete against this service. Cleaning up the now-orphaned EDEK record
afterward is still good hygiene but not time-critical.

### This table needs its own backup/DR plan

Losing the `ciphertext` column value is just as unrecoverable as
losing the service's own EDEK record — both halves are required to decrypt.
The backup responsibility is owned by the consuming team, not this service.

## 7. Going From Demo to Production

Set `DEMO_MODE=false` (or remove it) and supply:
- `AZURE_KEYVAULT_URL` — Managed HSM endpoint (`managedhsm.azure.net`), KEK wrap/unwrap
- `AZURE_KEYVAULT_SECRET_URL` — regular Key Vault (`vault.azure.net`), CEK secrets + Splunk token
- `AZURE_KEK_NAME`
- `DATABASE_URL` (PostgreSQL)
- `JWT_ISSUER` + `JWT_JWKS_URL` (or `JWT_PUBLIC_KEY_PEM`)
- `SPLUNK_ENABLED=true` + `SPLUNK_HEC_URL` (token pulled from Key Vault at startup)
- `API_V1_PREFIX` should match whatever your Istio (or equivalent) gateway
  routes to this service — defaults to `/api/sensec/hsm/v1`

**CEK rotation (optional but recommended):**
- `DEK_CACHE_ENABLED=true` + `REDIS_URL` (`rediss://` for TLS)
- `CEK_CURRENT_KEY_SECRET_NAME`, `CEK_ALPHA_SECRET_NAME`, `CEK_BETA_SECRET_NAME` — defaults match the rotation service
- Deploy the `cek-rotation-service` Helm chart with `AZURE_KEYVAULT_SECRET_URL` and `REDIS_URL`
- Seed `cek-alpha`, `cek-beta` (base64 32-byte AES-256 keys) and `cek-current-key` (`"alpha"`) in AKV Secrets before the first deploy

**PlainID PBAC (optional):**
- `PBAC_ENABLED=true` + `PLAINID_URL` + `PLAINID_API_KEY_SECRET_NAME`
- Mount `config/pbac_integration.json` as a ConfigMap — see §8 for full details

No application code changes are required — `app/dependencies.py` branches on
`settings.demo_mode` at startup and wires in the real Azure/Postgres clients.

---

## 8. PlainID PBAC Integration

### What it does

When `PBAC_ENABLED=true`, every `POST /encrypt` and `POST /decrypt` request
that carries a non-empty `end_user_id` is checked against PlainID **before the
DEK is touched**.  The result is a single boolean — permit or deny.  If denied,
the service returns `403 Access denied by policy` and writes a `pbac_denied`
audit event.

Service-to-service calls that omit `end_user_id` skip PBAC entirely and are
governed only by the existing app-level grant model.

### Decision flow

```
POST /encrypt or /decrypt  { end_user_id: "alice@corp.com", ... }
  │
  ├─ 1. JWT validated, scopes checked         (existing)
  ├─ 2. Cross-app grant checked               (existing, decrypt only)
  │
  ├─ 3. PBAC cache lookup
  │       key = SHA-256(end_user_id + action + resource)
  │       hit → return cached bool (no network call)
  │       miss → continue
  │
  ├─ 4. POST to PlainID  (e.g. /v2/isPermitted)
  │       payload field names, auth header, resource string
  │       all driven by pbac_integration.json — not hardcoded
  │
  ├─ 5. Response normalized → bool
  │       path driven by response.permitted_path in config
  │       supports any nesting depth via dot-notation
  │
  ├─ 6. Cache result for pbac_cache_ttl_seconds (default 30 s)
  │
  ├─ false → 403 + pbac_denied audit event
  └─ true  → DEK wrap / unwrap → response
```

### Decision cache — why it matters

PlainID adds a network round-trip (typically 5–20 ms).  Without caching, every
decrypt call would pay that cost.  The in-process cache eliminates the round-trip
for repeated decisions by the same user on the same action + resource within the
TTL window.

| Cache property | Value | Configurable? |
|---|---|---|
| Key | SHA-256(`end_user_id \0 action \0 resource`) | No (stable by design) |
| Storage | In-process dict, per pod | No |
| TTL | 30 s default | `PBAC_CACHE_TTL_SECONDS` env var |
| Eviction | Lazy on next read past expiry | — |
| Scope | Per pod — no shared cache | By design; avoids Redis dep for policy decisions |

**TTL trade-off**: shorter TTL → role/policy changes take effect sooner; more
PlainID calls. 30 s is the recommended starting point. If PlainID decisions
change in near-real-time (e.g. incident response), lower it to 5–10 s.

### Fail behaviour

| Setting | PlainID unreachable → | When to use |
|---|---|---|
| `PBAC_FAIL_OPEN=false` (default) | Deny (403) | Security-first; outage blocks operations |
| `PBAC_FAIL_OPEN=true` | Allow (permit) | Availability-first; outage is transparent to users |

### Wire-format config — nothing hardcoded

All PlainID API details live in `config/pbac_integration.json`, mounted as a
ConfigMap (`helm/hsm-encryption-service/templates/pbac-integration-configmap.yaml`).
No code change or image rebuild is needed to adapt to a different PlainID
tenant, version, or field naming.

```json
{
  "endpoint_path": "/v2/isPermitted",
  "auth": {
    "header_name": "Authorization",
    "header_value_template": "Bearer {api_key}"
  },
  "request": {
    "principal_field": "principal",
    "action_field":    "action",
    "resource_field":  "resource",
    "context_field":   "context"
  },
  "response": {
    "permitted_path": "permitted"
  },
  "resource_templates": {
    "encrypt": "hsm:encrypt:{data_classification}",
    "decrypt": "hsm:decrypt:{data_classification}"
  }
}
```

| Config key | What it controls | Example override |
|---|---|---|
| `endpoint_path` | PlainID API path | `/api/authz/v1/check` |
| `auth.header_name` | Auth header name | `X-Api-Key` |
| `auth.header_value_template` | Auth header value | `ApiKey {api_key}` |
| `request.principal_field` | Field carrying the user identity | `userId`, `subject` |
| `request.action_field` | Field carrying the action | `operation`, `verb` |
| `request.resource_field` | Field carrying the resource | `asset`, `object` |
| `request.context_field` | Field carrying extra metadata | `attributes`; set `""` to omit |
| `response.permitted_path` | Dot-path to the boolean in the response | `result.allowed`, `data.decision.isPermit` |
| `resource_templates.encrypt` | Resource string for encrypt calls | `arn:hsm:encrypt:{data_classification}` |
| `resource_templates.decrypt` | Resource string for decrypt calls | `arn:hsm:decrypt:{data_classification}` |

**`permitted_path` dot-notation examples:**

```
"permitted"              → { "permitted": true }
"result.allowed"         → { "result": { "allowed": true } }
"data.decision.isAllow"  → { "data": { "decision": { "isAllow": true } } }
```

Only the keys you want to change from the defaults need to be present in the
file — missing keys fall back to the built-in defaults automatically.

### Helm values quick-start

```yaml
# values.yaml
config:
  pbacEnabled: "true"
  plainidUrl: "https://your-tenant.plainid.io"
  plainidApiKeySecretName: "plainid-api-key"   # name of AKV Secret
  pbacCacheTtlSeconds: "30"
  pbacFailOpen: "false"
  pbacIntegrationConfigPath: "/app/config/pbac_integration.json"

pbacIntegration:
  enabled: true
  endpointPath: "/v2/isPermitted"
  response:
    permittedPath: "permitted"      # ← only override what differs from defaults
```

The `plainid-api-key` secret is fetched from Azure Key Vault Secrets at pod
startup via the Service SPN (`secrets/get`) — it is never stored in a
Kubernetes Secret or ConfigMap.
