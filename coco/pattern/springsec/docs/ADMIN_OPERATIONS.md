# Admin Operations Without a UI

There is no production admin UI — only demo mode's UI panels (grants, app
status via curl instructions in `DEMO.md`), which are gated off entirely by
`demo-mode=true` and don't exist in a real deployment. This documents what
exists today, the gap in it, and how to operate it safely in the meantime.

## What exists

All under `${API_V1_PREFIX}/admin/...` (default `/api/sensec/hsm/v1/admin`),
requiring a bearer JWT + `X-App-ID` for an app holding the matching
authority (see `hsm.security.access-rules` in `application.yml`):

| Endpoint | Authority | What it does |
|---|---|---|
| `POST /admin/apps/status` | `manage_apps` | Activate/deactivate an **existing** app_id |
| `POST /admin/apps/keys` | `provision_app_keys` | Provision/rotate an app's encryption and/or signing public key (see below) |
| `POST /admin/apps/mtls-cert` | `provision_app_keys` | Provision/rotate an app's mTLS client certificate (see below) |
| `POST /admin/grants` | `grant` | Add a coarse cross-app grant (`grantee_app_id` may now `scope` — `encrypt` or `decrypt` — against *any* of `owner_app_id`'s DEKs; see `AUTHORIZATION.md` §1d) |
| `DELETE /admin/grants` | `grant` | Remove a coarse grant |
| `GET /admin/grants` | `grant` | List all coarse grants |
| `POST /admin/dek-grants` | `grant` | Add a fine-grained grant, scoped to one `dek_name` of `owner_app_id`'s |
| `DELETE /admin/dek-grants` | `grant` | Remove a fine-grained grant |
| `GET /admin/dek-grants` | `grant` | List all fine-grained grants |
| `GET /admin/edek/{edekId}` | `grant` | Read-only ownership/metadata lookup for one EDEK — `owner_app_id`, `dek_name`, `data_classification`, etc. No key material or fingerprint (see below) |
| `POST /admin/rotate-kek` | `rotate` | Trigger routine KEK rotation, grouped by every distinct KEK actually in use (see `CACHING_AND_ROTATION.md`) |
| `POST /admin/rekey-kek` | `rotate` | Manually move every current EDEK from one KEK to another (compromise response, key decommissioning — not part of any schedule) |
| `POST /admin/rekey-kek/revert` | `rotate` | Undo the most recent rekey into a given KEK (single-level undo) |
| `GET /admin/health` | none (public) | Vault + DB reachability |

## Provisioning an app's public key(s) — `POST /admin/apps/keys`

Replaces the earlier no-admin-endpoint approach (a direct SQL `UPDATE` against
`app_registrations.public_key_pem`/`signing_public_key_pem`) with a real
endpoint — same "prefer the admin API over direct SQL" reasoning as the rest
of this doc, now closed for this specific gap too. `encryption_public_key_pem`
is the DEK-transport-wrap key (`TransportWrapper`, used by `/dek/issue` and
`/dek/unwrap`); `signing_public_key_pem` is optional and used only by
`SelfSignedAppKeyJwtValidator` to verify that app's self-issued bearer JWTs
(see `AUTHORIZATION.md`'s "Two authentication mechanisms" section) — an app
with only the encryption key registered runs on the legacy one-keypair
fallback (that same key verifies its signing too). Either field may be
omitted to leave that key unchanged; at least one is required. Both PEMs are
parsed and validated at write time (422 on a malformed key), not left to fail
later at first use, and the target `app_id` must already exist (404
otherwise — same "onboarding is a versioned migration, not a live API"
stance as everywhere else in this doc). A separate scope
(`provision_app_keys`, not `manage_apps`) gates this specifically because
this key becomes part of the authentication trust boundary once
`SelfSignedAppKeyJwtValidator` is in play, not just DEK confidentiality — a
bigger blast radius than the active/inactive toggle above.

```bash
# Provision both keys for a new self-signed-JWT-capable caller
curl -X POST "$BASE/admin/apps/keys" \
  -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin" \
  -H "Content-Type: application/json" \
  -d '{
    "app_id": "payments-svc",
    "encryption_public_key_pem": "-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----\n",
    "signing_public_key_pem": "-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----\n"
  }'

# Rotate just the signing key later, leaving the encryption key untouched
curl -X POST "$BASE/admin/apps/keys" \
  -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin" \
  -H "Content-Type: application/json" \
  -d '{"app_id": "payments-svc", "signing_public_key_pem": "-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----\n"}'
```

## Provisioning an app's mTLS client certificate — `POST /admin/apps/mtls-cert`

The fourth, optional authentication mechanism — see `AUTHORIZATION.md`'s
"mTLS as a fourth, optional authentication mechanism" for the full design.
`cert_pem` is the full PEM-encoded X.509 certificate, not just its public
key; only its SHA-256 fingerprint is stored (computed here, at write time)
— the certificate itself is never kept, since identity resolution only ever
needs to compare fingerprints against whatever certificate is presented at
the TLS handshake. Same scope as `POST /admin/apps/keys`
(`provision_app_keys`, not `manage_apps`): this is another
authentication-trust-boundary credential, not a new blast-radius category.
The target `app_id` must already exist (404 otherwise), same convention as
every other provisioning endpoint here. Requires
`hsm.security.mtls-enabled=true` on the server for the certificate to
actually be usable for authentication afterward — provisioning one while
mTLS is disabled succeeds but has no effect until it's turned on.

```bash
# Register a self-signed client certificate for an app
curl -X POST "$BASE/admin/apps/mtls-cert" \
  -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin" \
  -H "Content-Type: application/json" \
  -d '{"app_id": "payments-svc", "cert_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n"}'
# -> {"app_id":"payments-svc","fingerprint":"4cc8c5b3...","updated_at":"..."}
```

## Resolving a cross-app decrypt denial — `GET /admin/edek/{edekId}`

The most common support ticket in this system's shape is exactly the one that
motivated this endpoint: an app calls `/decrypt`, gets `403 {"detail":"Access
denied"}`, and the only fact support actually needs to resolve it is *who
owns the data*. Before this endpoint, that meant either a direct DB query
against `edek_records` or digging through Splunk for the `no_grant_for_owner`
audit event's `owner_app_id` field — both work, but both require DB/Splunk
access support may not have, and both are slower than they need to be for
what is otherwise a one-line answer.

`GET /admin/edek/{edekId}` returns exactly that one-line answer and nothing
riskier:

```bash
curl "$BASE/admin/edek/32dacf35-6fe7-45cb-b120-8d24bbe821b7" \
  -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin"
# -> {"edek_id":"32dacf35-...","owner_app_id":"payments-svc","dek_name":"customers.support-lookup-test",
#     "data_classification":null,"algorithm":"AES-256-GCM","encoding":"utf8","kek_name":"hsm-master-kek",
#     "kek_version":"demo-v2","rotation_status":"CURRENT","current_dek_name":"customers.support-lookup-test",
#     "created_at":"...","rotated_at":null}
```

**Support workflow, end to end:**
1. The calling app's error (or its own logs) names the `edek_id` from the
   ciphertext token it tried to decrypt. If it doesn't have that handy, the
   `no_grant_for_owner` audit event carries the same `edek_id` and already
   includes `owner_app_id` directly — either path gets you here.
2. `GET /admin/edek/{edekId}` → read `owner_app_id` (who to grant *from*) and
   `dek_name` (if you want a fine-grained grant instead of coarse).
3. `POST /admin/grants` (coarse) or `POST /admin/dek-grants` (scoped to that
   one `dek_name`) with the requesting app as `grantee_app_id` and the
   looked-up app as `owner_app_id`.
4. Done — no restart needed, since `POST /admin/grants`/`/admin/dek-grants`
   update `AppRegistryService`'s cache in-process, unlike a direct SQL write.

**Same `grant` scope as the grants endpoints, deliberately.** Whoever can
already see and manage cross-app grants is exactly who needs this to decide
what to grant — a separate scope would just mean provisioning another
permission for the same people. **Deliberately excludes `edek_blob` (the
wrapped key material) and `fingerprint`** — this endpoint can only ever
answer "who owns it," never "what does it decrypt to," which is what makes
it safe to hand to support tooling that must never come near plaintext or
anything that could help forge a match against one.

## Timestamps on `app_registrations` and `app_decrypt_grants` — implemented

Added via `V5__add_timestamps_to_access_tables.sql`, closing the gap that
used to exist here (neither table had any timestamp column at all —
confirmed against `V1__initial_schema.sql` before this was added):

- **`app_registrations.created_at` / `updated_at`** — `AppRegistration`
  sets `createdAt` in its constructor and bumps `updatedAt` in both
  `setActive` and `setAllowedScopes`. Not yet exposed via an API response
  (no `GET /admin/apps` list endpoint exists — see the gap noted below),
  but queryable directly, which is what `RUNBOOK.md`'s break-glass
  diagnosis actually needs ("was this row recently changed?").
- **`app_grants.created_at` / `app_dek_grants.created_at`** — set in
  `AppGrant`'s / `AppDekGrant`'s constructor, and exposed via
  `GET /admin/grants` / `GET /admin/dek-grants` and the response to
  `POST /admin/grants` / `POST /admin/dek-grants` (`GrantResponse.createdAt`
  / `DekGrantResponse.createdAt`). This is the field a periodic access
  review ("show every grant older than 90 days") queries directly, instead
  of searching Splunk's `grant_added`/`dek_grant_added` events against their
  own retention window. No `updated_at` on either table — grants are
  add/remove only, never mutated in place. (These two tables replaced the
  earlier decrypt-only, coarse-only `app_decrypt_grants` — see
  `AUTHORIZATION.md` §1d.)

Both columns are nullable with no backfill (same pattern as
`V2__add_fingerprint_to_edek_records.sql`): rows from before this migration
simply have `NULL` timestamps going forward.

## The gap: no endpoint creates a new app_registration

Notice what's missing from the table above: there is no `POST /admin/apps`
to *create* a new app_id's row in the first place. `/admin/apps/status` only
toggles `active` on a row that already exists. Onboarding a brand-new
calling app has to happen out-of-band — see `APP_ONBOARDING.md` for the
concrete procedure (a versioned migration, not a live API call).

This is arguably correct on purpose — creating a new app_registration is a
trust decision (what scopes does this app get?) that deserves a
version-controlled, reviewed change, not a live mutable API call a
compromised ops-admin token could abuse to silently mint a new
fully-privileged app. Keep it this way rather than "fixing" it by adding a
create endpoint, unless there's a strong operational reason to automate
onboarding at higher volume than manual migrations can support.

## Prefer the admin API over direct SQL for security-relevant changes

Direct SQL against `app_registrations`/`app_grants`/`app_dek_grants` is sometimes
unavoidable — see `RUNBOOK.md`'s total-lockout section, the one case where
the API itself is what's broken, so the DB is the only path left. Outside
that case, don't reach for it as a shortcut for something an admin endpoint
can already do, even when it feels like "just a data change." Two real,
non-cosmetic reasons:

- **Cache staleness.** `AppRegistryService` caches scopes for performance;
  the existing admin endpoints (`/admin/apps/status`, `/admin/grants`)
  invalidate that cache on write, so a change takes effect immediately. A
  raw SQL `UPDATE` bypasses that invalidation entirely — the row is correct
  in the database, but the running service can keep honoring the old value
  until the cache entry expires. That's the opposite of what you want for
  anything time-sensitive, like cutting off access right after an incident
  or the moment an onboarding/bulk window closes.
- **No audit trail.** Every admin endpoint fires an audit event
  (`app_status_changed`, `grant_added`, `grant_removed`, ...) recording who
  changed what and when. A raw SQL statement leaves no equivalent record
  inside this service — only whatever your DB's own query log happens to
  capture, if anything.

This is exactly why the still-missing scope-revocation capability discussed
in `BULK_OPERATIONS.md` (revoking `dek_issue`/`dek_unwrap` once a Tier-3
onboarding/de-boarding window closes) should be built as an admin endpoint
when Tier 3 is approved, not operated as an ad hoc SQL step — even though
it would be a trivially easy `UPDATE` to write by hand.

## How to actually run these day-to-day

Right now: hand-crafted HTTP calls (curl, Postman, an internal API client)
using an ops-admin-scoped token. Functional, but:

- No input validation beyond what the API itself does (e.g., nothing stops
  a typo'd `owner_app_id` from silently granting access to a nonexistent
  app — it'll just never match any real decrypt).
- No approval workflow — granting cross-app decrypt access is exactly the
  kind of change that should have a second set of eyes before it takes
  effect, and today nothing enforces that beyond human discipline.
- No visibility beyond tailing the audit log (`grant_added`,
  `grant_removed`, `app_status_changed` events) — there's no "list of
  pending/recent admin actions" view.

**Recommended, in order of effort:**

1. **A thin internal CLI** wrapping these three endpoints with input
   validation and a confirmation prompt before any mutating call. Cheap,
   removes the "hand-typed curl JSON" failure mode, no architecture change.
2. **Route grant changes through your existing access-request/ticketing
   system** (ServiceNow, Jira, whatever your org already uses for access
   requests) rather than a bearer token someone has sitting in a terminal.
   The API call becomes the *automated fulfillment step* of an approved
   ticket, not something anyone with an ops-admin token can fire directly.
3. **A real admin UI**, internal-network-only (VPN/private ingress, not the
   same public surface as the demo UI), if operation volume ever justifies
   building one. Not justified today given how infrequent these operations
   likely are — reassess if that changes.

### Example calls (reference)

```bash
# Deactivate an app (incident response)
curl -X POST "$BASE/admin/apps/status" \
  -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin" \
  -H "Content-Type: application/json" \
  -d '{"app_id": "compromised-app", "active": false}'

# Add a coarse cross-app grant -- scope is required, no default (see AUTHORIZATION.md #1d)
curl -X POST "$BASE/admin/grants" \
  -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin" \
  -H "Content-Type: application/json" \
  -d '{"grantee_app_id": "reporting-app", "owner_app_id": "payments-svc", "scope": "decrypt"}'

# List all coarse grants
curl "$BASE/admin/grants" -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin"

# Add a fine-grained grant, scoped to one dek_name only
curl -X POST "$BASE/admin/dek-grants" \
  -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin" \
  -H "Content-Type: application/json" \
  -d '{"grantee_app_id": "reporting-app", "owner_app_id": "payments-svc", "dek_name": "customers.ssn", "scope": "decrypt"}'

# List all fine-grained grants
curl "$BASE/admin/dek-grants" -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin"

# Look up who owns an EDEK, to resolve a cross-app decrypt denial
curl "$BASE/admin/edek/32dacf35-6fe7-45cb-b120-8d24bbe821b7" \
  -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin"
```
