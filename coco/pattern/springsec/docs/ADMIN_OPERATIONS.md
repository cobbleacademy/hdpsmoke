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
| `POST /admin/grants` | `grant` | Add a cross-app decrypt grant (`grantee_app_id` may now decrypt `owner_app_id`'s data) |
| `DELETE /admin/grants` | `grant` | Remove a grant |
| `GET /admin/grants` | `grant` | List all grants |
| `POST /admin/rotate-kek` | `rotate` | Trigger KEK rotation (see `RUNBOOK.md`) |
| `GET /admin/health` | none (public) | Vault + DB reachability |

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
- **`app_decrypt_grants.created_at`** — set in `AppDecryptGrant`'s
  constructor, and now exposed via `GET /admin/grants` and the response to
  `POST /admin/grants` (`GrantResponse.createdAt`). This is the field a
  periodic access review ("show every grant older than 90 days") queries
  directly, instead of searching Splunk's `grant_added` events against
  their own retention window. No `updated_at` on this table — grants are
  add/remove only, never mutated in place.

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

Direct SQL against `app_registrations`/`app_decrypt_grants` is sometimes
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

# Add a cross-app decrypt grant
curl -X POST "$BASE/admin/grants" \
  -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin" \
  -H "Content-Type: application/json" \
  -d '{"grantee_app_id": "reporting-app", "owner_app_id": "payments-svc"}'

# List all grants
curl "$BASE/admin/grants" -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin"
```
