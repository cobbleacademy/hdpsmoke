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
