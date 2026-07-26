# Onboarding a New Calling App

Step-by-step procedure for adding a new `app_id` that can call this service.
This is deliberately **not** a live API call (see `ADMIN_OPERATIONS.md` for
why) — it's a version-controlled change, same review bar as any schema
migration.

## Prerequisites checklist

- [ ] Confirm the new app has an Entra ID (Azure AD) app registration /
      service principal, configured for client-credentials flow against this
      service's expected audience/issuer (`JWT_AUDIENCE`, `JWT_ISSUER`).
- [ ] Decide the app's `app_id` string — must be stable, must match what
      the app's JWT will carry as the `app_id` claim (or Entra ID's built-in
      `appid` claim, which `RsaJwtValidator` accepts as equivalent).
- [ ] Decide the minimum scope set it actually needs (`encrypt`, `decrypt`,
      `rotate`, `grant`, `manage_apps`, `governance`) — least privilege;
      don't default to broad scopes because it's convenient.
- [ ] Decide whether it needs any cross-app decrypt grants on day one, or
      whether that's a separate follow-up change once the app is live.

## Step 1 — Add the app_registrations row via migration

Add a new Flyway migration (next `V{n}__...sql` in
`hsm-core-service/src/main/resources/db/migration/`), **not** a direct
`INSERT` run by hand against production:

```sql
-- V{n}__add_app_{new_app_id}.sql
INSERT INTO ${access_schema}.app_registrations (app_id, allowed_scopes, description, active, created_at, updated_at)
VALUES ('new-app-id', 'encrypt,decrypt', 'One-line description of what this app does and who owns it', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
```

Schema reference (`V1__initial_schema.sql` + `V5__add_timestamps_to_access_tables.sql`):

| Column | Notes |
|---|---|
| `app_id` | `VARCHAR(128)`, primary key — must exactly match the JWT's `app_id`/`appid` claim |
| `allowed_scopes` | `VARCHAR(512)`, comma-separated (e.g. `"encrypt,decrypt"`) — parsed by `AppRegistryService.getScopes` |
| `description` | `VARCHAR(512)` — put the owning team and a contact here, not just what it does |
| `active` | `BOOLEAN` — `TRUE` to onboard live; can be flipped via `/admin/apps/status` later without a new migration |
| `created_at` | Nullable, no default — this SQL-based onboarding path must set it explicitly (`CURRENT_TIMESTAMP`), unlike rows created through `AppRegistration`'s Java constructor (e.g. `DemoSeedInitializer`), which sets it automatically |
| `updated_at` | Same at insert time; bumped automatically afterward whenever `active` or `allowed_scopes` changes via the running service (`AppRegistration.setActive`/`setAllowedScopes`) |

If it needs an initial cross-app grant, add it in the same migration:

```sql
INSERT INTO ${access_schema}.app_decrypt_grants (grantee_app_id, owner_app_id, created_at)
VALUES ('new-app-id', 'some-owner-app', CURRENT_TIMESTAMP);
```

## Step 2 — If using Entra ID App Roles (see `AUTHORIZATION.md`)

If you've adopted the App Roles path described in `AUTHORIZATION.md` rather
than (or alongside) the DB-driven scopes above: assign the corresponding
App Roles (`Encrypt.Execute`, `Decrypt.Execute`, ...) to the new app's
service principal via Enterprise Applications → this API → "Users and
groups", matching the scopes granted in Step 1. Today, the DB row is still
the actual source of truth for authorization — App Roles aren't wired in yet
— so this step is a no-op until that migration happens, but worth doing now
if App Roles are on the near-term roadmap so onboarding doesn't need to be
revisited per-app later.

## Step 3 — Deploy the migration

Standard deploy — Flyway runs pending migrations at service startup
(`spring.flyway.enabled: true`). No code change, no image rebuild required
for the DB row itself.

## Step 4 — Verify

```bash
# Confirm the app can authenticate and encrypt with its granted scopes
curl -X POST "$BASE/encrypt" \
  -H "Authorization: Bearer $NEW_APP_TOKEN" -H "X-App-ID: new-app-id" \
  -H "Content-Type: application/json" \
  -d '{"plaintext": "onboarding smoke test"}'
# Expect 201

# Confirm it's denied a scope it was NOT granted
curl -X POST "$BASE/admin/rotate-kek" \
  -H "Authorization: Bearer $NEW_APP_TOKEN" -H "X-App-ID: new-app-id"
# Expect 403 if 'rotate' wasn't granted -- confirms least-privilege actually took effect,
# not just that the app can authenticate at all
```

- [ ] Encrypt succeeds with a granted scope
- [ ] A non-granted scope is correctly denied (403, not 500 — confirms the
      access-rules mapping is exercised for this app, not just "some app
      works")
- [ ] If a grant was seeded, cross-app decrypt succeeds for the grantee
- [ ] Audit log shows the expected events (`encrypt`/`decrypt` success, no
      unexpected `access_denied` entries for expected-allowed calls)

## Deactivating or removing an app later

Deactivate via `/admin/apps/status` (`active: false`) — this is a live API
call, appropriate for it since it's reversible and time-sensitive (incident
response, offboarding). It immediately fails every request from that app_id
(`AppRegistryService.setActive` invalidates the scope cache in the same
transaction, so there's no stale-cache window).

Fully removing the row (vs. just deactivating) should go through the same
migration process as adding it — don't hand-delete rows in production.
