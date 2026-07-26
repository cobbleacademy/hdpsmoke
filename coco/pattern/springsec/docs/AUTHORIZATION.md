# Authorization Model & Entra ID Correlation

This documents how `hsm-encryption-service` currently decides *what a caller
is allowed to do*, what role Entra ID (Azure AD) plays in that decision
today, and the recommended path to correlate resource paths with Entra ID
group/role membership if that becomes a requirement. `cek-rotation-service`
is covered separately in §4 — its authorization model is unrelated.

## 1. Current model: Entra ID authenticates, the local DB authorizes

Today, authorization is a two-step process, and only the first step involves
Entra ID:

1. **Authentication (Entra ID's role stops here).**
   [`RsaJwtValidator`](../hsm-encryption-service/src/main/java/com/hsm/encryption/auth/RsaJwtValidator.java)
   validates the JWT's signature (RS256, JWKS or static PEM), issuer, audience,
   and expiry, then extracts the caller's identity. It already normalizes
   Entra ID's built-in `appid` claim into this service's `app_id` field
   (`RsaJwtValidator.java:100-107`) — the codebase was written expecting
   Entra-issued client-credentials tokens. **No `roles` or `groups` claim is
   read today.**

2. **Authorization (entirely local DB, no Entra ID involvement).**
   [`JwtAppIdAuthenticationFilter`](../hsm-encryption-service/src/main/java/com/hsm/encryption/security/JwtAppIdAuthenticationFilter.java)
   takes the `app_id` from step 1 and calls
   [`AppRegistryService.getScopes(appId)`](../hsm-encryption-service/src/main/java/com/hsm/encryption/auth/AppRegistryService.java#L34),
   which looks up the `app_registrations.allowed_scopes` column — a
   comma-separated string (`"encrypt,decrypt"`) stored entirely in this
   service's own database, edited only through this service's own
   `/admin/apps/status` and `/admin/grants` endpoints. Those scopes become
   Spring Security `GrantedAuthority` values, which
   [`SecurityConfig`](../hsm-encryption-service/src/main/java/com/hsm/encryption/security/SecurityConfig.java)
   then matches against `hsm.security.access-rules` in `application.yml` to
   decide whether a given resource path + HTTP method is permitted.

**In short: Entra ID proves *who* the calling app is; a local DB table
decides *what* that app may do.** The resource-path → permission mapping
(`hsm.security.access-rules`) has no connection to Entra ID App Roles or
Security Groups today.

## 2. Recommended correlation mechanism: Entra ID App Roles, not Security Groups

For a service-to-service (client-credentials) scenario like this one, the
Microsoft-recommended mechanism is **App Roles**, not Security Groups:

| | Entra ID App Roles | Entra ID Security Groups |
|---|---|---|
| Declared | Directly on the API's own app registration | Separately, in Entra ID's group management |
| Assigned to a caller | Enterprise Applications → the API → "Users and groups" (or Graph `appRoleAssignments`) | Group membership, then the group assigned to the app |
| Shows up in the token | `roles: [...]` claim, always, in every client-credentials token | `groups: [...]` claim — **only** if the caller belongs to ≤200 groups; past that, Entra ID emits `hasgroups: true` instead and the API must call Microsoft Graph to resolve real membership |
| Extra runtime cost | None — claim is self-contained | Possible extra Graph API call + added latency on overage |
| Purpose-built for | Exactly this: API-level authorization of a calling service principal | Broader RBAC / conditional access / user assignment scenarios |

App Roles avoid the group-overage problem entirely and map directly onto the
`GrantedAuthority` model already in this codebase — no architectural change,
just a different source for the authority list.

## 3. What implementing this would look like (not yet done)

1. **Define App Roles on the HSM API's app registration**, one per existing
   scope: `Encrypt.Execute`, `Decrypt.Execute`, `Rotate.Execute`,
   `Grant.Manage`, `Apps.Manage`.
2. **Assign each calling service principal** (`payments-svc`, `ops-admin`,
   `reporting-app`, ...) the App Roles it needs, via the API's Enterprise
   Application blade or the Graph `appRoleAssignments` API.
3. **Read the `roles` claim in code.** `JwtAppIdAuthenticationFilter` would
   read `claims.get("roles")` from the already-validated JWT — the same
   claims map `app_id` is already pulled from — instead of (or as a fallback
   to) calling `AppRegistryService.getScopes(appId)`.
4. **No change needed** to `hsm.security.access-rules` in `application.yml`
   or to `SecurityConfig`'s `hasAnyAuthority(...)` matching — both only care
   about authority *strings*, not where those strings came from. Naming the
   App Roles identically to the existing scopes (`encrypt`, `decrypt`, ...)
   makes this a drop-in swap.

## 4. Tradeoff: local DB vs. Entra-ID-driven authorization

| | Current (DB-driven) | Entra ID App Roles |
|---|---|---|
| Source of truth | `app_registrations` table, this service's own DB | Entra ID Enterprise App role assignments |
| Who can change permissions | Anyone with access to this service's `/admin/*` endpoints | Anyone with Entra ID admin rights on the app registration |
| Change takes effect | Immediately (cache invalidated on write) | On next token issuance — client-credentials tokens are typically cached ~1h by the caller's MSAL library, so a revoked role can still be honored until the caller's cached token expires |
| Audit trail | This service's own `audit_log` (`grant_added`, `app_status_changed`, ...) | Entra ID's own sign-in / audit logs — a second place to look |
| Cross-app decrypt grants (`app_decrypt_grants`) | Unaffected either way — this is a separate, finer-grained mechanism (which specific *other* app may read *this* app's data) that has no Entra ID equivalent and would stay DB-driven regardless |

Moving to App Roles centralizes governance in Entra ID and removes a table
that must be kept in sync with reality, at the cost of losing the immediate
self-service admin API and moving the audit trail into Entra ID.

## 5. `cek-rotation-service`: not part of this model

`cek-rotation-service` has no inbound HTTP API and no `app_id`/JWT/scope
model at all — there is no resource path to correlate with Entra ID roles or
groups for it. It authenticates *outbound* to Azure Key Vault directly, using
its own managed identity via `DefaultAzureCredentialBuilder`
([`RotationRunner.java:3,47`](../cek-rotation-service/src/main/java/com/hsm/cekrotation/RotationRunner.java#L47)).
Its authorization is Azure RBAC scoped directly to that identity (the
"Rotation SPN" in the architecture diagram): write access to the `cek-alpha`
/ `cek-beta` / `cek-current-key` secrets in Key Vault, and nothing else. This
is a standard Azure resource-level role assignment (Key Vault Secrets
Officer, scoped to those specific secrets), independent of anything in this
document — it isn't a candidate for App Role correlation because it's a
worker, not an API being called by other apps.
