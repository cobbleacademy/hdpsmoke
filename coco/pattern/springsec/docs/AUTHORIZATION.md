# Authorization Model & Entra ID Correlation

This documents how `hsm-core-service` currently decides *what a caller
is allowed to do*, what role Entra ID (Azure AD) plays in that decision
today, and the recommended path to correlate resource paths with Entra ID
group/role membership if that becomes a requirement. `cek-rotation-service`
is covered separately in §4 — its authorization model is unrelated.

## 1. Current model: Entra ID authenticates, the local DB authorizes

Today, authorization is a two-step process, and only the first step involves
Entra ID:

1. **Authentication (Entra ID's role stops here).**
   [`RsaJwtValidator`](../hsm-core-service/src/main/java/com/hsm/encryption/auth/RsaJwtValidator.java)
   validates the JWT's signature (RS256, JWKS or static PEM), issuer, audience,
   and expiry, then extracts the caller's identity. It already normalizes
   Entra ID's built-in `appid` claim into this service's `app_id` field
   (`RsaJwtValidator.java:100-107`) — the codebase was written expecting
   Entra-issued client-credentials tokens. **No `roles` or `groups` claim is
   read today.**

2. **Authorization (entirely local DB, no Entra ID involvement).**
   [`JwtAppIdAuthenticationFilter`](../hsm-core-service/src/main/java/com/hsm/encryption/security/JwtAppIdAuthenticationFilter.java)
   takes the `app_id` from step 1 and calls
   [`AppRegistryService.getScopes(appId)`](../hsm-core-service/src/main/java/com/hsm/encryption/auth/AppRegistryService.java#L34),
   which looks up the `app_registrations.allowed_scopes` column — a
   comma-separated string (`"encrypt,decrypt"`) stored entirely in this
   service's own database, edited only through this service's own
   `/admin/apps/status` and `/admin/grants` endpoints. Those scopes become
   Spring Security `GrantedAuthority` values, which
   [`SecurityConfig`](../hsm-core-service/src/main/java/com/hsm/encryption/security/SecurityConfig.java)
   then matches against `hsm.security.access-rules` in `application.yml` to
   decide whether a given resource path + HTTP method is permitted.

**In short: Entra ID proves *who* the calling app is; a local DB table
decides *what* that app may do.** The resource-path → permission mapping
(`hsm.security.access-rules`) has no connection to Entra ID App Roles or
Security Groups today.

## 1a. Two authentication mechanisms, one authorization step

Step 1 above (authentication) now has two independent paths into the same
step 2 (local-DB authorization) — added for callers (typically legacy apps)
that find Entra ID client-credentials/JWT-renewal machinery operationally
painful but can manage a one-time RSA keypair, the same operational shape as
an SSH key:

| | Entra ID (`RsaJwtValidator`) | Self-issued (`SelfSignedAppKeyJwtValidator`) |
|---|---|---|
| Trust anchor | Entra ID's JWKS (or a static configured PEM) | The caller's own key, registered per-app via `POST /admin/apps/keys` (`app_registrations.signing_public_key_pem`) |
| Who mints the token | Entra ID, via client-credentials flow | The caller itself, locally, immediately before each call |
| "Renewal" | A network round-trip to Entra ID | Pure local computation — re-sign a small JWT with a key already in memory, never a network call |
| Token lifetime | Entra ID's own policy (~1h typical) | Capped server-side at 5 minutes (`SelfSignedAppKeyJwtValidator.MAX_TTL`) regardless of what the token claims — the caller fully controls its own claims, unlike an Entra-ID-issued token, so this can't be left to the token alone |
| `iss` claim | The configured Entra ID issuer URL | The caller's own `app_id` |

**Routing between the two is automatic, not a deployment-wide switch.**
`SelfIssuedRoutingJwtValidator` peeks a token's *unverified* `iss` claim
before trusting anything in it: `iss` matching the configured Entra ID
issuer routes to `RsaJwtValidator`; anything else (including every demo-mode
`MockJwtValidator` literal token, which isn't JWT-shaped at all and fails
the peek outright) routes to `SelfSignedAppKeyJwtValidator`, which then
verifies the signature against whichever app the `iss`/`sub` claims to be —
still fully untrusted until that verification succeeds. Both mechanisms
feed the exact same `Map.of("sub", appId, "app_id", appId)` shape into step
2, so nothing downstream (scope resolution, `AuthenticatedCaller`) needs to
know or care which path a given request took.

**One keypair or two, per app.** `signing_public_key_pem` is independent of
`public_key_pem` (the pre-existing DEK-transport-wrap key `/dek/issue` and
`/dek/unwrap` use) — an app can register a dedicated signing key, or leave
it unset and let `SelfSignedAppKeyJwtValidator` fall back to the encryption
key for signature verification too (the legacy one-keypair switch — see
`AppRegistryService.getSigningPublicKey`). Modern callers should register
both; the fallback exists specifically for callers that would rather manage
one keypair than two.

## 1b. Considered alternative: mutual TLS (mTLS) — not built

Before settling on the self-issued JWT design in §1a, mutual TLS was
considered as the mechanism for the same "legacy caller, one-time keypair"
population and rejected in favor of it. Recorded here for anyone revisiting
this decision later — nothing below is implemented.

**What it would replace.** mTLS establishes caller identity at the TLS
handshake itself (the client also presents a certificate, which the server
verifies), instead of at the application layer via a bearer token. Like
§1a's self-issued JWT, it would only ever replace *authentication* — step 2
(local-DB authorization via `AppRegistryService.getScopes`) is unaffected
either way.

**Phase 0 — provisioning, before any calls.** A bare keypair isn't enough;
TLS needs an X.509 certificate binding the public key to an identity (e.g.
`CN=payments-svc`). Two ways to get one: **self-signed** (the app signs its
own cert — closest analog to today's `POST /admin/apps/keys` model, just a
certificate instead of a bare PEM public key) or **CA-issued** (the app
submits a CSR to an internal CA — this repo's Istio mesh has one built in —
which issues a time-limited signed cert). Either way this needs a new admin
endpoint (e.g. `POST /admin/apps/mtls-cert`) registering the app's cert or
public-key fingerprint — none of this exists today.

**Phase 1 — the handshake.** Client opens an HTTPS connection; because the
server requires client certs (Istio `PeerAuthentication: STRICT`, or Spring
Boot's `ssl.client-auth: need` if terminated in the JVM), the client
presents its Phase-0 cert too. If verification fails, **the connection never
completes — the request never reaches any Java code at all**, unlike a bad
JWT, which still reaches `JwtAppIdAuthenticationFilter` and gets a clean
401.

**Phase 2 — resolving who's calling.** A certificate doesn't carry an
`app_id` claim the way a JWT does, so something has to extract identity from
it: either Istio terminates the handshake and forwards identity via a header
(Envoy's `X-Forwarded-Client-Cert`), or the app pulls the peer certificate
off the servlet request directly and a new filter reads the CN as `app_id`.
Either way this is new code — nothing reuses `JwtAppIdAuthenticationFilter`
as-is.

**Phase 3 — actual single/batch/bulk calls.** Request/response shapes for
`/encrypt`, `/encrypt/batch`, `/decrypt`, `/decrypt/batch` would be
unchanged — only the `Authorization: Bearer ...` header disappears, since
auth already happened when the connection opened. `/dek/issue`/`/dek/unwrap`
(bulk) are the same, **but with one important clarification: mTLS only
replaces the authentication keypair, not the separate DEK-transport keypair**
(`encryption_public_key_pem`, `TransportWrapper`) that wraps the raw DEK in
the response body. A bulk caller under mTLS would still need to register and
hold that second keypair — mTLS gets a caller in the door, it doesn't touch
DEK wrapping. Single/batch calls never need that second keypair at all,
regardless of auth mechanism, since the server does the AES-GCM itself.

**Phase 4 — renewal, and why this wasn't the pick.** Self-signed certs have
no external expiry, so "renewal" is a manual re-registration — same
operational shape as rotating the §1a signing key, just with X.509
parsing/generation overhead on top. CA-issued certs get real expiry and
revocation (the actual security benefit of this route), but then *renewal
before expiry* is back on the caller — Istio automates this for workloads
already inside the mesh, but the population this was aimed at is explicitly
callers that aren't comfortably inside that automation, so the CA route
reintroduces almost exactly the "renewal is operationally painful" problem
§1a was built to eliminate.

**One place mTLS is genuinely better, for balance:** the handshake happens
once per TCP connection, not once per request, so a client hammering the API
with keep-alive pays the auth cost once per connection rather than attaching
a JWT to every request — a real efficiency edge for high-volume callers.
That didn't outweigh the added certificate/CA machinery and the renewal-burden
fork above for the target audience, but it's worth revisiting if a future
caller's traffic pattern makes that efficiency matter more than setup
simplicity.

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
