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
| `iss` claim | One of the configured Entra ID issuer URLs (`JWT_ISSUER` accepts a comma-separated list -- e.g. a v1.0- and a v2.0-endpoint issuer for the same app registration) | The caller's own `app_id` |

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

## 1b. mTLS as a fourth, optional authentication mechanism

mTLS was first considered as a *replacement* for the self-issued JWT design
in §1a and rejected in that shape — the history below (originally written
when nothing here was built) explains why. It was later revisited, not as a
replacement but as a genuinely **optional fourth mechanism** apps can adopt
independently of the other three, and built on that basis. Like §1a, it only
ever replaces *authentication* — step 2 (local-DB authorization via
`AppRegistryService.getScopes`) is identical regardless of which of the four
mechanisms got a caller in the door.

### What's actually built

- **Fingerprint-pinned, not PKI.** `app_registrations.mtls_cert_fingerprint`
  stores the SHA-256 fingerprint of one X.509 certificate per app
  ([`V13__add_mtls_cert_fingerprint_to_app_registrations.sql`](../hsm-core-service/src/main/resources/db/migration/V13__add_mtls_cert_fingerprint_to_app_registrations.sql)),
  provisioned via `POST /admin/apps/mtls-cert`
  ([`AdminController.setMtlsCert`](../hsm-core-service/src/main/java/com/hsm/core/web/AdminController.java)) —
  the same "provision a credential over the admin API, validated at write
  time" shape as `POST /admin/apps/keys`, just a certificate instead of a
  bare public key. Identity is decided by comparing the fingerprint of
  whatever certificate was *actually presented* at the TLS handshake against
  this column — not chain-of-trust validation, since a self-signed cert has
  no CA to validate a chain against. This is the same trust shape as SSH
  host-key pinning, chosen deliberately over the CA-issued route Phase 4
  below explains was rejected for the renewal-burden reason.
- **The TLS layer accepts any client cert; the filter decides trust.**
  [`MtlsServerConfig`](../hsm-core-service/src/main/java/com/hsm/core/security/MtlsServerConfig.java)
  configures the embedded Tomcat connector with `certificateVerification:
  optional` (Tomcat's "want", not "need") and
  [`PermissiveClientTrustManager`](../hsm-core-service/src/main/java/com/hsm/core/security/PermissiveClientTrustManager.java)
  (accepts any cert at the handshake -- self-signed certs have no CA to
  validate). Real validation happens afterward in
  [`MtlsAppIdAuthenticationFilter`](../hsm-core-service/src/main/java/com/hsm/core/security/MtlsAppIdAuthenticationFilter.java),
  which resolves the caller from the `X-App-ID` header (same header every
  other mechanism already requires), looks up that app's registered
  fingerprint, and compares it against the certificate's actual fingerprint.
  A request with no client certificate at all skips this filter entirely and
  falls through to `JwtAppIdAuthenticationFilter` unchanged -- **this is what
  makes mTLS genuinely optional**, not a breaking change to any existing
  caller. A request *with* a certificate that doesn't match is rejected
  outright (401) here, not silently passed through to the JWT filter --
  presenting a certificate is an explicit choice to authenticate via mTLS.
- **Off by default, cluster-wide.** `hsm.security.mtls-enabled` (env
  `MTLS_ENABLED`, default `false`) gates all of the above -- when false,
  `MtlsServerConfig` and `MtlsAppIdAuthenticationFilter` aren't even
  registered as beans, so the connector and filter chain are exactly what
  they were before this existed.
- **Client side:** a fourth `SvcConfig.AuthMode` (`STATIC`, `AZURE_AD`,
  `SELF_SIGNED_JWT`, `MTLS`) in `hsm-crypto-client`. `HsmCryptoClient.Builder.mtls(certPem,
  keyPem)` builds an in-memory PKCS12 keystore from the same bare PEM
  cert/key pair every other credential in this module uses (see
  [`MtlsSupport`](../hsm-crypto-client/src/main/java/com/hsm/client/svc/MtlsSupport.java)),
  never written to disk. **No `Authorization` header is sent at all** in
  this mode -- identity was already established at the handshake.

### Worked example -- create, use, validate

```bash
# 1. Generate a self-signed client certificate (the caller's own identity)
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out client-key.pem
openssl req -new -x509 -key client-key.pem -out client-cert.pem -days 365 -subj "/CN=payments-svc"

# 2. Register it -- fingerprint computed and stored server-side, cert itself is not
curl -X POST "$BASE/admin/apps/mtls-cert" \
  -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin" \
  -H "Content-Type: application/json" \
  -d '{"app_id": "payments-svc", "cert_pem": "'"$(cat client-cert.pem)"'"}'
# -> {"app_id":"payments-svc","fingerprint":"4cc8c5b3...","updated_at":"..."}

# 3. Use it -- no Authorization header, the certificate IS the credential
curl --cert client-cert.pem --key client-key.pem \
  -H "X-App-ID: payments-svc" -H "Content-Type: application/json" \
  -X POST "$BASE/encrypt" -d '{"plaintext":"hello","data_classification":"pii"}'
# -> 201 {"ciphertext":"v1...","status":"success",...}

# 4. Validate the negative case -- a DIFFERENT, unregistered cert must be rejected
curl --cert wrong-cert.pem --key wrong-key.pem \
  -H "X-App-ID: payments-svc" -X POST "$BASE/encrypt" -d '...'
# -> 401 {"detail":"Client certificate not recognized for this app_id"}

# 5. Validate "optional" -- the SAME app_id, with NO certificate, using its
#    existing token-based auth, must still work unchanged
curl -H "Authorization: Bearer $EXISTING_TOKEN" -H "X-App-ID: payments-svc" \
  -X POST "$BASE/encrypt" -d '...'
# -> 201, exactly as before mTLS existed
```
All five steps above were run against a real instance during this feature's
own verification, including the client-side Java path
(`HsmCryptoClient.builder().mtls(certPem, keyPem)`) — not just curl.

For `hsm-spark-adapter`, the equivalent is `spark.hsm.authMode=MTLS` plus
`spark.hsm.mtlsCertPath`/`spark.hsm.mtlsKeyPath` (Secret-mounted file paths,
same convention as `privateKeyPath`/`signingKeyPath`) — see
[`SPARK_ADAPTER.md`](SPARK_ADAPTER.md).

### Why not one of the other three, once mTLS is available?

Technically, an app fully migrated to mTLS needs none of `STATIC`/
`AZURE_AD`/`SELF_SIGNED_JWT` — `AppRegistryService.getScopes(appId)` is a
server-side DB lookup keyed by `app_id`, not something read out of token
claims, so a certificate-derived `app_id` authorizes exactly the same way a
JWT-derived one does. The other three mechanisms stay valuable for
**coexistence**, not because mTLS depends on them: callers not yet
migrated, deployments where TLS terminates somewhere mTLS can't reach, and
callers who'd rather not manage a certificate at all. The DEK-transport
keypair (`encryption_public_key_pem`) is unaffected either way regardless of
which of the four authenticates a caller — see Phase 3 below, unchanged by
this build.

### History: why the original (CA-issued, replacement) design was rejected

**Phase 0 — provisioning, before any calls.** A bare keypair isn't enough;
TLS needs an X.509 certificate binding the public key to an identity (e.g.
`CN=payments-svc`). Two ways to get one: **self-signed** (the app signs its
own cert — the route actually built, above) or **CA-issued** (the app
submits a CSR to an internal CA — this repo's Istio mesh has one built in —
which issues a time-limited signed cert — the route considered and not
taken, for the renewal reason in Phase 4).

**Phase 1 — the handshake.** The route originally considered would have
made the server *require* a client cert (Istio `PeerAuthentication: STRICT`,
or `ssl.client-auth: need`) — connection failure for a bad cert, before any
Java code runs, unlike a bad JWT (which still reaches
`JwtAppIdAuthenticationFilter` for a clean 401). The route actually built
uses `client-auth: want` instead specifically so a missing or non-matching
certificate can fall through to (or fail through to) the existing filter
chain in Java, which is what "optional" requires.

**Phase 2 — resolving who's calling.** A certificate doesn't carry an
`app_id` claim the way a JWT does, so something has to extract identity from
it — this was correctly anticipated (Istio forwarding identity via a
header, or a new filter reading the cert directly); the route actually
built does the latter (`MtlsAppIdAuthenticationFilter`), keyed off
`X-App-ID` plus fingerprint comparison rather than the certificate's CN
alone.

**Phase 3 — actual single/batch/bulk calls.** Request/response shapes for
`/encrypt`, `/encrypt/batch`, `/decrypt`, `/decrypt/batch` are unchanged —
only the `Authorization: Bearer ...` header disappears, since auth already
happened when the connection opened. `/dek/issue`/`/dek/unwrap` (bulk) are
the same, **but with one important clarification that still holds: mTLS
only replaces the authentication keypair, not the separate DEK-transport
keypair** (`encryption_public_key_pem`, `TransportWrapper`) that wraps the
raw DEK in the response body. A bulk caller under mTLS still registers and
holds that second keypair — mTLS gets a caller in the door, it doesn't
touch DEK wrapping. Single/batch calls never need that second keypair at
all, regardless of auth mechanism, since the server does the AES-GCM
itself.

**Phase 4 — renewal, and why CA-issued certs specifically weren't the
pick.** Self-signed certs (the route built) have no external expiry, so
"renewal" is a manual re-registration — same operational shape as rotating
the §1a signing key, just with X.509 parsing/generation overhead on top.
CA-issued certs get real expiry and revocation (a real security benefit),
but then *renewal before expiry* is back on the caller — Istio automates
this for workloads already inside the mesh, but the population this was
aimed at is explicitly callers that aren't comfortably inside that
automation, so the CA route would have reintroduced almost exactly the
"renewal is operationally painful" problem §1a was built to eliminate. This
reasoning is why the fingerprint-pinned, self-signed route was built instead
of the CA-issued one — not a reason to skip mTLS altogether, which is the
part that changed between the original write-up and this one.

**One place mTLS is genuinely better, for balance:** the handshake happens
once per TCP connection, not once per request, so a client hammering the API
with keep-alive pays the auth cost once per connection rather than attaching
a JWT to every request — a real efficiency edge for high-volume callers.

## 1c. mTLS does not address client-side DEK memory exposure

Raised in review: a caller-side JVM (`hsm-bulk-client`, `hsm-crypto-client`,
or a Spark executor running `hsm-spark-adapter`) is compromised and its heap
dumped to extract cached plaintext DEKs. Worth recording explicitly, since
mTLS (§1b above) sounds like it might be relevant and isn't.

**Why it's real.** `HsmCryptoClient` keeps two unbounded, no-TTL
`ConcurrentHashMap` caches of plaintext DEK bytes — one keyed by `dekName`
(encrypt side), one by `edek_id` (decrypt side) — for as long as the
process runs, by design (that's what avoids a fresh `/dek/issue` round trip
per row). `close()` zeroes both, but only at graceful shutdown; a live
`jmap`/core dump captures whatever's currently cached, in the clear.

**Why mTLS doesn't help.** mTLS authenticates a *network connection* —
it's a control over who's allowed to open a TLS session to hsm-core-service.
It has no visibility into, or control over, what an already-authenticated
client does with data after the connection delivered it. By the time
`TransportWrapper` has unwrapped a DEK, mTLS's job is finished; the DEK now
lives purely in the client's own address space, a domain mTLS was never
designed to reach. An attacker who can dump that JVM's memory already *is*
the authenticated client, as far as mTLS is concerned — this is a different
threat category (endpoint/memory security) from what mTLS or the self-issued
JWT in §1a addresses (network authentication).

**Rotation doesn't help either — checked against the actual code, not
assumed.** `RotationService.rekey()` unwraps the old EDEK and re-wraps the
*same* `dekBytes` under the new KEK (`kekClient.unwrapDek(...)` then
`kekClient.wrapDek(dekBytes, ...)` — one variable, in and out); KEK
rotation/rekey changes which KEK protects a DEK at rest and never touches
the DEK itself. `NamedDekRotationScheduler` does mint a genuinely fresh DEK
for a `(app_id, dek_name)` pair, but the retired `EdekRecord` row is kept,
not deleted (`RotationStatus.ROTATED`, not removed) — every ciphertext
token already issued references that row's `edek_id` and must stay
decryptable forever, so the old DEK's ability to decrypt everything already
encrypted with it is permanent and no rotation policy can revoke it without
re-encrypting the underlying data, which nothing here automates. Rotation
also doesn't reliably protect *future* writes either: `HsmCryptoClient`'s
`encryptCacheByName` has no server-pushed invalidation, so a caller already
holding a warm cache entry for that `dekName` keeps encrypting new rows with
its own cached (possibly-compromised) copy regardless of what the scheduler
did server-side, until that specific cache entry is evicted or the process
restarts. Net: DEK rotation, of either kind, is key hygiene for future
writes from callers that haven't already cached the name — not a mitigation
for this threat, and not counted as one below.

**What actually mitigates this, cheapest first:**

1. **Already built, partial:** `HsmCryptoClient.close()` zeros both caches
   — closes the window only at clean shutdown, not while the process is
   live and serving.
2. **Client-side hardening, not yet built — shrink the window and make it
   harder to open:** bound `encryptCacheByName`/`decryptCacheByEdekId`
   with a max-size/TTL eviction instead of unbounded process-lifetime
   caching, so less plaintext DEK material is resident at any one instant;
   pair that with host/container hardening on whatever runs these JVMs —
   disable core dumps and `-XX:-HeapDumpOnOutOfMemoryError`, disable or
   encrypt swap so a DEK never gets paged to disk in the clear, drop
   `CAP_SYS_PTRACE`/apply a seccomp profile so dumping memory needs a
   kernel-level exploit rather than `jmap`. Neither half alone is much of a
   barrier; together they cut both how much is exposed and how easy it is
   to actually get at it.
3. **The real structural fix, not built, and in direct tension with the
   current design:** move DEK-holding operations behind a hardware
   boundary on the client side too (confidential-computing VM / enclave),
   or stop caching DEKs client-side entirely and route every record back
   through the server's Managed HSM. That reverses the exact
   performance-vs-exposure tradeoff `DekManager`/`HsmCryptoClient` were
   built to make in the first place — not a patch, a different design.

## 1d. Cross-app grants: `dek_name` ownership, coarse and fine-grained

Raised in review/testing: a `dek_name` (the caller-supplied handle that lets
`/encrypt`, `/encrypt/batch`, and `/dek/issue` reuse the same DEK across many
calls instead of minting a fresh one every time — see `EncryptionService`'s
DEK-reuse cache) used to be scoped `(app_id, dek_name)` at the DB level
(`idx_edek_current_name` in V7). Two different apps could each mint a DEK
under the identical name string with zero relationship between them, and
whichever app called `/decrypt`/`/dek/unwrap` afterward was the only thing
gated by a grant — reusing someone else's `dek_name` on the *encrypt* side
was silently accepted. `V14__add_scoped_grants_and_global_dek_name_ownership.sql`
closes this:

**`dek_name` is now globally unique, first-encrypt-wins.** The unique index
(`idx_edek_current_name`) is now on `current_dek_name` alone, not
`(app_id, current_dek_name)`. Whichever app's `EdekRecord` first holds a
given name becomes that name's owner system-wide, permanently (barring an
explicit grant to someone else). A second app attempting to reuse that name
via `/encrypt`, `/encrypt/batch`, or `/dek/issue` without a grant now gets a
hard `403 Forbidden` — never a silently-independent DEK.

**Migrating a real (non-demo) deployment: V14 reconciles pre-existing data,
it doesn't just assume a clean slate.** Two things the old, looser model
could have left behind, both handled by V14 itself before it enforces
anything new:

1. *Colliding `current_dek_name`s across apps.* Under the old
   `(app_id, current_dek_name)`-scoped uniqueness, two different apps could
   already legitimately hold the identical `current_dek_name`. The new
   global unique index can't be created over that, so V14 resolves it first
   — same first-encrypt-wins rule the rest of this fix uses: for each
   duplicated name, the row with the earliest `created_at` (a row with no
   timestamp at all is ordered last, deliberately, never treated as
   "oldest"; `edek_id` breaks any remaining tie) keeps `current_dek_name`;
   every other row sharing that name gets it set to `NULL`. This only
   touches `current_dek_name` — `dek_name` (permanent audit history),
   `edek_id`, and every ciphertext token already issued are untouched, and
   decrypt/unwrap are keyed by `edek_id`, never `current_dek_name`, so
   nothing already encrypted becomes undecryptable. The real, intended
   effect lands on the *losing* app(s): their next `/encrypt`,
   `/encrypt/batch`, or `/dek/issue` call under that name mints a genuinely
   fresh DEK instead of silently continuing to share the old one — exactly
   the accidental, ungranted sharing this migration exists to stop. If a
   losing app actually needs continued access to the winner's data under
   that name, grant it explicitly afterward via `/admin/grants` or
   `/admin/dek-grants`, the same as any other cross-app case.
2. *Existing `app_decrypt_grants` rows.* Carried forward, not dropped —
   V14 inserts one `app_grants` row per existing grant with `scope =
   'decrypt'` (every pre-V14 row implicitly meant decrypt) before dropping
   the old table, so no previously-granted app loses access as a side
   effect of this migration.

Verified directly against a synthetic pre-V14 dataset with real collisions
(two independently-created rows sharing a name, one with a null
`created_at`, plus a genuine cross-app grant) before shipping — not just
assumed correct from reading the SQL.

**Grants are symmetric across encrypt and decrypt, each with a coarse and a
fine-grained tier**, replacing the old decrypt-only, coarse-only
`app_decrypt_grants` table:

- `app_grants (grantee_app_id, owner_app_id, scope)` — coarse: grantee may
  act (per `scope`) on *any* of the owner's `dek_name`s / EDEKs.
- `app_dek_grants (grantee_app_id, owner_app_id, dek_name, scope)` —
  fine-grained: grantee may act (per `scope`) on that one specific
  `dek_name` only.

`scope` is `"encrypt"` or `"decrypt"` today. It is deliberately **not**
DB-constrained (no `CHECK`, no separate table per scope) — the same
unconstrained-string convention `app_registrations.allowed_scopes` already
uses — so a future scope needs no migration, only an addition to
`AdminController.KNOWN_GRANT_SCOPES` (the application-layer check that
rejects unknown scopes with `422`) the same day real enforcement code for it
ships.

**Check order**, implemented once as
`AppRegistryService.isGranted(granteeAppId, ownerAppId, scope, dekName)` and
called identically from `EncryptionService`/`DekIssueService` (scope
`"encrypt"`) and `DecryptionService`/`DekUnwrapService` (scope `"decrypt"`):

1. Same app → always allowed (an app always owns its own DEKs).
2. Coarse grant exists for `(granteeAppId, ownerAppId, scope)` → allowed.
3. Fine-grained grant exists for `(granteeAppId, ownerAppId, dekName, scope)`
   → allowed. Uses `EdekRecord.getDekName()` (permanent, never nulled), not
   `getCurrentDekName()` (nulled on rotation) — a grant keeps covering a
   name's historical/rotated data even after it rotates away from current.
4. Otherwise → denied (`403`).

The pre-existing `governance` scope bypass (`callerScopes.contains
("governance")` skips the grant check entirely, for audit tooling) is
unchanged in both `DecryptionService` and `DekUnwrapService`.

**Admin API:** `POST`/`DELETE`/`GET /admin/grants` (coarse, now scope-aware —
see [`ADMIN_OPERATIONS.md`](ADMIN_OPERATIONS.md)) and the new
`POST`/`DELETE`/`GET /admin/dek-grants` (fine-grained), both gated behind the
existing `grant` authority.

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
| Cross-app grants (`app_grants`, `app_dek_grants`) | Unaffected either way — this is a separate, finer-grained mechanism (which specific *other* app may encrypt/decrypt *this* app's data, coarse or per-`dek_name`, see §1d) that has no Entra ID equivalent and would stay DB-driven regardless |

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
