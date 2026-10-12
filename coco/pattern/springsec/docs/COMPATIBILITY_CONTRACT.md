# Compatibility Contract

What must stay byte-for-byte or field-for-field stable while the chassis work
(`java/chassis/`) is adopted service by service, and while old and new builds run
side by side (`<service>-v2` releases behind Istio weights). Every item here is
read or written by more than one implementation, often in more than one language,
against shared data (the `edek_records` table, Redis, Key Vault secrets, files at
rest). A change that only one side picks up breaks decryption, authorization or
auditing for the other — usually silently, in production, on data that already
exists.

**Rule:** anything in this document changes only through the process in
[Changing a contract](#changing-a-contract). Refactors (including every chassis
phase) must leave all of it unchanged.

## Who depends on what

| Contract | hsm-core-service (+ bulk release) | hsm-crypto-client (file-service, bulk-client, Spark adapter) | hsm-databricks-udf (Python) | hsm-cache-key-rotator |
|---|---|---|---|---|
| [C1 Ciphertext token](#c1-ciphertext-token-v1) | writes, reads | writes, reads | writes, reads | — |
| [C2 AAD](#c2-aad-app-binding) | ✓ | ✓ | ✓ | — |
| [C3 Fingerprint](#c3-fingerprint) | writes, checks | — | — | — |
| [C4 DEK transport wrap](#c4-dek-transport-wrap-rsa-oaep) | wraps | unwraps | unwraps | — |
| [C5 EDEK record](#c5-edek-record-edek_records) | owns (Flyway) | — | — | — |
| [C6 KEK wrapping](#c6-kek-wrapping-managed-hsm) | ✓ | — | — | — |
| [C7 Redis DEK cache](#c7-redis-dek-cache) | reads, writes | — | — | rekeys/flushes |
| [C8 Cache CEK secrets](#c8-cache-encryption-key-secrets) | reads | — | — | writes |
| [C9 Encrypted file format](#c9-encrypted-file-format) | rescue decrypt of a chunk | writes, reads | — | — |
| [C10 HTTP API](#c10-http-api) | serves | calls | calls | — |
| [C11 Caller identity](#c11-caller-identity) | enforces | presents | presents | — |
| [C12 Audit events](#c12-audit-events) | emits | — | — | emits |
| [C13 Job lock keys](#c13-job-lock-keys) | ✓ | — | — | — |

Out of scope: the legacy Python service (`app/`, `scheduler/`, `migrations/`, `tests/`)
is not active and is not covered by this contract.

---

## C1 Ciphertext token (`v1.`)

The opaque string `/encrypt` returns and `/decrypt` accepts; also produced and
consumed locally by the crypto client and the UDF.

```
on-wire:  "v1." + base64url(binary)
binary:   version(1) = 0x01
          | edek_id(16)      UUID, big-endian (RFC 4122 byte order)
          | iv(12)           AES-GCM nonce
          | tag(16)          AES-GCM authentication tag
          | ciphertext(N≥1)
```

- Cipher: AES-256-GCM, 96-bit IV, 128-bit tag, AAD per [C2](#c2-aad-app-binding).
- **Encoders emit padded base64url** (Java `Base64.getUrlEncoder()`, Python
  `urlsafe_b64encode`). **Decoders must accept padded and unpadded** input (Java
  re-pads; Python appends `==`).
- Fixed overhead 45 bytes; a token with an empty ciphertext is rejected.
- Implementations: `hsm-crypto-core` `DekManager` (the shared library, used by
  `hsm-crypto-client` and so by hsm-file-service, hsm-bulk-client and the Spark
  adapter), `hsm-core-service` `crypto/DekManager` (a copy, until it adopts
  `hsm-crypto-core`), and `hsm-databricks-udf` `dek_manager.py`.

## C2 AAD (app binding)

`AAD = UTF-8("hsm-svc:app_id=" + owner_app_id)` — the app that **owns** the EDEK,
not the caller. Applies to C1 tokens and to every chunk of [C9](#c9-encrypted-file-format).
Changing this string makes every existing ciphertext undecryptable.

## C3 Fingerprint

`hex(SHA-256(iv || tag)[0:8])` — 16 lowercase hex chars, stored in
`edek_records.fingerprint` (nullable for pre-V2 rows) and checked before
decryption to catch mixed-up `iv`/`tag`/`edek_id` inputs early. A consistency
check, not a MAC.

## C4 DEK transport wrap (RSA-OAEP)

How `/dek/issue` and `/dek/unwrap` hand a raw DEK to the calling app.

- `RSA/ECB/OAEPWithSHA-256AndMGF1Padding`: OAEP, SHA-256 for both the hash and
  MGF1, **no label**. BC-FIPS requires `Cipher.WRAP_MODE`/`UNWRAP_MODE` for this.
- Public key: the app's `app_registrations.public_key_pem`, PEM `PUBLIC KEY`
  (X.509 SubjectPublicKeyInfo). Private key (client side only): PEM `PRIVATE KEY` (PKCS#8).
- Response field `wrapped_dek_b64`: **standard** base64 (not url-safe).
- Implementations: `hsm-core-service` `crypto/TransportWrapper` (wrap; a copy until
  it adopts `hsm-crypto-core`), `hsm-crypto-core` `TransportWrapper` (unwrap, via
  `hsm-crypto-client`) and `hsm-databricks-udf` `transport.py` (unwrap).

## C5 EDEK record (`edek_records`)

Schema owner: **`hsm-core-service` Flyway** (`db/migration/V1…V15`), schema
`${CRYPTO_SCHEMA}`. Access tables (`app_registrations`, `app_grants`,
`app_dek_grants`, `app_classification_grants`) live in `${ACCESS_SCHEMA}`.

| Column | Contract |
|---|---|
| `edek_id` | UUID primary key; the same id is embedded in every C1 token and C9 header |
| `app_id` | owning app; source of the C2 AAD |
| `edek_blob` | standard base64 of the Managed-HSM-wrapped DEK |
| `kek_name`, `kek_version` | which KEK/version wrapped `edek_blob`. `kek_name` NULL = legacy row, meaning `AZURE_KEK_NAME` (backfilled on the next rotation) |
| `algorithm` | `AES-256-GCM` |
| `encoding` | `utf8` \| `base64` — how `/decrypt` returns plaintext |
| `rotation_status` | `current` \| `pending` \| `rotated`. Rotated rows stay decryptable |
| `fingerprint` | C3; nullable |
| `dek_name`, `current_dek_name` | named-DEK reuse; `idx_edek_current_name` is unique on `current_dek_name` (globally, since V14) |
| `previous_kek_name`, `previous_kek_version`, `previous_edek_blob` | single-level undo buffer for rekey |

Rules: **additive, nullable columns only** while two builds share the database;
**only one release runs Flyway** during an overlap (the other runs with
`spring.flyway.enabled=false`); never drop or rename a column that a running build
reads.

## C6 KEK wrapping (Managed HSM)

DEKs are wrapped by Azure Key Vault Managed HSM, `RSA-OAEP-256` `wrapKey`/`unwrapKey`,
key addressed by `kek_name` + `kek_version`. Old KEK versions must stay enabled
for unwrap until no `current` row references them (see RUNBOOK "KEK rotation").

## C7 Redis DEK cache

- Key: `dek:{slot}:{kv_version}:{edek_id}` (UTF-8), e.g.
  `dek:alpha:0f4a…:7000…-0007`. `slot` is the active CEK slot (`alpha`/`beta`) and
  `kv_version` that slot's Key Vault secret version.
- Value: ASCII `base64(iv) + ":" + base64(ciphertext_with_tag)`, standard base64,
  AES-256-GCM under the CEK, 12-byte IV, **no AAD**.
- TTL: `DEK_CACHE_TTL_SECONDS`. Lookup tries the current CEK version, then the
  previous one, so a CEK rotation causes no miss storm.
- Implementations: `hsm-crypto-core` `DekCacheCodec` (the shared codec), used by
  `hsm-cache-key-rotator` `RedisOps` (rekey/flush); `hsm-core-service`
  `crypto/RedisDekCache` still carries its own copy until it adopts `hsm-crypto-core`.

## C8 Cache encryption key secrets

Key Vault secrets (names configurable, defaults shown):

| Secret | Content |
|---|---|
| `cek-current-key` | the literal slot name `alpha` or `beta` |
| `cek-alpha`, `cek-beta` | standard base64 of a 32-byte AES key |

The rotator writes a new key into the **inactive** slot, then flips
`cek-current-key`. Services poll (`CekHotReloadScheduler`) and switch on a slot or
version change. Exactly one rotator may run (chart: `replicaCount: 1`,
`strategy: Recreate`).

## C9 Encrypted file format

Normative spec: [`FILE_FORMAT.md`](FILE_FORMAT.md) (v1 and v2 `HSMF` headers,
length-prefixed frames, chunk payload markers). Pinned by golden files in
`hsm-crypto-client/src/test/resources/golden/`. Each chunk uses the C2 AAD so
`hsm-core-service` `/decrypt` can rescue any single chunk.

## C10 HTTP API

- Prefix `API_V1_PREFIX` (default `/api/sensec/hsm/v1`); JSON in **snake_case**.
- Client-facing endpoints: `POST /encrypt`, `/decrypt`, `/encrypt/batch`,
  `/decrypt/batch`, `/dek/issue`, `/dek/unwrap`. Admin endpoints are listed in
  [`ADMIN_OPERATIONS.md`](ADMIN_OPERATIONS.md).
- Status codes: `201` encrypt, `200` others; `401` missing/invalid credentials,
  `403` scope/grant/app-id mismatch, `404` unknown path or record, `409` rotation
  already running (admin), `422` validation and undecryptable input.
- **hsm-core-service error body: `{"detail": "<message>"}`.** hsm-file-service
  uses its own `{"error_code", "message", "request_id"}` (see `FILE_SERVICE.md`). The
  chassis error format must keep both available until every client has migrated.
- **Pinned by `ApiContractBaselineTest`** (hsm-core-service): status, contract
  headers and JSON shape of 15 representative calls, baselines in
  `src/test/resources/contract-baseline/`.

## C11 Caller identity

| Mechanism | Presented as | Rule |
|---|---|---|
| JWT | `Authorization: Bearer <jwt>` + `X-App-ID` | `app_id` claim (Azure AD `appid` accepted as equivalent) must equal `X-App-ID`, else 403 |
| Self-signed app JWT | same headers | signed with the app's `signing_public_key_pem` |
| mTLS (optional) | Istio `x-forwarded-client-cert` + `X-App-ID` | SHA-256 cert fingerprint must equal the app's `mtls_cert_fingerprint` |

Scopes come from `app_registrations.allowed_scopes` (comma-separated):
`encrypt`, `decrypt`, `dek_issue`, `dek_unwrap`, `rotate`, `grant`, `manage_apps`,
`provision_app_keys`, `manage_classifications`, `manage_kek_registry`.

Request/trace headers: `X-Correlation-Id` (hsm-core-service: accepted, generated
if absent, echoed; MDC `correlationId`), `X-Request-Id` (hsm-file-service, via
`hsm-chassis-web`'s `RequestIdFilter`: accepted if it matches
`[A-Za-z0-9._:-]{1,128}`, else generated; echoed; MDC `requestId`), `X-End-User`,
`X-Response-Detail`, `X-Forwarded-For`. When hsm-core-service adopts the chassis it
must keep `X-Correlation-Id`/`correlationId` and add `X-Request-Id` to
`also-accept`. Not yet done: hsm-file-service does not forward its request id on
its calls to hsm-core-service (hsm-crypto-client `SvcClient`), so one user request
appears under two ids.

## C12 Audit events

Every audit record carries `event_type`; stdout lines add `event`, `level`,
`logger=audit`, `host`, `timestamp` (`yyyy-MM-ddTHH:mm:ssZ`); Splunk records add
`_epoch`. `splunkSource` distinguishes releases (`hsm-core-service`,
`hsm-bulk-service`; give a v2 release its own).

Event names in use (Java): `access_denied`, `app_keys_updated`,
`app_mtls_cert_updated`, `app_status_changed`, `auth_failure`, `batch_decrypt`,
`batch_encrypt`, `classification_check`, `classification_grant_added`,
`classification_grant_removed`, `decrypt`, `dek_grant_added`, `dek_grant_removed`,
`dek_issued`, `dek_name_reservation_check`, `dek_unwrapped`, `edek_metadata_viewed`,
`encrypt`, `grant_added`, `grant_removed`, `kek_registry_entry_added`,
`kek_registry_entry_removed`, `kek_rekey_completed`, `kek_rekey_reverted`,
`kek_rotation_completed`, `named_dek_rotation_completed`,
`named_dek_rotation_row_failed`, and `<operation>_skipped` for lock contention.

Rules: adding events or fields is fine; renaming or removing an event or field, or
changing a field's type, breaks Splunk searches and dashboards. Never log
plaintext, DEKs or exception messages that may carry secrets.

## C13 Job lock keys

Postgres advisory locks, `hashtextextended(key, 0)`: `hsm:kek-wrap`
(rotate/rekey/revert) and `hsm:named-dek-rotation`. Every build that can run these
jobs against the same database **must use the same keys**, or v1 and v2 will
rotate concurrently. See RUNBOOK "Rotation jobs: one runner across all pods".
The lock itself is `hsm-chassis-core` `com.hsm.chassis.lock.PostgresAdvisoryJobLock`
(hsm-core-service keeps its own identical copy until it adopts the chassis); the
key strings live in the calling code (`RotationService`) and must be carried over
unchanged.

---

## Enforcement status

| Contract | Pinned by | Status |
|---|---|---|
| C1, C2, C3 | `java/chassis/test-vectors/crypto-v1.json`, run by `hsm-crypto-core` and `hsm-core-service` (`CryptoVectorsTest`) and `hsm-databricks-udf` (`test_crypto_vectors.py`); `hsm-crypto-client` now uses `hsm-crypto-core` itself | ✓ (Phase 1) |
| C4 | transport vectors in the same file, same four suites | ✓ (Phase 1) |
| C5 | Flyway migrations + `ApiContractBaselineTest` indirectly | partial |
| C7 | cache vectors in the same file, run by `hsm-crypto-core`; `RedisOpsContainerTest` (rotator, real Redis) | partial — `RedisDekCache` keeps its own copy until hsm-core-service adopts `hsm-crypto-core` |
| C8 | — | manual review |
| C9 | `hsm-crypto-client` golden files | ✓ |
| C10 | `ApiContractBaselineTest` | ✓ (Phase 0) |
| C11 | existing auth tests (`RsaJwtValidatorTest`, `SelfSignedJwtMultiAudienceTest`, `AdminMtlsCertTest`) | partial |
| C12 | — | manual review |
| C13 | `RotationServiceLockTest`, `PostgresAdvisoryJobLockContainerTest` | ✓ |

The vectors are generated from the written spec by
`java/chassis/test-vectors/generate_crypto_v1.py` (Python `cryptography`, independent
of every implementation under test). If an implementation stops matching them, fix
the implementation; regenerate only for a deliberate, versioned contract change.

## Changing a contract

1. Write down the change and every consumer in the table above that it touches,
   including the Python implementations.
2. Make it **additive or versioned**: a new optional field, a new event, a new
   token prefix (`v2.`) — never a reinterpretation of existing bytes or fields.
3. Ship readers first: every consumer must accept the new form before any
   producer emits it, and keep reading the old form for as long as old data or
   old builds exist (ciphertexts and EDEKs effectively live forever).
4. Update this document, the vectors/baselines, and `FILE_FORMAT.md` where relevant,
   in the same change.

## Open items

- **Error body divergence** (C10): `{"detail"}` vs `{"error_code","message","request_id"}`
  — the target format for the chassis is still to be decided.
