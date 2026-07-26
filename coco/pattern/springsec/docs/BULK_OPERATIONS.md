# Bulk Encrypt/Decrypt: Design Plan

**Status: Tier 1 (synchronous batch encrypt and batch decrypt) is
implemented in `hsm-core-service`.** Tier 2 (large datasets/files) is
**not** a service to build — see "Architecture correction" below. Tier 3
(client-side local envelope encryption via a separate hsm-bulk-service) is a
**design proposal, not yet approved or built** — see that section below.
This exists to support app on-boarding (migrating an existing plaintext
dataset into envelope encryption) and de-boarding (bulk export/decrypt, or
bulk crypto-shred) — see `APP_ONBOARDING.md` for the onboarding procedure
this would plug into.

## Architecture correction: bulk is a client-side pattern, not a service we operate

An earlier version of this doc proposed a separate "bulk-job worker"
service, owned by this repo, that would read from and write to the calling
app's own blob storage/database. That's wrong, and it's wrong for the same
reason `DEMO.md` §6 already states for storage: **this service never stores
ciphertext, the calling app does, in its own schema.** The same boundary
applies to *process*, not just data at rest:

- Only the calling app's team knows their own DB schema or file layout —
  `hsm-core-service` has no business understanding, say,
  `payments-svc`'s legacy table structure.
- A separate bulk-job service reaching into an app's own DB/blob storage to
  read plaintext at scale would need a **new, broad cross-app
  storage-access grant** — a far bigger permission surface than "call this
  app's own scoped JWT against a stateless crypto API." The batch endpoint
  needs zero knowledge of where the caller's data lives; a bulk-job
  service reaching into it would need exactly that.
- De-boarding's crypto-shred case makes this obvious in the limit:
  deleting a `ciphertext_token` row is *already* effective crypto-shredding
  (see `DEMO.md` §6), entirely within the app's own database. There is
  nothing for a "bulk service" to do there — it was never
  `hsm-core-service`'s job to begin with.

**Corrected shape:** Tier 2 is a *pattern* the calling app's own code
follows — reading its own data source, driving Tier 1 (or repeated
single-item calls) as the crypto primitive, writing results back into its
own storage. `hsm-core-service`'s role doesn't change at all: told
about plaintexts, returns tokens, never touches or knows about the
caller's data at rest. What's still useful from this doc is the *design
guidance* for how a client should structure that work correctly (chunking
strategy, manifest, stitch-back) — not a new deployable on our side.

## What's implemented: `POST /encrypt/batch` and `POST /decrypt/batch`

Multiple plaintexts, one authenticated call, each item correlated back by a
caller-supplied `key` (not array position) so the calling app can track a
result to its own row/record identifier:

```
POST ${API_V1_PREFIX}/encrypt/batch   -- requires the "encrypt" authority, same as /encrypt

{ "items": [
    { "key": "app-side-id-1", "plaintext": "...", "data_classification": "pii" },
    { "key": "app-side-id-2", "plaintext": "..." }
] }

→ 200 OK   (always 200 -- see "partial failure semantics" below)
{ "items": [
    { "key": "app-side-id-1", "status": "success", "result": { "ciphertext_token": "v1....", ... } },
    { "key": "app-side-id-2", "status": "success", "result": { "ciphertext_token": "v1....", ... } }
] }
```

Implementation notes:

- `EncryptionService.encryptBatch` reuses `encrypt(...)` unmodified, once
  per item — no new crypto logic, no new EDEK-writing path.
- **Sequential, not concurrent**, for v1 — bounded concurrent fan-out is
  still deferred pending real HSM throughput numbers (see "Open decisions"
  below); a batch of 100 items today means 100 sequential HSM wrap calls,
  correct but not yet optimized for latency.
- Item-count cap: `hsm.service.batch-max-items` (`ENCRYPT_BATCH_MAX_ITEMS`
  env var), default 100 — a conservative starting point, not a measured one.
- Structural violations (empty batch, over-cap batch, duplicate key, any
  item's blank/oversized plaintext) reject the **whole request** with 422
  before any item is processed. A single item's runtime outcome (PBAC
  denial, an unexpected per-item error) does **not** fail the batch — it's
  reported as that item's `status: "error"` with a `detail` message, and
  every other item still completes. This mirrors SQS `SendMessageBatch`
  more than S3 batch operations, chosen for client simplicity (always
  parse a 200, never branch on `207`).
- Same scope/grant model as the single-item endpoints — no new privilege
  surface, verified by test (`batchRequiresEncryptScope`).
- Audit logging: one event per item (`encrypt`, same as today) plus one
  `batch_encrypt` summary event (item/success/failure counts).
- Verified end-to-end in `BatchEncryptIntegrationTest`: multi-item
  correlation by key, a batch result's `ciphertext_token` genuinely
  decrypts via the existing `/decrypt` endpoint (not just shaped
  correctly), duplicate-key/empty/over-cap/blank-plaintext rejection, and
  scope enforcement.

### `POST /decrypt/batch` — symmetric, with one real behavioral difference

```
POST ${API_V1_PREFIX}/decrypt/batch   -- requires the "decrypt" authority, same as /decrypt

{ "items": [
    { "key": "row-1", "ciphertext_token": "v1...." },
    { "key": "row-2", "edek_id": "...", "iv_b64": "...", "ciphertext_b64": "...", "tag_b64": "..." }
] }

→ 200 OK
{ "items": [
    { "key": "row-1", "status": "success", "result": { "plaintext": "...", ... } },
    { "key": "row-2", "status": "success", "result": { "plaintext": "...", ... } }
] }
```

- `DecryptionService.decryptBatch` reuses `decrypt(...)` unmodified, once
  per item — same pattern as encrypt, same sequential-not-concurrent
  choice, same `hsm.service.batch-max-items` cap (shared with encrypt, not
  a separate knob), same duplicate-key/empty/over-cap rejection.
- **The one real difference from batch encrypt**: the "provide either
  `ciphertext_token` or all four legacy fields" check is *not* expressible
  as a static Bean Validation constraint — it's a runtime check inside
  `decrypt()` itself. So a malformed item there (missing both, or a
  corrupt token) surfaces as **that item's error**, not a whole-batch
  rejection — one bad token among 99 good ones doesn't sink the batch.
  Verified directly (`oneMalformedItemDoesNotFailWholeBatch`).
- Same grant model, per item: a decrypt without a valid cross-app grant is
  a per-item `error`, not a batch-wide 403 (verified:
  `crossAppDecryptDeniedPerItemWithoutGrant`).
- Verified security wiring exists at all (missing bearer token → 401 on
  the batch endpoint itself, `batchDecryptRequiresAuthentication`) — the
  more meaningful check than a scope-specific 403 here, since every demo
  app happens to hold `decrypt`, so there's no convenient demo app to prove
  scope *denial* with; forgetting the `hsm.security.access-rules` entry
  entirely would make the endpoint silently public, which this test rules
  out.
- Audit logging: one `decrypt` event per item (same as today) plus one
  `batch_decrypt` summary event.
- 8 tests in `BatchDecryptIntegrationTest`, full suite now 55/55.

### What's still not implemented

- **Audit timing.** Both `batch_encrypt` and `batch_decrypt` summary events
  carry item/success/failure counts and the standard single completion
  timestamp every audit event gets — but no `duration_ms` or start-time
  field. There's no way to derive elapsed time for a batch from the audit
  trail today; that needs a captured start `Instant` and a computed
  duration added to both events. Small, not built.
- Tier 2 (bulk from any datastore, file chunking/stitch-back) — see
  "Architecture correction" above; this was reframed as client-side
  guidance, not something this repo builds.

## Batch vs. bulk — the actual distinction

| | **Batch** (Tier 1) | **Bulk** (Tier 2) |
|---|---|---|
| What it is | Many *plaintexts* in one HTTP request/response | A *dataset or file* too large for one HTTP call |
| Shape | Synchronous — caller sends, gets a result back immediately | The caller's own job/script, iterating its own data, driving batch calls |
| Volume | Tens to low-hundreds of items per call | Thousands to millions of records, or multi-GB files, across many calls |
| Lives in | `hsm-core-service` — the endpoint above | **The calling app's own code and infrastructure** |
| Motivating case | "Encrypt these 50 records from today's ETL run" | "Migrate this app's entire legacy table" / "export everything before de-boarding" |

`hsm-core-service` only ever implements the left column. The right
column is guidance for app teams, not a roadmap item for this service.

## Authentication: no new mechanism, on either side

`JwtAppIdAuthenticationFilter` already authenticates once per HTTP request,
not once per encrypt operation — a batch expressed as one request with N
items gets "one auth, N operations" for free from the existing model. A
client-side bulk job uses the exact same per-app JWT it always would,
calling the batch endpoint (or repeated single-item calls) like any other
caller — there's no separate "bulk auth" concept to design, because there's
no separate service to authenticate against.

---

## Guidance for client-side bulk jobs

This section is reference material for app teams building their own
migration/export tooling on top of Tier 1 — not a spec for something this
repo builds.

### Driving the batch endpoint at scale

- Page through your own data source, call `/encrypt/batch` with chunks of
  up to `hsm.service.batch-max-items` records, write the returned
  `ciphertext_token` back into your own schema per `key`.
- Size your own concurrency (how many batch calls in flight at once)
  against Azure Managed HSM's actual throughput — the same constraint
  `CACHING_AND_ROTATION.md` describes for this service's own internal
  fan-out applies equally to a client hammering the batch endpoint from
  multiple workers. There's no cache benefit for bulk/unique-key traffic
  either way (see that doc): every record is a unique DEK regardless of
  who's driving the loop.
- Track your own job progress (which records are done) in your own
  storage, so a job that dies partway through resumes rather than
  restarting — this service has no concept of your job at all, so nothing
  here can help you resume; that state has to live on your side.

### Files with multiple chunks: chunking + stitch-back

For a file too large to encrypt as one plaintext:

**Chunking strategy — reuse the existing per-record envelope model
unmodified, once per chunk**, called from your own code. Rejected
alternative: a shared DEK across all chunks with a monotonic-counter
nonce — more storage-efficient (one EDEK per file instead of one per
chunk) but new crypto-construction surface in a system where getting nonce
uniqueness wrong is catastrophic (GCM nonce reuse under the same key
breaks confidentiality *and* integrity). Not worth the risk for a storage
saving, and not something to build without a dedicated review regardless
of who builds it.

Instead: split the file into chunks (aligned to whatever your own storage's
multipart chunk size is, e.g. 4–16 MB, so the file is never held whole in
memory on your side), and encrypt each chunk as its own item via the batch
endpoint — the existing, already-reviewed primitive, invoked once per
chunk instead of once per logical record.

**Chunk identity binding.** If you want a chunk's AEAD tag to fail
verification when it's decrypted out of position or attributed to the
wrong file (not just when its bytes are tampered), that requires binding
`file_id`/`chunk_index`/`chunk_count` into the AAD — today's AAD is fixed
to `owner_app_id` (`DekManager.encrypt(..., appId)`) and isn't
caller-configurable. If this level of integrity binding turns out to
matter for a real onboarding migration, that's a scoped, reviewable
addition to `DekManager`/`EncryptRequest` — flag it as a real need before
assuming it, not speculative.

**Manifest — your own record, in your own storage**, not something this
service creates or stores: `file_id`, original filename, total size, chunk
count, the ordered list of `ciphertext_token`s per chunk, and a whole-file
digest (SHA-256 over the plaintext) computed on your side before encrypting.
You may optionally encrypt the manifest itself through the same batch/single
endpoint if it contains sensitive metadata — that's just one more item
through the existing pipeline, not new capability.

**Stitch-back (decrypt), on your side:** read your manifest → decrypt each
chunk (batch endpoint, or repeated single calls) → concatenate in manifest
order → verify the reassembled plaintext's SHA-256 against what you stored
at encrypt time before using it. The digest check is what catches a chunk
being silently dropped or duplicated during your own reassembly — the
per-chunk AEAD tag alone only proves each chunk's ciphertext wasn't
tampered with, not that your stitching logic assembled them correctly.

### Bulk crypto-shred (de-boarding) needs no new capability at all

Per `DEMO.md` §6: deleting the `ciphertext_token` value from your own table
is immediately effective crypto-shredding, since the wrapped DEK alone
(still sitting in this service's EDEK store) is useless without it. At
scale, this is just your own bulk `DELETE`/update against your own schema
— nothing to build here, on either side. Cleaning up the now-orphaned EDEK
rows on this service's side afterward is good hygiene but not
time-critical (per the same section) — a candidate for a periodic
reconciliation job *if* it ever becomes an actual operational problem, not
something to build speculatively.

---

## Tier 3 (proposed, not yet approved): local envelope encryption via a separate hsm-bulk-service

**Status: design proposal captured for review — nothing in this section is
built.** Distinct from Tier 1 (built) and Tier 2 (client-side guidance, zero
new capability) — Tier 3 requires new service-side capability that neither
of those needed, so it's tracked separately and must not be read as already
decided.

### Motivation: SVC's own compute/bandwidth footprint, not the HSM's RPS ceiling

For very large onboarding volumes (millions of records, files of varying
size), the case for Tier 3 is **not** that it reduces the number of Azure
Managed HSM wrap/unwrap operations — it doesn't. That stays one HSM
operation per DEK regardless of tier, because DEK-per-record is preserved
(see the constraint below), and HSM call count is identical whether SVC
does the wrap for `/encrypt/batch` or hsm-bulk-service does it for
`/dek/issue`. The actual case: SVC today pays a real, separate cost per
record — receiving the plaintext, running AES-256-GCM locally, returning
the ciphertext — on a *shared, multi-tenant* process that other apps'
concurrent latency-sensitive single-item and batch traffic also depends on.
Removing that per-record plaintext/ciphertext traffic and compute from SVC
reduces its footprint as a shared resource — a real benefit, but a
multi-tenant-fairness one, not an HSM-throughput one. This was informed by
real experimental data (local-vs-over-wire timing against non-HSM tools)
showing a large difference — though that comparison's "local" arm used one
key for an entire operation, a materially different (and rejected, see
below) design from DEK-per-record.

### Non-negotiable constraint: DEK-per-record, unchanged

Explicitly considered and rejected: issuing one DEK per batch or file
(shared across many records via a counter-based nonce) to reduce HSM call
count. That would more closely match the throughput the local-vs-over-wire
comparison showed, but it breaks the actual point of HSM-backed envelope
encryption — record-by-record isolation, where a single compromised or
leaked DEK only ever exposes that one record. Not worth trading away for
throughput. **Any Tier 3 work keeps one DEK per record, full stop.**

### Design

**Key exchange:** a long-lived asymmetric keypair (RSA-OAEP or ECIES — pick
one, not yet decided) per app, generated at CLNT install/provisioning time.
Public key registered with the service at onboarding (new column/table
alongside `app_registrations`, same migration-based onboarding pattern used
today). Private key never leaves CLNT's host. SVC wraps every issued DEK
with the app's public key — no shared secret is ever transmitted, so there
is no "how do we deliver the wrapping key" problem to solve at all.
Rotation is a periodic admin operation, not per-batch; a leaked private key
alone gives an attacker nothing without a valid CLNT service credential too.

**New endpoints, on a separate service (hsm-bulk-service) — not on
`hsm-core-service`:**
- `POST /dek/issue` (encrypt path) — mint N new DEKs, KEK-wrap and persist
  each as a normal EDEK via the HSM exactly as `/encrypt` does today
  (unchanged persistence, unchanged source of truth), also wrap each raw
  DEK with the caller's public key for transport, return
  `{edek_id, wrapped_dek}` per item. Batch-native from day one (accepts N
  items per call) — unlike Tier 1, which started single-item and added
  batch later, Tier 3 only ever exists to serve bulk volume.
- `POST /dek/unwrap` (decrypt path) — given N `edek_id`s, unwrap each via
  the HSM exactly as `/decrypt` does today, re-wrap for transport, return
  the same shape.
- New scopes: `dek_issue`, `dek_unwrap` — distinct from `encrypt`/
  `decrypt`; a materially bigger trust grant, since this is the first place
  in this system raw key material (even transport-wrapped) ever leaves a
  service process. Provisioned separately in onboarding, not bundled with
  `encrypt`/`decrypt`.

**Why a separate service, not new endpoints on `hsm-core-service`:**
keeps this more sensitive capability out of the always-on, latency-critical
primary service's binary entirely — not just access-controlled,
structurally absent. Mirrors the pattern `cek-rotation-service` already
proves works in this codebase: an independently deployed service with its
own direct HSM/Key Vault access (own managed identity, not delegated
through the main service, so it isn't coupled to the main service's
uptime), sharing the same Postgres `app_registrations`/EDEK store as the
single source of truth (so `/decrypt`/`/decrypt/batch` on the main service
can still resolve any EDEK this service creates), and its own
`hsm.security.access-rules`-equivalent config for the two new scopes. Audit
events tagged by service name into the same pipeline as the main service's
audit trail, so the trail stays unified across both processes.

**On/off is a deployment operation, not a code path.** A separate
Helm-managed Deployment, scaled to zero (or fully undeployed) between
scheduled onboarding/de-boarding windows — stronger than a build-time
compile flag (would mean maintaining two variants of the same binary) or a
runtime scope toggle (which doesn't exist today anyway: confirmed no admin
endpoint edits an existing app's `allowed_scopes` post-onboarding, only
`/admin/apps/status` active/inactive toggling exists). "Off" here means
genuinely unreachable at the network layer, not just denied at the
authorization layer.

**IV and tag generation: entirely CLNT's responsibility.** SVC never sees
either, matching "plaintext never leaves CLNT." CLNT must generate a fresh
IV immediately before each local encrypt, from a CSPRNG with the same rigor
SVC's own `IvFactory` already requires (BC-FIPS's approved DRBG if CLNT is
JVM-based, the platform's real CSPRNG otherwise) — recommend porting
`IvFactory`'s exact logic into CLNT rather than reimplementing it, so IV
length/encoding matches byte-for-byte what `/decrypt` expects to parse. The
tag isn't generated as a separate step — it's an automatic output of
whichever side performs the AES-GCM operation, which in this tier is always
CLNT, for both encrypt and decrypt.

**Token-format compatibility — a hard requirement, not a nice-to-have.**
CLNT must produce the exact same `ciphertext_token` byte format `/encrypt`
produces. This guarantees any record encrypted locally by CLNT can still be
decrypted through the normal `/decrypt` or `/decrypt/batch` endpoint on
`hsm-core-service`, with no dependency on CLNT, the app's keypair, or
hsm-bulk-service at all. Local crypto is purely a throughput/footprint
optimization for the bulk case — never a second, divergent format only
CLNT can read back.

**Files:** the same manifest-based chunking approach already documented
above for Tier 2 (ordered `ciphertext_token`s per chunk, whole-file
SHA-256, stitch-back with a digest check) — each chunk gets its own DEK via
`/dek/issue`, same as a record. Do not use inline newline-delimited framing
for chunk boundaries: raw AEAD ciphertext can contain a literal newline
byte and will corrupt that framing; the manifest approach already avoids
this problem entirely.

**Audit gap — a named tradeoff, not silently absorbed.** SVC's audit trail
today captures every actual encrypt/decrypt operation. With Tier 3,
hsm-bulk-service only sees `dek_issued`/`dek_unwrapped` events (batch id,
count, app_id) — it does not see the actual per-record AES-GCM operation,
since that happens entirely on CLNT, outside either service's visibility.
If per-record audit visibility ever matters for this path, CLNT could
optionally report a completion summary back after a batch finishes — not
required for a first version.

### What this is NOT

- **Not an HSM-RPS reduction.** One HSM wrap/unwrap operation per DEK
  either way — Tier 3 doesn't change Managed HSM call volume, only where
  the AES-GCM step and the plaintext/ciphertext transfer happen.
- **Not a shared-DEK design.** DEK-per-record is preserved exactly as
  today; explicitly rejected trading that isolation property for
  additional throughput.
- **Not a mechanism for real-time or steady-state traffic.** Confirmed
  scope: onboarding (once) and de-boarding (once) per app's lifecycle.
  Once onboarding completes, an app goes back to single-item or Tier 1
  batch calls — never Tier 3 — for ongoing operation.

---

## Open decisions

- Tier 1 item-count and payload-size caps — needs real HSM throughput
  numbers (ties to the "Performance testing and capture metrics" backlog
  item).
- Whether `hsm-core-service`'s team should publish a small reference
  client library (a thin SDK wrapping the chunking/manifest/stitch-back
  pattern above in a couple of languages) so app teams doing this
  correctly don't each reinvent it — genuinely useful, but it's a shared
  *library*, not a service, and doesn't change this service's own
  boundary.
- Tier 3: RSA-OAEP vs. ECIES for the DEK transport wrap.
- Tier 3: long-lived per-app keypair (simpler, standing infrastructure) vs.
  ephemeral per-batch keypair (stronger forward secrecy, more moving
  parts) — current lean is long-lived, given a compromised private key
  alone still can't get anything issued without a valid CLNT service
  credential.
- Tier 3: whether an admin capability to revoke individual scopes from an
  existing app (not just today's active/inactive toggle) gets built
  generally, or scoped narrowly to just `dek_issue`/`dek_unwrap`.
- Tier 3 needs a dedicated security/crypto review before any of it is
  built — this doc is the input to that review, not a substitute for it.

## What this doesn't change

No changes to the core envelope-encryption model, the KEK/HSM boundary, the
grant/scope authorization model, or the audit-logging shape — batch is a
*calling pattern* layered on top of the existing single-item primitive, not
a new crypto design, and bulk was never this service's responsibility to
begin with. The architecture correction above doesn't loosen that
boundary — it reinforces it: the fix for "bulk needs work" turned out to be
"the calling app already had everything it needed," not a new deployable.
Tier 3, if approved, is the one exception to "no new capability" — it's
scoped narrowly (DEK issuance/unwrap only, a separate service, DEK-per-
record preserved) specifically so it doesn't loosen the KEK/HSM boundary
either: the real KEK is still the only thing that ever produces a durable,
recoverable EDEK, exactly as today.

## Development plan for Tier 3 (if approved)

Sequenced so each phase produces something reviewable or testable on its
own, rather than a single large build. Nothing here starts until Phase 0
concludes.

**Phase 0 — Review sign-off, no code.** Take this document to
security/crypto review. Needs explicit decisions on: RSA-OAEP vs. ECIES for
the transport wrap, long-lived vs. ephemeral per-app keypairs, the
`dek_issue`/`dek_unwrap` scope model, the separate-service (hsm-bulk-service)
deployment boundary, and the audit-visibility tradeoff. Everything below
depends on these being decided, not assumed — this is the same "don't
build ahead of a decision" discipline used for the CLNT-vs-embedded-
endpoint question earlier in this doc.

**Phase 1 — hsm-bulk-service skeleton + `POST /dek/issue` (encrypt path
only).** New Maven module, scaffolded the same way `cek-rotation-service`
was: its own managed identity and direct Key Vault/HSM client wiring (reuse
the existing `KekClient` interface), reusing `DekManager`'s DEK-generation
and KEK-wrap/EDEK-persistence logic unmodified, plus the new public-key
transport-wrap step. Wire `dek_issue` against the shared `app_registrations`
table. No decrypt path yet. The single most important test in this phase:
a CLNT-produced `ciphertext_token` must decrypt correctly through the
*existing, unmodified* `/decrypt` endpoint on `hsm-core-service` —
this is the token-format-compatibility requirement from the design section
above, and it's the one thing that has to be proven before anything else
matters.

**Phase 2 — `POST /dek/unwrap` (decrypt path).** Symmetric addition:
`dek_unwrap` scope, HSM unwrap via the existing path, transport re-wrap,
plus CLNT-side local decrypt and tag verification.

**Phase 3 — App keypair provisioning.** Extend `APP_ONBOARDING.md`'s
migration-based onboarding to register an app's public key. Build the still
-missing admin capability to revoke `dek_issue`/`dek_unwrap` (no admin
endpoint edits an existing app's scopes post-onboarding today for *any*
scope — this is new work regardless of Tier 3). Build it as a proper admin
endpoint, not an ad hoc SQL update — see `ADMIN_OPERATIONS.md`'s "Prefer
the admin API over direct SQL" for why (cache invalidation, audit trail).
`RUNBOOK.md` already documents the close-out procedure that depends on this
endpoint existing.

**Phase 4 — CLNT reference library: files.** Extend the record-path CLNT
library to the manifest-based chunking pattern already documented for
Tier 2 — same `/dek/issue`/`/dek/unwrap` primitive, once per chunk.

**Phase 5 — Deployment lifecycle.** Helm chart for hsm-bulk-service as an
independently scalable/undeployable Deployment; ops runbook for standing it
up for a scheduled onboarding/de-boarding window and tearing it down after
(extends `RUNBOOK.md`).

**Phase 6 — Pilot + measure.** Run one real onboarding case through it,
measure the actual SVC-load reduction directly (ties to the "Performance
testing and capture metrics" backlog item) — validates the motivating claim
with real numbers before any wider rollout, the same measure-before-build
discipline applied everywhere else in this doc.
