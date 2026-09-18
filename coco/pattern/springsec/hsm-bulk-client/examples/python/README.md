# Python reference: file encrypt/decrypt against hsm-core-service

Two reference modules, covering both directions of the same interoperability
guarantee: hsm-core-service's two DEK-issuance API shapes -- `/encrypt`/`/decrypt`
and `/dek/issue`/`/dek/unwrap` -- produce mutually decryptable ciphertext,
always, with no adapter beyond parsing bytes already sitting in the file or
token.

## `hsm_core_batch_file.py` — Tier 1: encrypt/decrypt directly, no /dek/issue

Chunk the file yourself, but send each chunk's actual data directly to
hsm-core-service's own `POST /encrypt/batch` and `POST /decrypt/batch`.
hsm-core-service does the real AES-256-GCM encryption server-side and hands
back one opaque ciphertext token per chunk. **No `/dek/issue`, no raw
DEK, ever.** This module never imports a crypto library at all — it's purely
HTTP + local file chunking + a JSON manifest. This is the reviewed,
foundational pattern described in `java/docs/BULK_OPERATIONS.md`'s "Files
with multiple chunks: chunking + stitch-back" section, which this file
follows directly.

## `hsm_bulk_file_reader.py` — read a REAL `hsm-bulk-client` file, decrypt via `/decrypt/batch` alone

Reads a file actually produced by `hsm-bulk-client`'s `FileBulkJob` (the
Tier 3 pipeline — local AES-GCM against a DEK obtained from
hsm-core-service's `/dek/issue`) and decrypts it purely through
hsm-core-service's own `/decrypt/batch` — `/dek/issue`/`/dek/unwrap` are
never contacted on this side at all. This is the direct proof that the two
API shapes' ciphertext is genuinely, mutually interoperable: `FileBulkJob`'s
own `reconstructCoreServiceToken()` method is what makes this possible, and
this module is a straight port of that same logic.

## Setup

```bash
pip install requests
# only if you use SELF_SIGNED_JWT auth (see auth.py):
pip install cryptography
# only if you use AZURE_AD auth (see auth.py):
pip install azure-identity
```

## Usage

```python
from hsm_core_batch_file import HsmCoreClient, encrypt_file, decrypt_file
from auth import StaticTokenProvider

client = HsmCoreClient(
    base_url="https://your-hsm-core-service",
    api_v1_prefix="/api/sensec/hsm/v1",
    app_id="your-app-id",
    token_provider=StaticTokenProvider("<bearer token>"),
)

# Tier 1: encrypt/decrypt directly, no /dek/issue
manifest = encrypt_file(client, "plain.pdf", "plain.pdf.manifest.json")
decrypt_file(client, "plain.pdf.manifest.json", "plain.pdf")

# Read a file hsm-bulk-client's FileBulkJob actually produced, via hsm-core-service alone
from hsm_bulk_file_reader import decrypt_bulk_file
decrypt_bulk_file(client, "customer-file.pdf", "customer-file-decrypted.pdf")
```

The manifest (`file_id`, `filename`, sizes, ordered ciphertext tokens, a
whole-file plaintext SHA-256) `encrypt_file`/`decrypt_file` use is **your own
record, in your own storage** — hsm-core-service doesn't create or store
anything like it. Keep it next to (or alongside metadata about) the file it
describes; you need it to decrypt later. `hsm_bulk_file_reader.py` needs no
such manifest — `FileBulkJob`'s own file already carries everything needed
(`edek_id` plus ordered frames).

## Verified against real, running services — both directions

Not just read from the DTO/Java source — both modules were run against real
local service instances (demo mode, H2, `MockJwtValidator`'s
`payments-svc`/`demo-token-payments-svc`), sharing one database:

- `hsm_core_batch_file.py`: a 37,000-byte plaintext at a 4,096-byte chunk
  size (10 real `/encrypt/batch` → `/decrypt/batch` round trips) — reassembled
  plaintext's SHA-256 matched the original exactly.
- `hsm_bulk_file_reader.py`: a 50,000-byte file encrypted by the **actual,
  compiled `hsm-bulk-client` jar** running a real `FileBulkJob` ENCRYPT job
  against a real `hsm-core-service` `/dek/issue` call — then decrypted by
  this Python module talking only to `hsm-core-service`'s `/decrypt/batch`.
  SHA-256 matched the original exactly.

## Auth — `auth.py`

`HsmCoreClient` takes a `TokenProvider`, not a raw token string — see
`auth.py`, which implements 3 of hsm-crypto-client's 4 `SvcConfig.AuthMode`
values:

- **`StaticTokenProvider`** — a fixed bearer token, sent as-is on every
  call. What the local demo-mode server accepts (`demo-token-payments-svc`,
  etc., see `MockJwtValidator.DEMO_TOKENS`) — fine for trying this module
  out, not a template for real auth (a real static Azure AD JWT here would
  expire mid-session).
- **`SelfSignedJwtTokenProvider`** — locally signs a short-lived RS256
  bearer assertion with this app's own private key, matching
  hsm-core-service's `SelfSignedAppKeyJwtValidator` exactly. No external IdP
  round trip; caches and only re-signs near expiry. Needs `cryptography`.
- **`AzureAdTokenProvider`** — acquires a real Entra ID token via
  `azure-identity`'s `DefaultAzureCredential` (environment → workload
  identity → managed identity → local-dev fallbacks), scoped to whatever
  hsm-core-service's own Azure AD app registration exposes. The natural fit
  for a caller that already runs under an Azure identity (e.g. an Azure
  Function under its own managed identity) with no key material to
  provision or rotate. Needs `azure-identity`.

**`MTLS` is not supported by this module.** It authenticates at the TLS
transport layer (a client cert/key on the connection itself), not via a
bearer token, so it doesn't fit `TokenProvider`'s shape — `HsmCoreClient`'s
`requests.Session` would need its own `cert=` wiring if that's ever needed.

Either way, getting the underlying credential material (a signing key, an
Azure AD app registration/managed identity) provisioned is outside this
module's scope — a matching row in `app_registrations` (`allowed_scopes`
including `encrypt`/`decrypt`) is still required regardless of auth mode,
see `java/docs/APP_ONBOARDING.md`.

`build_token_provider_from_env(app_id)` in `auth.py` selects and constructs
one of the three from `HSM_CORE_AUTH_MODE` (`STATIC` default,
`SELF_SIGNED_JWT`, or `AZURE_AD`) plus the matching env vars — used by both
modules' own `__main__` demo blocks, and reusable directly:

```bash
export HSM_CORE_AUTH_MODE=SELF_SIGNED_JWT
export HSM_CORE_SIGNING_PRIVATE_KEY_PEM_PATH=/path/to/private-key.pem
export HSM_CORE_SELF_SIGNED_AUDIENCE=hsm-core-service   # optional, this is the default

# or:
export HSM_CORE_AUTH_MODE=AZURE_AD
export HSM_CORE_AZURE_TOKEN_SCOPE=api://<hsm-core-service-app-id>/.default
```

## Two things worth knowing

**Chunk order isn't a documented server contract.** Each batch item has
its own caller-supplied `key` (here, the zero-based chunk index as a
string), echoed back on the result — this module always re-orders by that
`key` rather than assuming the response array order matches the request
order.

**The manifest + whole-file SHA-256 is the real integrity backstop.** Each
chunk's AEAD tag only proves *that chunk's* ciphertext wasn't tampered
with — it says nothing about whether reassembly dropped, duplicated, or
reordered a chunk. `decrypt_file` writes to a temp file, verifies the
SHA-256 against the manifest, and only renames into place on a match —
`target_path` is never left holding a partially-wrong file.
