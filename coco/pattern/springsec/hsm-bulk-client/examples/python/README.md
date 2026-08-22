# Python reference: file encrypt/decrypt against hsm-core-service

Two reference modules, covering both directions of the same interoperability
guarantee: `hsm-core-service` and `hsm-bulk-service` ciphertext is mutually
decryptable, always, with no adapter beyond parsing bytes already sitting in
the file or token.

## `hsm_core_batch_file.py` — Tier 1: encrypt/decrypt directly, no bulk-service

Chunk the file yourself, but send each chunk's actual data directly to
hsm-core-service's own `POST /encrypt/batch` and `POST /decrypt/batch`.
hsm-core-service does the real AES-256-GCM encryption server-side and hands
back one opaque ciphertext token per chunk. **No `hsm-bulk-service`, no raw
DEK, ever.** This module never imports a crypto library at all — it's purely
HTTP + local file chunking + a JSON manifest. This is the reviewed,
foundational pattern described in `java/docs/BULK_OPERATIONS.md`'s "Files
with multiple chunks: chunking + stitch-back" section, which this file
follows directly.

## `hsm_bulk_file_reader.py` — read a REAL `hsm-bulk-client` file, decrypt via `hsm-core-service` alone

Reads a file actually produced by `hsm-bulk-client`'s `FileBulkJob` (the
Tier 3 pipeline — local AES-GCM against a DEK obtained from
`hsm-bulk-service`'s `/dek/issue`) and decrypts it purely through
`hsm-core-service`'s own `/decrypt/batch` — `hsm-bulk-service` is never
contacted on this side at all. This is the direct proof that the two
services' ciphertext is genuinely, mutually interoperable: `FileBulkJob`'s
own `reconstructCoreServiceToken()` method is what makes this possible, and
this module is a straight port of that same logic.

## Setup

```bash
pip install requests
```

## Usage

```python
from hsm_core_batch_file import HsmCoreClient, encrypt_file, decrypt_file

client = HsmCoreClient(
    base_url="https://your-hsm-core-service",
    api_v1_prefix="/api/sensec/hsm/v1",
    app_id="your-app-id",
    token="<bearer token>",
)

# Tier 1: encrypt/decrypt directly, no hsm-bulk-service
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
  against a real `hsm-bulk-service` `/dek/issue` call — then decrypted by
  this Python module talking only to `hsm-core-service`. SHA-256 matched the
  original exactly.

## Auth — the one thing this module can't do for you

Getting a bearer token is out of scope: a real deployment uses an Entra
ID/Azure AD app registration doing OAuth2 client-credentials against
hsm-core-service's own `JWT_AUDIENCE`/`JWT_ISSUER`, with a matching row in
`app_registrations` (`allowed_scopes` including `encrypt`/`decrypt`) —
neither of which is a live API call, see `java/docs/APP_ONBOARDING.md`. A
local demo-mode server instead accepts one of a handful of fixed literal
strings (`demo-token-payments-svc`, etc.) — useful for trying this module
out, not a template for real auth.

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
