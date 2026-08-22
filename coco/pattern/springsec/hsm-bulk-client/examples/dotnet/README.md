# .NET reference: file encrypt/decrypt against hsm-core-service

Two reference classes, covering both directions of the same interoperability
guarantee: `hsm-core-service` and `hsm-bulk-service` ciphertext is mutually
decryptable, always, with no adapter beyond parsing bytes already sitting in
the file or token.

## `HsmCoreBatchFile.cs` — Tier 1: encrypt/decrypt directly, no bulk-service

Chunk the file yourself, but send each chunk's actual data directly to
hsm-core-service's own `POST /encrypt/batch` and `POST /decrypt/batch`.
hsm-core-service does the real AES-256-GCM encryption server-side and hands
back one opaque ciphertext token per chunk. **No `hsm-bulk-service`, no raw
DEK, ever.** This file never touches `System.Security.Cryptography` at all —
it's purely `HttpClient` + local file chunking + a JSON manifest. This is the
reviewed, foundational pattern described in `java/docs/BULK_OPERATIONS.md`'s
"Files with multiple chunks: chunking + stitch-back" section, which this
file follows directly.

## `HsmBulkFileReader.cs` — read a REAL `hsm-bulk-client` file, decrypt via `hsm-core-service` alone

Reads a file actually produced by `hsm-bulk-client`'s `FileBulkJob` (the
Tier 3 pipeline — local AES-GCM against a DEK obtained from
`hsm-bulk-service`'s `/dek/issue`) and decrypts it purely through
`hsm-core-service`'s own `/decrypt/batch` — `hsm-bulk-service` is never
contacted on this side at all. This is the direct proof that the two
services' ciphertext is genuinely, mutually interoperable: `FileBulkJob`'s
own `reconstructCoreServiceToken()` method is what makes this possible, and
this class is a straight port of that same logic — deliberately never
constructing a `System.Guid` (see the file's own header comment for why
that matters here).

## Requirements

.NET 6 or later. No external NuGet package — `HttpClient`,
`System.Net.Http.Json`, and `System.Text.Json` have all been part of the
BCL/shared framework since .NET Core 3.0/.NET 5.

```bash
dotnet run   # builds and runs the demo in Program.cs against a real hsm-core-service
```

## Usage

```csharp
using Hsm.BulkClient.Examples;

var client = new HsmCoreClient(
    baseUrl: "https://your-hsm-core-service",
    apiV1Prefix: "/api/sensec/hsm/v1",
    appId: "your-app-id",
    token: "<bearer token>");

// Tier 1: encrypt/decrypt directly, no hsm-bulk-service
FileManifest manifest = await HsmCoreBatchFile.EncryptFileAsync(client, "plain.pdf", "plain.pdf.manifest.json");
await HsmCoreBatchFile.DecryptFileAsync(client, "plain.pdf.manifest.json", "plain.pdf");

// Read a file hsm-bulk-client's FileBulkJob actually produced, via hsm-core-service alone
await HsmBulkFileReader.DecryptBulkFileAsync(client, "customer-file.pdf", "customer-file-decrypted.pdf");
```

The manifest (`file_id`, `filename`, sizes, ordered ciphertext tokens, a
whole-file plaintext SHA-256) `HsmCoreBatchFile` uses is **your own record,
in your own storage** — hsm-core-service doesn't create or store anything
like it. Keep it next to (or alongside metadata about) the file it
describes; you need it to decrypt later. `HsmBulkFileReader` needs no such
manifest — `FileBulkJob`'s own file already carries everything needed
(`edek_id` plus ordered frames).

## Verified against real, running services — both directions

Not just read from the DTO/Java source — both classes were run against real
local service instances (demo mode, H2, `MockJwtValidator`'s
`payments-svc`/`demo-token-payments-svc`), sharing one database:

- `HsmCoreBatchFile`: a 37,000-byte plaintext at a 4,096-byte chunk size (10
  real `/encrypt/batch` → `/decrypt/batch` round trips) — reassembled
  plaintext's SHA-256 matched the original exactly.
- `HsmBulkFileReader`: a 50,000-byte file encrypted by the **actual, compiled
  `hsm-bulk-client` jar** running a real `FileBulkJob` ENCRYPT job against a
  real `hsm-bulk-service` `/dek/issue` call — then decrypted by this C# class
  talking only to `hsm-core-service`. SHA-256 matched the original exactly.

## Auth — the one thing this class can't do for you

Getting a bearer token is out of scope: a real deployment uses an Entra
ID/Azure AD app registration doing OAuth2 client-credentials against
hsm-core-service's own `JWT_AUDIENCE`/`JWT_ISSUER`, with a matching row in
`app_registrations` (`allowed_scopes` including `encrypt`/`decrypt`) —
neither of which is a live API call, see `java/docs/APP_ONBOARDING.md`. A
local demo-mode server instead accepts one of a handful of fixed literal
strings (`demo-token-payments-svc`, etc.) — useful for trying this class
out, not a template for real auth.

## Two things worth knowing

**Chunk order isn't a documented server contract.** Each batch item has
its own caller-supplied `key` (here, the zero-based chunk index as a
string), echoed back on the result — this class always re-orders by that
`key` rather than assuming the response array order matches the request
order.

**The manifest + whole-file SHA-256 is the real integrity backstop.** Each
chunk's AEAD tag only proves *that chunk's* ciphertext wasn't tampered
with — it says nothing about whether reassembly dropped, duplicated, or
reordered a chunk. `DecryptFileAsync` writes to a temp file, verifies the
SHA-256 against the manifest, and only renames into place on a match —
`targetPath` is never left holding a partially-wrong file.
