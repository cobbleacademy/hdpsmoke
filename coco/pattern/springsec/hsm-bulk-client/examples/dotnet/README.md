# .NET reference: file encrypt/decrypt against hsm-core-service

Two reference classes, covering both directions of the same interoperability
guarantee: hsm-core-service's two DEK-issuance API shapes -- `/encrypt`/`/decrypt`
and `/dek/issue`/`/dek/unwrap` -- produce mutually decryptable ciphertext,
always, with no adapter beyond parsing bytes already sitting in the file or
token -- plus `Auth.cs`, the `ITokenProvider` implementations both classes
authenticate through (see "Auth" below).

## `HsmCoreBatchFile.cs` — Tier 1: encrypt/decrypt directly, no /dek/issue

Chunk the file yourself, but send each chunk's actual data directly to
hsm-core-service's own `POST /encrypt/batch` and `POST /decrypt/batch`.
hsm-core-service does the real AES-256-GCM encryption server-side and hands
back one opaque ciphertext token per chunk. **No `/dek/issue`, no raw
DEK, ever.** This file never touches `System.Security.Cryptography` at all —
it's purely `HttpClient` + local file chunking + a JSON manifest. This is the
reviewed, foundational pattern described in `java/docs/BULK_OPERATIONS.md`'s
"Files with multiple chunks: chunking + stitch-back" section, which this
file follows directly.

## `HsmBulkFileReader.cs` — read a REAL `hsm-bulk-client` file, decrypt via `/decrypt/batch` alone

Reads a file actually produced by `hsm-bulk-client`'s `FileBulkJob` (the
Tier 3 pipeline — local AES-GCM against a DEK obtained from
hsm-core-service's `/dek/issue`) and decrypts it purely through
hsm-core-service's own `/decrypt/batch` — `/dek/issue`/`/dek/unwrap` are
never contacted on this side at all. This is the direct proof that the two
API shapes' ciphertext is genuinely, mutually interoperable: `FileBulkJob`'s
own `reconstructCoreServiceToken()` method is what makes this possible, and
this class is a straight port of that same logic — deliberately never
constructing a `System.Guid` (see the file's own header comment for why
that matters here).

## Requirements

.NET 6 or later. No external NuGet package for STATIC or SELF_SIGNED_JWT
auth — `HttpClient`, `System.Net.Http.Json`, `System.Text.Json`, and
`System.Security.Cryptography.RSA` have all been part of the BCL/shared
framework since .NET Core 3.0/.NET 5. AZURE_AD auth needs the `Azure.Identity`
NuGet package (already referenced in `HsmCoreBatchFile.csproj`).

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
    tokenProvider: new StaticTokenProvider("<bearer token>"));

// Tier 1: encrypt/decrypt directly, no /dek/issue
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
  real `hsm-core-service` `/dek/issue` call — then decrypted by this C# class
  talking only to `hsm-core-service`'s `/decrypt/batch`. SHA-256 matched the
  original exactly.

## Auth — `Auth.cs`

`HsmCoreClient` takes an `ITokenProvider`, not a raw token string — see
`Auth.cs`, which implements 3 of hsm-crypto-client's 4 `SvcConfig.AuthMode`
values, ported directly from that Java package (`com.hsm.client.svc`):

- **`StaticTokenProvider`** — a fixed bearer token, sent as-is on every
  call. What the local demo-mode server accepts (`demo-token-payments-svc`,
  etc., see `MockJwtValidator.DEMO_TOKENS`) — fine for trying this class
  out, not a template for real auth (a real static Azure AD JWT here would
  expire mid-session).
- **`SelfSignedJwtTokenProvider`** — locally signs a short-lived RS256
  bearer assertion (hand-rolled via `System.Security.Cryptography.RSA`, no
  JWT NuGet package) with this app's own private key, matching
  hsm-core-service's `SelfSignedAppKeyJwtValidator` exactly — port of
  `SelfSignedJwtTokenProvider.java`. No external IdP round trip; caches and
  only re-signs near expiry.
- **`AzureAdTokenProvider`** — acquires a real Entra ID token via the same
  credential cascade as `AzureAdTokenProvider.java` (Workload Identity →
  explicit `AZURE_CLIENT_SECRET` → user-assigned Managed Identity → Azure
  CLI/PowerShell/system-assigned Managed Identity), scoped to whatever
  hsm-core-service's own Azure AD app registration exposes. The natural fit
  for a caller that already runs under an Azure identity (e.g. an Azure
  Function under its own managed identity) with no key material to
  provision or rotate. Needs the `Azure.Identity` NuGet package.

**`MTLS` is not supported by this class.** It authenticates at the TLS
transport layer (a client cert/key on the connection itself), not via a
bearer token, so it doesn't fit `ITokenProvider`'s shape — `HsmCoreClient`'s
`HttpClient` would need its own `HttpClientHandler.ClientCertificates`
wiring if that's ever needed.

Either way, getting the underlying credential material (a signing key, an
Azure AD app registration/managed identity) provisioned is outside this
class's scope — a matching row in `app_registrations` (`allowed_scopes`
including `encrypt`/`decrypt`) is still required regardless of auth mode,
see `java/docs/APP_ONBOARDING.md`.

`TokenProviderFactory.BuildFromEnv(appId)` in `Auth.cs` selects and
constructs one of the three from `HSM_CORE_AUTH_MODE` (`STATIC` default,
`SELF_SIGNED_JWT`, or `AZURE_AD`) plus the matching env vars — used by
`Program.cs`'s own demo, and reusable directly:

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
string), echoed back on the result — this class always re-orders by that
`key` rather than assuming the response array order matches the request
order.

**The manifest + whole-file SHA-256 is the real integrity backstop.** Each
chunk's AEAD tag only proves *that chunk's* ciphertext wasn't tampered
with — it says nothing about whether reassembly dropped, duplicated, or
reordered a chunk. `DecryptFileAsync` writes to a temp file, verifies the
SHA-256 against the manifest, and only renames into place on a match —
`targetPath` is never left holding a partially-wrong file.
