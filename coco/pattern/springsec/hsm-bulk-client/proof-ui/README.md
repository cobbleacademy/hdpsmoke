# hsm-bulk-client decrypt proof (throwaway verification tool)

Not part of `hsm-bulk-client` itself, which stays headless/CLI-only by design
(`ClientApplication`'s own javadoc: "this is a batch job, not a server"). This
is a separate, standalone tool that drives the real `hsm-bulk-client.jar`
through a real ENCRYPT then DECRYPT run and proves the round trip is
byte-for-byte correct — built to answer a specific question ("does file
decrypt actually work, chunk-by-chunk, on a real file"), not a production
feature. No dependencies beyond Python 3's standard library + Pillow (only
used to generate the sample PDF, not for anything crypto-related).

The **compress-before-encrypt** checkbox sets that flag on the ENCRYPT job
only (see `ClientProperties.File.compressBeforeEncrypt`) — check it, point
the path field at a compressible file, and the "Encrypted (intermediate)"
row will come out smaller than "Original" instead of ~33% larger.

## Prerequisites

1. **`hsm-core-service` running** (demo mode) -- its own `DemoSeedInitializer`
   creates the `payments-svc` row on startup; the `/dek/issue`/`/dek/unwrap`
   endpoints this client's BULK File path uses live on this same service:
   ```bash
   # from java/hsm-core-service/
   DEMO_MODE=true DEMO_DATABASE_URL="jdbc:h2:file:./demo_hsm_h2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;AUTO_SERVER=TRUE" \
     java -jar target/hsm-core-service.jar
   ```

2. **`payments-svc` provisioned with `dek_issue`/`dek_unwrap` scopes and a
   public key matching `demo-private-key.pem`** in this directory — the
   default demo seed only grants `encrypt`/`decrypt`, not the bulk-specific
   scopes (same provisioning `BulkVsBatchBenchmark` does programmatically).
   One-time, via H2's own Shell tool against the running file:
   ```bash
   java -cp ~/.m2/repository/com/h2database/h2/2.4.240/h2-2.4.240.jar org.h2.tools.Shell \
     -url "jdbc:h2:file:/absolute/path/to/hsm-core-service/demo_hsm_h2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;AUTO_SERVER=TRUE" \
     -user sa -password "" \
     -sql "UPDATE app_registrations SET allowed_scopes = 'encrypt,decrypt,dek_issue,dek_unwrap', public_key_pem = '$(cat demo-private-key.pem | openssl rsa -pubout)' WHERE app_id = 'payments-svc';"
   ```
   (Re-run this if you ever regenerate `demo-private-key.pem` — the public
   key registered here must match the private key in that file, or every
   `/dek/unwrap` call will fail with a decrypt error.)

3. **`hsm-bulk-client.jar` built**: `mvn -pl hsm-bulk-client package -DskipTests`
   from `java/`.

## Running it

```bash
python3 server.py --port 8000
open http://127.0.0.1:8000
```

(Use `127.0.0.1`, not `localhost` — some clients resolve `localhost` to `::1`
first and hang before falling back to IPv4, which is all this server binds.)

**Pointing at services that aren't the local demo defaults** (a remote
deployment, a different port, non-demo credentials): set these before
launching — all optional, all default to the local-demo values used
throughout this README:

```bash
export PROOF_UI_SVC_BASE_URL=http://<remote-host>:3005
export PROOF_UI_API_V1_PREFIX=/api/sensec/hsm/v1   # must match that deployment's own prefix
export PROOF_UI_APP_ID=payments-svc
export PROOF_UI_TOKEN=demo-token-payments-svc
python3 server.py --port 8000
```

Getting `PROOF_UI_SVC_BASE_URL` wrong (e.g. leaving it at `localhost` while
`hsm-core-service` runs elsewhere) surfaces as a generic `ENCRYPT run
failed` with a connection error in the log tail — check that field first if
`Run proof` fails against anything other than a local demo setup.

**If the log tail instead shows `PKIX path building failed`**, that's TLS
certificate trust, not connectivity — the JVM running the jar doesn't trust
whatever certificate a remote `hsm-core-service` presents over HTTPS (e.g.
self-signed, internal CA). The correct fix is importing that certificate
into the JVM's own trust store (`keytool -importcert -alias hsm-bulk-remote
-file server.crt -keystore "<java.home>/lib/security/cacerts" -storepass
changeit`, where `server.crt` comes from `keytool -printcert -sslserver
<host>:<port> -rfc > server.crt`). If that's not practical (e.g. a
throwaway internal test deployment you don't control the trust chain for),
`export SVC_INSECURE_TLS=true` before launching disables TLS verification
for the jar's connection to SVC only — testing only, never against a
deployment you don't fully control.

Type an **absolute path to a local file** in the text field, or leave it
blank to use the built-in sample, then click **Run proof**. It will:
1. Copy the chosen file — either your path, or `sample.pdf` (auto-generated
   on first run if missing/undersized: a random-noise image saved as a PDF,
   deliberately sized to at least 80MB so the run exercises multiple real
   chunks at the default 8 MiB `chunk-size-bytes`, not a trivial
   single-chunk case) — into a scratch `work/` directory.
2. Run a real ENCRYPT via `hsm-bulk-client.jar` against the real running
   services.
3. Run a real DECRYPT of that output.
4. SHA-256 the original and the round-tripped result, compare them.
5. Render both PDFs in the browser side by side.

Hash comparison works for any file type; the browser preview panes assume
PDF specifically (same as this whole tool's original motivation) — a
non-PDF file still gets correctly verified, it just won't render inline.

## Proving chunked read/decrypt from a real ADLS or Blob container

By default the encrypted intermediate lives in a local scratch dir between
ENCRYPT and DECRYPT — proving `LocalFileStore`'s I/O, not `AdlsFileStore`'s
or `AzureBlobFileStore`'s. To prove one of those instead (a real Azure
container, read back in chunks and decrypted, not just local disk): set
`PROOF_UI_AZURE_ROOT` before launching, then pick the matching option in
the UI's "encrypted intermediate" selector.

**Which one to pick is not a preference — it depends on your storage
account**, and the two are not interchangeable:
- **ADLS Gen2** requires Hierarchical Namespace (HNS) enabled on the
  account. Root: `abfss://<container>@<account>.dfs.core.windows.net/<path>`
  (`AdlsFileStore`).
- **Azure Blob Storage** needs no HNS, and has no known conflict with the
  account-level "soft delete for blobs" feature the way ADLS Gen2's Data
  Lake REST API does — a real, Microsoft-documented incompatibility
  (`EndpointUnsupportedAccountFeatures`, see `AzureBlobFileStore.java`'s
  class javadoc). Use this one if HNS is off, or if blob soft delete is on.
  Root: `https://<account>.blob.core.windows.net/<container>/<path>`
  (`AzureBlobFileStore`).

```bash
export PROOF_UI_AZURE_ROOT="<the appropriate URI for whichever you picked>"
python3 server.py --port 8000
```

The ENCRYPT job then writes its chunked, encrypted output straight to that
location instead of local disk, and the DECRYPT job reads it back from
there — this script itself never touches Azure storage directly (no Azure
SDK dependency added); the real jar does all of it via the matching
`FileStore` class, the same one a real deployment uses. The plaintext
source and the final decrypted output still live locally either way, since
those are what get hashed and served for the browser preview regardless of
where the encrypted bytes sat in between. The "Encrypted (intermediate)"
row in the results table shows `n/a` for size in this mode — this script
deliberately doesn't add an Azure SDK dependency just to read that one
number back; the jar's own `hsm_bulk_client_complete` log line is what
actually proves the write/read happened, same as everywhere else this tool
relies on the real jar rather than re-checking its work.

**Every run reuses one fixed `proof-ui-scratch` subfolder under
`PROOF_UI_AZURE_ROOT`, not the bare root itself.** `FileBulkJob`'s DECRYPT
lists every file under its source root — pointing that at the bare shared
root means a file left over from something else (unrelated manual testing
against the same container) gets swept into this run's decrypt attempt
too, and parsing something that was never in `FileBulkJob`'s wire format
fails in confusing ways (found live: exactly this, via a fixed `input.pdf`
working name at a shared root). A dedicated, clearly-named scratch
subfolder avoids that, as long as nothing else is deliberately placed
inside it. It's deliberately the *same* subfolder on every run, not a
fresh one each time — a fresh-per-run subfolder was tried and reverted,
since it never collides but also never gets cleaned up, and this script
adds no Azure SDK dependency to delete anything. One fixed name means each
run's ENCRYPT just overwrites its one working file there, the same way
`work/` gets reused (not recreated from scratch) on local disk.

Credentials resolve automatically (`WorkloadIdentityCredential` →
`ManagedIdentityCredential` → `DefaultAzureCredential` — e.g. `az login`
works from a dev machine) unless `PROOF_UI_AZURE_ACCOUNT_KEY` is also set,
which forces `StorageSharedKeyCredential` instead. That's a deliberate
local-testing-only escape hatch (see `ClientProperties.File.StoreRef`'s
javadoc) for validating against a real container before the deployment
identity's RBAC data-plane role is actually granted — never set it for a
real deployment's own config.

## What "no live per-chunk progress" means here

Deliberate, not an oversight: `FileBulkJob` only logs progress at the file
level (`files_done=N`), not per-chunk within one file, and there was no
reason to add chunk-level logging just for this tool. This runs the real
pipeline to completion, then shows the result — not a live progress bar.

## Verified live (2026-08-22)

89.9 MB sample PDF (94,253,087 bytes), 12 chunks at 8 MiB each. Encrypt:
8.0s. Decrypt: 6.84s. Result: `match: true` — decrypted SHA-256 identical
to the original.

Encrypted intermediate: 125,671,200 bytes — **33.33% larger**, not the
~400-byte framing overhead this note previously reported. That earlier
number described `FileBulkJob`'s old wire format; the current format
base64-encodes each chunk's plaintext before encryption (so the ciphertext
is always safely decryptable via `hsm-core-service`'s own `/decrypt` too,
not just this tool's local decrypt path — see `FileBulkJob.java`'s class
javadoc). 125,671,200 / 94,253,087 = 1.3333 exactly the 4:3 ratio base64
encoding always produces, confirming the overhead is precisely what the
new format predicts, nothing extra or missing.

(Historical, for reference: the 2026-08-20 run against the previous format
reported `match: true` with the file only 400 bytes larger — 16-byte
`edek_id` header + 12 chunks × 32 bytes IV+tag each. That format has since
been replaced; see `FileBulkJob.java`'s class javadoc and
`java/docs/BULK_OPERATIONS.md` for why.)

## Verified live: compress-before-encrypt + cross-service interop (2026-08-22, later round)

`compress-before-encrypt: true` verified end-to-end against a real
`hsm-bulk-client.jar` run, not just unit tests. Source: 235,897 bytes of
concatenated Java source (genuinely compressible text).

**Via this proof-ui tool itself**, checkbox checked, path field pointed at
the source bundle: encrypted intermediate came out at **72,624 bytes**
(smaller than the 235,897-byte original, despite AES-GCM + base64 overhead)
— confirming gzip is actually applied before encryption, not just accepted
as a no-op config flag, and that the checkbox correctly threads through to
`ClientProperties.File.compressBeforeEncrypt` on the ENCRYPT job only.
`match: true`. The unchecked/default path (built-in ~90 MB sample, no
compression) was re-run immediately after as a regression check — still
`match: true`, encrypted size still the expected 4:3 base64 ratio — since
the checkbox required a shared-code change (`write_job_yaml`'s signature).

Three further decrypt paths, same source file, same SHA-256
(`f7688061a0dc608f00bb72b5dfbf6ae6b8efef1ca7f19147fdcf25ded3e102c4`) on
every one:
1. **Remote decrypt** via `hsm_bulk_file_reader.py`
   (`examples/python/hsm_bulk_file_reader.py`) against `hsm-core-service`
   directly — `hsm-bulk-service` never contacted for this path.
2. **Remote decrypt** via `HsmBulkFileReader.cs`
   (`examples/dotnet/HsmBulkFileReader.cs`, `dotnet run -- <enc> <out>`
   against `HsmCoreClient`), same real `hsm-core-service`, same
   `hsm-bulk-service`-never-contacted shape. A .NET 8 SDK wasn't installed
   in this environment at first pass — installed user-locally (no `sudo`)
   via `dot.net/v1/dotnet-install.sh --channel 8.0` specifically to run
   this check for real rather than claim the port was equivalent by
   inspection.
3. **`CoreBulkFileInteropTest`**'s
   `compressedChunks_decryptCorrectlyBothLocallyAndViaCoreService()` (Java,
   automated, runs on every `mvn test`) — both local and remote decrypt,
   spawned fresh service instances, asserted rather than eyeballed.

All three languages (Java, Python, .NET) are now genuinely live-verified
against the compression feature, not just structurally similar by
inspection.
