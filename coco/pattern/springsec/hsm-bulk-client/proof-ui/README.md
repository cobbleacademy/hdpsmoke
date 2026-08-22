# hsm-bulk-client decrypt proof (throwaway verification tool)

Not part of `hsm-bulk-client` itself, which stays headless/CLI-only by design
(`ClientApplication`'s own javadoc: "this is a batch job, not a server"). This
is a separate, standalone tool that drives the real `hsm-bulk-client.jar`
through a real ENCRYPT then DECRYPT run and proves the round trip is
byte-for-byte correct — built to answer a specific question ("does file
decrypt actually work, chunk-by-chunk, on a real file"), not a production
feature. No dependencies beyond Python 3's standard library + Pillow (only
used to generate the sample PDF, not for anything crypto-related).

## Prerequisites

1. **`hsm-core-service` and `hsm-bulk-service` both running**, sharing one H2
   file via `AUTO_SERVER=TRUE` — `hsm-bulk-service` has no seed data of its
   own, it relies on `hsm-core-service`'s `DemoSeedInitializer` having already
   created the `payments-svc` row:
   ```bash
   # terminal 1, from java/hsm-core-service/
   DEMO_MODE=true DEMO_DATABASE_URL="jdbc:h2:file:./demo_hsm_h2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;AUTO_SERVER=TRUE" \
     java -jar target/hsm-core-service.jar

   # terminal 2, from java/hsm-bulk-service/ -- absolute path avoids any
   # cwd-relative-path mismatch between the two services' H2 URLs
   DEMO_MODE=true DATABASE_URL="jdbc:h2:file:/absolute/path/to/hsm-core-service/demo_hsm_h2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;AUTO_SERVER=TRUE" \
     java -jar target/hsm-bulk-service.jar
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
