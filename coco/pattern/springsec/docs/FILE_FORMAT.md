# Encrypted File Format (v1 and v2)

Normative spec for the chunked encrypted-file layout written by
`hsm-bulk-client` and read by `hsm-bulk-client`, `hsm-file-service`,
`HsmCryptoClient` embedders and the Python/.NET example readers.

- **Reference implementation:** `hsm-crypto-client`,
  `com.hsm.client.fileformat` (`EncryptedFileFormat`, `ChunkPayload`,
  `EncryptedFileWriter`, `EncryptedFileReader`). Every Java reader and writer
  in this repo goes through those classes; nothing else frames bytes.
- **Golden files:** `hsm-crypto-client/src/test/resources/golden/`. These are
  fixed ciphertexts, with the test key and inputs in `GoldenVectors.java`.
  Any new reader, in any language, must decrypt all five and reject the
  tamper cases below.
- If this document and the code disagree, the golden files decide.

## Why v2 exists

v1 authenticates each chunk on its own, but nothing ties a chunk to its
position, its file, or the end of the file. Someone who can write to storage,
without having the key, can:

- drop trailing chunks, which yields a valid, shorter file;
- reorder or duplicate chunks;
- splice chunks from another file encrypted under the same named DEK.

v1 then returns wrong bytes with no error. v2 closes all three, keeps the
format streamable, and leaves hsm-core-service unchanged.

## Layout

All integers are big-endian. UUIDs are 16 bytes: most-significant 8 bytes,
then least-significant 8 bytes (Java `UUID` order, *not* .NET `Guid` order).

### Header

| Version | Bytes | Content |
|---|---|---|
| v1 | 16 | `edek_id` |
| v2 | 41 | `"HSMF"` (4) · `0x02` (1) · `edek_id` (16) · `file_id` (16) · `chunk_size` (int32) |

- `file_id`: random version-4 UUID, new for **every** encryption (including
  re-encrypting the same content), drawn from the BC-FIPS DRBG. It is v4 and
  not v7 because the header is not encrypted, and a v7 id would reveal when
  each file was created.
- `chunk_size`: plaintext bytes per chunk before compression and base64
  (1 … 2³¹−1). Every non-final chunk holds exactly this many bytes.

### Frames (identical in v1 and v2)

```
repeat until end of stream:
    length      int32         = 28 + len(ciphertext)
    iv          12 bytes      random, per chunk
    tag         16 bytes      AES-GCM tag
    ciphertext  length − 28 bytes
```

The stream must end exactly on a frame boundary.

### Chunk encryption (identical in v1 and v2)

```
ciphertext, tag = AES-256-GCM(key = DEK, iv,
                              aad = UTF-8("hsm-svc:app_id=" + owner_app_id),
                              plaintext = UTF-8(base64(chunk_plaintext)))
```

- `owner_app_id` is the DEK's owner as returned by `/dek/issue` or
  `/dek/unwrap`, never the caller's own app_id. They differ under a
  cross-app grant.
- The base64 layer exists because hsm-core-service's `/decrypt` returns the
  plaintext as a UTF-8 string. Base64 keeps any single chunk decryptable
  there, which is the rescue path.

### Chunk plaintext (what is base64-encoded, then encrypted)

| Version | Layout |
|---|---|
| v1 | `marker` (1) · `payload` |
| v2 | `marker` (1) · `file_id` (16) · `chunk_index` (int64) · `is_final` (1: `0x00`/`0x01`) · `chunk_size` (int32) · `payload` |

| Marker | Meaning |
|---|---|
| `0x00` | v1, raw payload |
| `0x01` | v1, gzip payload |
| `0x02` | v2, raw payload |
| `0x03` | v2, gzip payload |

`payload` is the chunk's original bytes, gzip-compressed when the marker says
so. For v2, the decompressed payload must be at most `chunk_size` bytes.

**Why the v2 binding fields sit inside the plaintext, not in the AAD.** Leaving
the AAD exactly as in v1 is what lets hsm-core-service's existing `/decrypt`
decrypt any v2 chunk, with no core change. The fields are still tamper-proof
because they are inside the AES-GCM-protected plaintext. The trade-off is
that the **reader** must enforce them, not the cipher. That is why there is
one codec, golden files, and tamper tests for every reader.

### Empty files

- v1: header only, no frames.
- v2: one frame, `chunk_index = 0`, `is_final = 1`, empty payload. This way,
  cutting a file down to its header is detectable.

## Reader rules

**Version detection.** First 5 bytes equal `"HSMF" 0x02` → v2. Otherwise → v1,
and the first 16 bytes are the `edek_id`. A v1 file matches by accident only
if its random edek_id starts with those 5 bytes, about 1 in 2⁴⁰. The marker
rule below catches even that case.

**For every chunk at 0-based position *i*:**

1. Frame length must be greater than 28 and within the reader's frame limit:

   | Reader | Frame limit |
   |---|---|
   | Library default | 64 MiB |
   | hsm-file-service default | 16 MiB |
   | v2 file | Also bounded by `28 + base64(1 + 29 + gzipBound(chunk_size))` |

2. AES-GCM must verify. Otherwise the reason is `AUTH_FAILED`.
3. Marker must match the header version. A v2 marker in a v1-looking file is
   `VERSION_MISMATCH`: the v2 header was stripped, a **downgrade attempt**.
4. v2 only:

   | Check | Reason on failure |
   |---|---|
   | `file_id` equals the header's | `FILE_ID_MISMATCH` (spliced) |
   | `chunk_index` equals *i* | `CHUNK_OUT_OF_ORDER` |
   | `chunk_size` equals the header's | `HEADER_MISMATCH` |
   | Non-final payload is exactly `chunk_size` bytes | `HEADER_MISMATCH` |

5. Decompression is bounded, so a crafted gzip bomb is `LIMIT_EXCEEDED` rather
   than an out-of-memory crash.

**For the file as a whole (v2):**

- exactly one chunk has `is_final = 1`, and it is the last;
- any frame after it is `TRAILING_DATA`;
- end of stream before it is `TRUNCATED`.

**Streaming.** A reader may release each chunk's bytes once that chunk passes.
A failure in a later chunk then means earlier bytes were already released. A
caller that must never expose partial content verifies the whole file first.
`hsm-file-service` does this for files up to its buffer threshold.

## What v2 detects

| Tampering by someone with storage write access and no key | v1 | v2 |
|---|---|---|
| Change any ciphertext bit | detected | detected |
| Drop trailing chunks | **missed** | `TRUNCATED` |
| Cut mid-frame | detected (`TRUNCATED`) | detected |
| Reorder / duplicate chunks | **missed** | `CHUNK_OUT_OF_ORDER` |
| Splice a chunk from another file with the same named DEK | **missed** | `FILE_ID_MISMATCH` |
| Swap in another file's header | n/a | `FILE_ID_MISMATCH` |
| Strip the v2 header (downgrade) | n/a | `VERSION_MISMATCH` |
| Append frames after the end | **missed** | `TRAILING_DATA` |
| Replace the **whole file** with another valid file under the same app | missed | **missed by the format.** Use `X-Expected-File-Id`, below. |

**Whole-file swap.** Pass the `file_id` recorded at write time (bulk-client's
result files) to `hsm-file-service` as `X-Expected-File-Id`. The service then
refuses any other file with `412`. This only protects if the recorded
`file_id` lives in a store the attacker can't write, such as the consumer's
database. Blob metadata doesn't qualify.

## Chunk size

| Use | Recommended `chunk_size` | Why |
|---|---|---|
| Bulk jobs (default) | 8 MiB (`8388608`) | Throughput; fewer frames |
| Files a UI opens through `hsm-file-service` | **1 MiB** (`1048576`) | A chunk is verified before any of it is sent. Smaller chunks give a faster first byte and use about 4 MB instead of about 30 MB of heap per download. |
| With a named (shared) DEK | ≥ 64 KiB | AES-GCM with random 96-bit IVs has a budget of about 2³² encryptions per key. At 64 KiB that is about 256 TiB per key; at 1 MiB about 4 PiB. Named-DEK rotation resets it. |

Per-chunk overhead is 32 bytes of frame plus 29 bytes of v2 binding, then
base64's 4/3. The stored file is about 1.33× the original.

## Key scope for high-volume file sets

The default is one DEK per file. At billions of files, that means billions of
`/dek/issue` calls, HSM wrap operations and key records in core. Use a
**named DEK** per dataset, tenant or retention period instead. The scope you
choose is the smallest unit you can crypto-shred. See `BULK_OPERATIONS.md`,
"Key scope for high-volume file sets".

## Rescue: recovering a file through hsm-core-service alone

Any single chunk, v1 or v2, rebuilds into the exact token core's `/encrypt`
produces:

```
"v1." + base64url_with_padding(0x01 · edek_id · iv · tag · ciphertext)
```

Send it to `POST /decrypt` or `/decrypt/batch`. The returned `plaintext` is
the chunk's base64 text; decode it and apply the reader rules above. Tools:

- Java: `EncryptedFileReader.open(in).decryptTo(out, chunkDecryptor)`, where
  the decryptor calls core. `EncryptedFileReader.toCoreServiceToken` builds
  the token.
- Python: `hsm-bulk-client/examples/python/hsm_bulk_file_reader.py`, verified
  against the golden files.
- .NET: `hsm-bulk-client/examples/dotnet/HsmBulkFileReader.cs`, a port of the
  Python reader.
- `CoreBulkFileInteropTest.v2File_rescuedChunkByChunkViaCoreDecrypt_keepsIntegrityChecks`
  proves this against a real hsm-core-service on every full build.

The rescuing app needs `decrypt` on the owner's keys: either it is the owner,
or it holds a grant (`AUTHORIZATION.md` §1d).

## Rollout: readers first

An old reader, one that only knows v1, reads a v2 header's first 16 bytes
(`HSMF` 0x02 + 11 bytes of the real id) as an edek_id. `/dek/unwrap` answers
"EDEK not found". So an old reader **fails loudly and never outputs wrong
bytes**, but it does fail. Hence:

1. Ship readers that understand both versions. Writers stay on v1 (the default).
   - This change: `hsm-crypto-client`, `hsm-bulk-client`, `hsm-file-service`,
     and the Python and .NET example readers.
   - **Also any consumer-owned copy of the example readers.** Tell consumers
     before step 2.
2. Confirm every reader of a job's output is upgraded. Then set
   `file.format-version: 2` on that job. It is per job, so roll out per data
   set.
3. Later, make v2 the default. Keep reading v1 indefinitely.

Rewriting existing v1 files as v2 is optional: run a `hsm-bulk-client`
decrypt job, then an encrypt job with `format-version: 2`. Every rewritten file
gets a new `file_id`, so consumers must reload the new result files.

## Changing this format

A new layout is a new version byte (`0x03`), a new marker pair, new golden
files, and the same readers-first rollout. Never change the meaning of an
existing version.
