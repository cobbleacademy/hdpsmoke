"""
Reference implementation, in Python, of reading a REAL hsm-bulk-client
FileBulkJob-produced file -- format v1 or v2 -- and decrypting it via
hsm-core-service's own POST /decrypt/batch directly. This is also the
"rescue" path: any encrypted file can be recovered through core alone, with
no hsm-bulk-client or hsm-file-service involved.

The normative format spec is java/docs/FILE_FORMAT.md; the Java codec
(hsm-crypto-client's EncryptedFileFormat / ChunkPayload / EncryptedFileReader)
is the reference implementation. If this module and the Java code ever
disagree, the Java code and the golden files in
hsm-crypto-client/src/test/resources/golden/ win.

File layout:

    v1 header (16 B):  edek_id (big-endian UUID)
    v2 header (41 B):  b"HSMF" | 0x02 | edek_id(16) | file_id(16) | chunk_size(int32 BE)
    frames (both):     repeat { length(int32 BE) | iv(12) | tag(16) | ciphertext }

Detection: first 5 bytes == b"HSMF\\x02" means v2, anything else v1. The marker
byte inside each decrypted chunk must agree (0x00/0x01 = v1, 0x02/0x03 = v2);
a v2 marker inside a v1-looking file means the v2 header was stripped
(downgrade attempt) and is rejected.

Each frame reconstructs to the exact token hsm-core-service's /encrypt emits:

    "v1." + base64url(0x01 + edek_id(16) + iv(12) + tag(16) + ciphertext)

and core's /decrypt returns the chunk plaintext, which is base64 text of:

    v1: marker(0x00 raw | 0x01 gzip) | payload
    v2: marker(0x02 raw | 0x03 gzip) | file_id(16) | chunk_index(int64 BE)
        | is_final(0x00|0x01) | chunk_size(int32 BE) | payload

For v2 this module enforces what the Java reader enforces: every chunk's
file_id matches the header, chunk_index equals its position, chunk_size
matches, non-final chunks are exactly chunk_size bytes, exactly one final
chunk comes last, and nothing follows it. Anything else raises
FileIntegrityError -- the output file is never left looking complete.

Dependency: pip install requests (via hsm_core_batch_file's HsmCoreClient).
Auth is a TokenProvider, not a raw token -- see auth.py.
"""

from __future__ import annotations

import base64
import os
import struct
import uuid
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

IV_LENGTH = 12
TAG_LENGTH = 16
FRAME_OVERHEAD = IV_LENGTH + TAG_LENGTH
TOKEN_VERSION = b"\x01"
TOKEN_PREFIX = "v1."

V2_MAGIC = b"HSMF\x02"
V1_HEADER_BYTES = 16
V2_HEADER_BYTES = 41
V2_BINDING_BYTES = 16 + 8 + 1 + 4

# Hard caps whatever a (possibly tampered) header claims -- same defaults as
# hsm-file-service. Raise them only for files written with chunks > ~11.9 MiB.
MAX_FRAME_BYTES = 16 * 1024 * 1024
MAX_CHUNK_BYTES = 12 * 1024 * 1024

_FRAME_LEN_FMT = ">i"  # signed 4-byte big-endian int, matches DataOutputStream.writeInt/readInt


class FileIntegrityError(ValueError):
    """The file is not an authentic, complete encrypted file (truncated, reordered, spliced, downgraded...)."""


@dataclass(frozen=True)
class FileHeader:
    version: int
    edek_id_bytes: bytes
    file_id: uuid.UUID | None
    chunk_size: int


def read_header(data: bytes) -> FileHeader:
    if len(data) >= len(V2_MAGIC) and data[:len(V2_MAGIC)] == V2_MAGIC:
        if len(data) < V2_HEADER_BYTES:
            raise FileIntegrityError("truncated v2 header")
        edek = data[5:21]
        file_id = uuid.UUID(bytes=data[21:37])
        (chunk_size,) = struct.unpack(">i", data[37:41])
        if chunk_size <= 0 or chunk_size > MAX_CHUNK_BYTES:
            raise FileIntegrityError(f"v2 header chunk_size {chunk_size} out of range")
        return FileHeader(2, edek, file_id, chunk_size)
    if len(data) < V1_HEADER_BYTES:
        raise FileIntegrityError("too short to contain a 16-byte edek_id header")
    return FileHeader(1, data[:16], None, 0)


def read_frames(data: bytes, header: FileHeader) -> list[tuple[bytes, bytes, bytes]]:
    """Returns [(iv, tag, ciphertext)] in file order; raises on a cut-off or oversized frame."""
    pos = V2_HEADER_BYTES if header.version == 2 else V1_HEADER_BYTES
    frames = []
    while pos < len(data):
        if pos + 4 > len(data):
            raise FileIntegrityError(f"truncated frame-length field at frame {len(frames)}")
        (frame_len,) = struct.unpack(_FRAME_LEN_FMT, data[pos:pos + 4])
        pos += 4
        if frame_len <= FRAME_OVERHEAD or frame_len > MAX_FRAME_BYTES:
            raise FileIntegrityError(f"frame {len(frames)} has invalid length {frame_len}")
        if pos + frame_len > len(data):
            raise FileIntegrityError(f"truncated frame body at frame {len(frames)}")
        frame = data[pos:pos + frame_len]
        pos += frame_len
        frames.append((frame[:IV_LENGTH], frame[IV_LENGTH:FRAME_OVERHEAD], frame[FRAME_OVERHEAD:]))
    return frames


def reconstruct_core_service_token(edek_id_bytes: bytes, iv: bytes, tag: bytes, ciphertext: bytes) -> str:
    """Ports EncryptedFileReader.toCoreServiceToken() / FileBulkJob.reconstructCoreServiceToken() exactly."""
    payload = TOKEN_VERSION + edek_id_bytes + iv + tag + ciphertext
    return TOKEN_PREFIX + base64.urlsafe_b64encode(payload).decode("ascii")


def _bounded_gunzip(data: bytes, limit: int, index: int) -> bytes:
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    out = d.decompress(data, limit + 1)
    if len(out) > limit or d.unconsumed_tail:
        raise FileIntegrityError(f"chunk {index} decompresses beyond limit {limit}")
    return out + d.flush()


def decode_chunk(header: FileHeader, index: int, base64_plaintext: str) -> tuple[bytes, bool]:
    """Decodes one decrypted chunk and applies the per-chunk rules. Returns (payload, is_final)."""
    try:
        marked = base64.b64decode(base64_plaintext, validate=True)
    except ValueError as e:
        raise FileIntegrityError(f"chunk {index} is not valid base64") from e
    if not marked:
        raise FileIntegrityError(f"chunk {index} is empty")
    marker = marked[0]
    if marker not in (0x00, 0x01, 0x02, 0x03):
        raise FileIntegrityError(f"chunk {index} has unknown marker {marker:#04x}")
    if header.version == 1 and marker in (0x02, 0x03):
        raise FileIntegrityError(f"chunk {index} is a v2 chunk in a v1-looking file (header stripped / downgrade)")
    if header.version == 2 and marker in (0x00, 0x01):
        raise FileIntegrityError(f"chunk {index} is a v1 chunk in a v2 file")
    gz = marker in (0x01, 0x03)

    if header.version == 1:
        body = marked[1:]
        payload = _bounded_gunzip(body, MAX_CHUNK_BYTES, index) if gz else body
        return payload, False

    if len(marked) < 1 + V2_BINDING_BYTES:
        raise FileIntegrityError(f"chunk {index} too short for v2 binding fields")
    file_id = uuid.UUID(bytes=marked[1:17])
    (chunk_index,) = struct.unpack(">q", marked[17:25])
    final_flag = marked[25]
    (chunk_size,) = struct.unpack(">i", marked[26:30])
    if file_id != header.file_id:
        raise FileIntegrityError(f"chunk {index} belongs to a different file (spliced)")
    if chunk_index != index:
        raise FileIntegrityError(f"chunk at position {index} carries index {chunk_index} (reordered/duplicated/dropped)")
    if final_flag not in (0, 1):
        raise FileIntegrityError(f"chunk {index} has invalid is_final flag")
    if chunk_size != header.chunk_size:
        raise FileIntegrityError(f"chunk {index} chunk_size {chunk_size} != header {header.chunk_size}")
    body = marked[1 + V2_BINDING_BYTES:]
    payload = _bounded_gunzip(body, header.chunk_size, index) if gz else body
    if len(payload) > header.chunk_size:
        raise FileIntegrityError(f"chunk {index} payload exceeds chunk_size")
    is_final = final_flag == 1
    if not is_final and len(payload) != header.chunk_size:
        raise FileIntegrityError(f"non-final chunk {index} holds {len(payload)} bytes, expected {header.chunk_size}")
    return payload, is_final


def decrypt_bytes(data: bytes, decrypt_tokens: Callable[[list[str]], list[str]]) -> bytes:
    """
    Decrypts a whole file held in memory. decrypt_tokens maps a list of core
    tokens to their decrypted base64 plaintexts, in order -- normally
    hsm-core-service's /decrypt/batch (see decrypt_bulk_file), but anything
    with the same contract works.
    """
    header = read_header(data)
    frames = read_frames(data, header)
    tokens = [reconstruct_core_service_token(header.edek_id_bytes, iv, tag, ct) for iv, tag, ct in frames]
    plaintexts = decrypt_tokens(tokens) if tokens else []

    out = bytearray()
    final_seen = False
    for i, b64 in enumerate(plaintexts):
        if final_seen:
            raise FileIntegrityError(f"data follows the final chunk (chunk {i})")
        payload, is_final = decode_chunk(header, i, b64)
        out += payload
        final_seen = is_final
    if header.version == 2 and not final_seen:
        raise FileIntegrityError(f"file ended after {len(frames)} chunk(s) without a final chunk (truncated)")
    return bytes(out)


def decrypt_bulk_file(client, source_path: str | Path, target_path: str | Path) -> None:
    """
    Reads a FileBulkJob-produced file (v1 or v2) and decrypts it purely via
    hsm-core-service's /decrypt/batch. Writes target_path only after every
    check has passed, via a temp file + rename, so a failed rescue never
    leaves a plausible-looking partial file behind.
    """
    data = Path(source_path).read_bytes()

    def via_core(tokens: list[str]) -> list[str]:
        results = client.decrypt_items([{"key": str(i), "ciphertext": t} for i, t in enumerate(tokens)])
        return [results[str(i)]["plaintext"] for i in range(len(tokens))]

    plaintext = decrypt_bytes(data, via_core)
    target = Path(target_path)
    tmp = target.with_name(target.name + ".partial")
    tmp.write_bytes(plaintext)
    os.replace(tmp, target)


if __name__ == "__main__":
    import sys

    from auth import build_token_provider_from_env
    from hsm_core_batch_file import HsmCoreClient

    if len(sys.argv) != 3:
        print("usage: python hsm_bulk_file_reader.py <source_bulk_file> <target_output_file>")
        sys.exit(1)

    app_id = os.environ.get("HSM_CORE_APP_ID", "payments-svc")
    client = HsmCoreClient(
        base_url=os.environ.get("HSM_CORE_BASE_URL", "http://localhost:3105"),
        api_v1_prefix=os.environ.get("HSM_CORE_API_V1_PREFIX", "/api/sensec/hsm/v1"),
        app_id=app_id,
        # HSM_CORE_AUTH_MODE: STATIC (default) / SELF_SIGNED_JWT / AZURE_AD -- see auth.py
        token_provider=build_token_provider_from_env(app_id),
    )
    decrypt_bulk_file(client, sys.argv[1], sys.argv[2])
    print(f"Decrypted {sys.argv[1]} -> {sys.argv[2]} via hsm-core-service directly.")
