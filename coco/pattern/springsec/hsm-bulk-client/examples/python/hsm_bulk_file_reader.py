"""
Reference implementation, in Python, of reading a REAL hsm-bulk-client
FileBulkJob-produced file (Tier 3 -- the file hsm-bulk-service's local
encrypt path writes) and decrypting it via hsm-core-service's own
POST /decrypt directly. Zero contact with hsm-bulk-service on this side.

Complements hsm_core_batch_file.py (the Tier 1 pattern: chunk and encrypt
directly against hsm-core-service, with its own JSON manifest). This
module instead reads a file that already went through hsm-bulk-service's
Tier 3 pipeline, and decrypts it purely via hsm-core-service, with no
adapter beyond parsing the file's own binary layout -- proving the two
services' ciphertext is genuinely, mutually interoperable, not just
similar.

FileBulkJob.java's class javadoc and reconstructCoreServiceToken() are the
canonical source of truth this module ports -- if the two ever disagree,
the Java source wins.

File layout (FileBulkJob.java):

    [8 bytes: edek_id most-significant bits, big-endian signed long]
    [8 bytes: edek_id least-significant bits, big-endian signed long]
    repeated until EOF:
        [4 bytes: frame length N, big-endian signed int]
        [12 bytes: AES-GCM IV/nonce]
        [16 bytes: AES-GCM authentication tag]
        [N - 28 bytes: ciphertext]

hsm-core-service's ciphertext token format (DekManager.packToken, shared
verbatim by hsm-core-service and hsm-bulk-service):

    "v1." + base64url(0x01 [version byte] + edek_id(16, big-endian) +
                       iv(12) + tag(16) + ciphertext)

Reconstructing one is just concatenating bytes already sitting in the
file -- the version byte, the file's own edek_id header, and one frame's
iv/tag/ciphertext -- then base64url-encoding the result. No re-encryption,
no crypto library needed in this module at all: it's pure bytes and one
HTTP call, reusing HsmCoreClient from hsm_core_batch_file.py.

Each chunk's plaintext, once decrypted by hsm-core-service, is itself a
base64-encoded string (FileBulkJob's own plaintext-safety encoding, so the
ciphertext survives hsm-core-service's UTF-8 response encoding losslessly)
-- one more base64 decode recovers the original raw chunk bytes.

Dependency: pip install requests (via hsm_core_batch_file's HsmCoreClient)
"""

from __future__ import annotations

import base64
import struct
from pathlib import Path

from hsm_core_batch_file import HsmCoreClient

IV_LENGTH = 12
TAG_LENGTH = 16
TOKEN_VERSION = b"\x01"
TOKEN_PREFIX = "v1."

_FRAME_LEN_FMT = ">i"  # signed 4-byte big-endian int, matches DataOutputStream.writeInt/readInt


def _read_edek_id_and_frames(path: str | Path) -> tuple[bytes, list[tuple[bytes, bytes, bytes]]]:
    """
    Parses FileBulkJob's binary layout. Returns (edek_id_bytes, frames),
    where edek_id_bytes is the raw 16-byte big-endian header and each frame
    is (iv, tag, ciphertext). Deliberately keeps edek_id as raw bytes, never
    wrapped in a uuid.UUID -- this module only ever concatenates it into a
    reconstructed token, so there's no need to parse it into anything richer.
    """
    with open(path, "rb") as f:
        edek_id_bytes = f.read(16)
        if len(edek_id_bytes) != 16:
            raise ValueError(f"{path}: too short to contain a 16-byte edek_id header")

        frames = []
        while True:
            length_bytes = f.read(4)
            if not length_bytes:
                break  # clean end of frames
            if len(length_bytes) != 4:
                raise ValueError(f"{path}: truncated frame-length field near end of file")
            (frame_len,) = struct.unpack(_FRAME_LEN_FMT, length_bytes)

            frame = f.read(frame_len)
            if len(frame) != frame_len:
                raise ValueError(f"{path}: truncated frame body near end of file")

            iv = frame[:IV_LENGTH]
            tag = frame[IV_LENGTH:IV_LENGTH + TAG_LENGTH]
            ciphertext = frame[IV_LENGTH + TAG_LENGTH:]
            frames.append((iv, tag, ciphertext))

        return edek_id_bytes, frames


def reconstruct_core_service_token(edek_id_bytes: bytes, iv: bytes, tag: bytes, ciphertext: bytes) -> str:
    """
    Ports FileBulkJob.reconstructCoreServiceToken() exactly: rebuilds the
    same "v1.<base64url(...)>" string hsm-core-service's own /encrypt
    produces, from one frame's raw bytes plus the file's edek_id.
    """
    payload = TOKEN_VERSION + edek_id_bytes + iv + tag + ciphertext
    return TOKEN_PREFIX + base64.urlsafe_b64encode(payload).decode("ascii")


def decrypt_bulk_file(client: HsmCoreClient, source_path: str | Path, target_path: str | Path) -> None:
    """
    Reads a real FileBulkJob-produced file and decrypts it purely via
    hsm-core-service's /decrypt/batch -- zero contact with hsm-bulk-service
    on this side. Chunks are already in file order (frames appear in the
    exact order they were written), so no separate ordering key is needed
    the way hsm_core_batch_file.py's own manifest needs one -- the file
    itself is already an ordered record.
    """
    edek_id_bytes, frames = _read_edek_id_and_frames(source_path)

    items = [
        {"key": str(i), "ciphertext": reconstruct_core_service_token(edek_id_bytes, iv, tag, ciphertext)}
        for i, (iv, tag, ciphertext) in enumerate(frames)
    ]
    results = client.decrypt_items(items)

    with open(target_path, "wb") as out:
        for i in range(len(frames)):
            base64_plaintext = results[str(i)]["plaintext"]
            out.write(base64.b64decode(base64_plaintext))


if __name__ == "__main__":
    import os
    import sys

    if len(sys.argv) != 3:
        print("usage: python hsm_bulk_file_reader.py <source_bulk_file> <target_output_file>")
        sys.exit(1)

    client = HsmCoreClient(
        base_url=os.environ.get("HSM_CORE_BASE_URL", "http://localhost:3105"),
        api_v1_prefix=os.environ.get("HSM_CORE_API_V1_PREFIX", "/api/sensec/hsm/v1"),
        app_id=os.environ.get("HSM_CORE_APP_ID", "payments-svc"),
        token=os.environ.get("HSM_CORE_TOKEN", "demo-token-payments-svc"),
    )
    decrypt_bulk_file(client, sys.argv[1], sys.argv[2])
    print(f"Decrypted {sys.argv[1]} -> {sys.argv[2]} via hsm-core-service directly (hsm-bulk-service never contacted).")
