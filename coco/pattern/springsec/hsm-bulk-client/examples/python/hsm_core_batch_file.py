"""
Reference implementation, in Python, of the Tier 1 (reviewed, foundational)
pattern for encrypting/decrypting large files: chunk the file yourself, but
send each chunk's actual data straight to hsm-core-service's own
POST /encrypt/batch and POST /decrypt/batch -- hsm-core-service does the
real AES-256-GCM work server-side and hands back one opaque ciphertext
token per chunk. No hsm-bulk-service anywhere in this picture, and no raw
DEK ever reaches this code -- this module never imports a crypto library
at all. See java/docs/BULK_OPERATIONS.md's "Files with multiple chunks:
chunking + stitch-back" section, which this follows directly: chunking
strategy, chunk identity caveat, manifest, and stitch-back verification are
all straight from that section, not invented here.

(There is a second, different pattern -- examples/dotnet's and this
directory's own now-superseded sibling implemented it -- where a separate
hsm-bulk-service hands the client a wrapped DEK for local AES-GCM. That's a
later, "proposed, not yet approved"/PoC-stage addition (Tier 3), not the
reviewed foundational one. This file is Tier 1: hsm-core-service directly,
nothing else running.)

Wire contract, straight from EncryptController/DecryptController's actual
DTOs (com.hsm.core.dto) -- every JSON field below is snake_case, since
hsm-core-service's whole API uses spring.jackson.property-naming-strategy:
SNAKE_CASE:

    POST {base_url}{api_v1_prefix}/encrypt/batch
    Authorization: Bearer <token>
    X-App-ID: <app_id>
    X-Response-Detail: full        (optional -- minimal omits edek_id/
                                     owner_app_id/algorithm/encoding/
                                     kek_version, this module doesn't need
                                     any of those, minimal is enough)
    {"items": [{"key": "<correlation id>", "plaintext": "<base64>",
                "encoding": "base64", ...}]}
    -> {"items": [{"key": "...", "status": "success"|"error",
                    "result": {"ciphertext": "v1....", ...} | null,
                    "detail": null | "<error message>"}]}

    POST {base_url}{api_v1_prefix}/decrypt/batch -- same shape, ciphertext
    tokens in, {"plaintext": "...", "encoding": "..."} results out.

Plaintext is ALWAYS sent/received as base64 here (encoding: "base64"),
never raw UTF-8 text -- a file chunk is arbitrary binary, not necessarily
valid text, so there's no case where the utf8 encoding is the right choice
for this module's purpose.

Batch size cap is hsm.service.batch-max-items on the server (default 100,
shared by encrypt and decrypt) -- this module batches into groups of at
most that many chunks per HTTP call; pass a smaller value if your server's
configured cap is lower.

Auth: this module needs a bearer token and matching app_id from you --
getting one is entirely outside this module's scope (a real deployment
uses an Entra ID/Azure AD app registration doing OAuth2 client-credentials
against hsm-core-service's own JWT_AUDIENCE/JWT_ISSUER; a local demo-mode
server instead accepts one of a handful of fixed literal strings like
"demo-token-payments-svc", see MockJwtValidator.DEMO_TOKENS -- not a
template for real auth). See java/docs/APP_ONBOARDING.md.

Dependency: pip install requests
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

DEFAULT_CHUNK_SIZE_BYTES = 8 * 1024 * 1024
DEFAULT_BATCH_MAX_ITEMS = 100


class HsmCoreBatchError(Exception):
    """Raised when a batch item comes back with status="error", or a request-level failure occurs."""


@dataclass
class HsmCoreClient:
    """
    Thin wrapper around the two batch endpoints this module needs. Not a
    general hsm-core-service client -- just enough to drive file
    encrypt/decrypt via chunking. token/app_id/base_url are exactly what
    SvcClient.java's Java equivalent takes for hsm-bulk-service, applied
    here to hsm-core-service instead.
    """

    base_url: str
    api_v1_prefix: str
    app_id: str
    token: str
    batch_max_items: int = DEFAULT_BATCH_MAX_ITEMS
    session: requests.Session = field(default_factory=requests.Session)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "X-App-ID": self.app_id,
            "X-Response-Detail": "minimal",  # this module only ever needs ciphertext/plaintext + encoding
            "Content-Type": "application/json",
        }

    def _post_batch(self, path: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        resp = self.session.post(
            f"{self.base_url}{self.api_v1_prefix}{path}",
            headers=self._headers(),
            data=json.dumps({"items": items}),
            timeout=60,
        )
        resp.raise_for_status()  # a 4xx/5xx here means the WHOLE batch was rejected (e.g. bad auth, over-cap, empty)
        return resp.json()["items"]

    def encrypt_items(self, items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """items: [{"key": ..., "plaintext": <base64>, "encoding": "base64", ...}]. Returns {key: result_dict}, raising on any per-item error."""
        by_key: dict[str, dict[str, Any]] = {}
        for i in range(0, len(items), self.batch_max_items):
            for result_item in self._post_batch("/encrypt/batch", items[i:i + self.batch_max_items]):
                if result_item["status"] != "success":
                    raise HsmCoreBatchError(f"encrypt failed for key={result_item['key']}: {result_item['detail']}")
                by_key[result_item["key"]] = result_item["result"]
        return by_key

    def decrypt_items(self, items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """items: [{"key": ..., "ciphertext": <token>}]. Returns {key: result_dict}, raising on any per-item error."""
        by_key: dict[str, dict[str, Any]] = {}
        for i in range(0, len(items), self.batch_max_items):
            for result_item in self._post_batch("/decrypt/batch", items[i:i + self.batch_max_items]):
                if result_item["status"] != "success":
                    raise HsmCoreBatchError(f"decrypt failed for key={result_item['key']}: {result_item['detail']}")
                by_key[result_item["key"]] = result_item["result"]
        return by_key


def encrypt_file(
    client: HsmCoreClient,
    source_path: str | Path,
    manifest_path: str | Path,
    chunk_size_bytes: int = DEFAULT_CHUNK_SIZE_BYTES,
    data_classification: str | None = None,
    dek_name: str | None = None,
) -> dict[str, Any]:
    """
    Chunks source_path locally, encrypts each chunk via /encrypt/batch, and
    writes a manifest (JSON: file_id, filename, sizes, ordered ciphertext
    tokens, whole-file plaintext SHA-256) to manifest_path -- your own
    record, in your own storage, per BULK_OPERATIONS.md's guidance; nothing
    here is created or stored by hsm-core-service itself. Returns the same
    manifest dict that was written.
    """
    source_path = Path(source_path)
    digest = hashlib.sha256()
    items: list[dict[str, Any]] = []

    with open(source_path, "rb") as f:
        index = 0
        while True:
            chunk = f.read(chunk_size_bytes)
            if not chunk:
                break
            digest.update(chunk)
            item: dict[str, Any] = {
                "key": str(index),
                "plaintext": base64.b64encode(chunk).decode("ascii"),
                "encoding": "base64",
            }
            if data_classification:
                item["data_classification"] = data_classification
            if dek_name:
                item["dek_name"] = dek_name
            items.append(item)
            index += 1

    results = client.encrypt_items(items)
    # Re-order by numeric key rather than trust response array order -- the
    # server never documents item-order preservation as a contract, only
    # that each item's own "key" is echoed back correctly.
    ordered_ciphertexts = [results[str(i)]["ciphertext"] for i in range(len(items))]

    manifest = {
        "file_id": source_path.name,
        "filename": source_path.name,
        "total_size_bytes": source_path.stat().st_size,
        "chunk_size_bytes": chunk_size_bytes,
        "chunk_count": len(items),
        "plaintext_sha256": digest.hexdigest(),
        "chunks": ordered_ciphertexts,
    }
    Path(manifest_path).write_text(json.dumps(manifest, indent=2))
    return manifest


def decrypt_file(client: HsmCoreClient, manifest_path: str | Path, target_path: str | Path) -> None:
    """
    Reads a manifest written by encrypt_file, decrypts every chunk via
    /decrypt/batch, reassembles them in manifest order, and verifies the
    reassembled plaintext's SHA-256 against what encrypt_file recorded --
    the check that catches a chunk silently dropped or duplicated during
    reassembly (the per-chunk AEAD tag alone only proves each chunk's
    ciphertext wasn't tampered with, not that this function assembled them
    correctly). Written to a temp file first, renamed into place only after
    the digest check passes -- target_path is never left holding a
    partially-wrong file.
    """
    manifest = json.loads(Path(manifest_path).read_text())
    items = [{"key": str(i), "ciphertext": token} for i, token in enumerate(manifest["chunks"])]
    results = client.decrypt_items(items)

    target_path = Path(target_path)
    tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    digest = hashlib.sha256()
    with open(tmp_path, "wb") as out:
        for i in range(manifest["chunk_count"]):
            result = results[str(i)]
            plaintext_field = result["plaintext"]
            chunk = base64.b64decode(plaintext_field) if result["encoding"] == "base64" else plaintext_field.encode("utf-8")
            digest.update(chunk)
            out.write(chunk)

    if digest.hexdigest() != manifest["plaintext_sha256"]:
        tmp_path.unlink()
        raise HsmCoreBatchError(
            f"{manifest_path}: reassembled plaintext SHA-256 ({digest.hexdigest()}) "
            f"does not match manifest ({manifest['plaintext_sha256']}) -- a chunk was "
            f"dropped, duplicated, or reordered during reassembly"
        )
    tmp_path.replace(target_path)


if __name__ == "__main__":
    # Demo/smoke-test against a REAL, reachable hsm-core-service -- unlike
    # the old Tier-3 crypto-only reference, this module has no local crypto
    # to self-test in isolation; every call here is a real HTTP round trip.
    # Configure via env vars so this can point at any environment.
    import os
    import sys
    import tempfile

    base_url = os.environ.get("HSM_CORE_BASE_URL", "http://localhost:3105")
    api_v1_prefix = os.environ.get("HSM_CORE_API_V1_PREFIX", "/api/sensec/hsm/v1")
    app_id = os.environ.get("HSM_CORE_APP_ID", "payments-svc")
    token = os.environ.get("HSM_CORE_TOKEN", "demo-token-payments-svc")

    client = HsmCoreClient(base_url=base_url, api_v1_prefix=api_v1_prefix, app_id=app_id, token=token)

    demo_plaintext = os.urandom(37_000)  # forces multiple chunks at the small chunk size below
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        src, manifest, dec = tmp / "plain.bin", tmp / "manifest.json", tmp / "decrypted.bin"
        src.write_bytes(demo_plaintext)

        m = encrypt_file(client, src, manifest, chunk_size_bytes=4096)
        print(f"encrypted: chunk_count={m['chunk_count']} plaintext_sha256={m['plaintext_sha256']}")

        decrypt_file(client, manifest, dec)
        match = hashlib.sha256(dec.read_bytes()).hexdigest() == m["plaintext_sha256"]
        print(f"decrypted: match={match}")
        if not match:
            sys.exit(1)
        print("OK -- round trip verified against a real hsm-core-service")
