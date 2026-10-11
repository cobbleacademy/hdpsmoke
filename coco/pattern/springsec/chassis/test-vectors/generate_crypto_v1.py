"""
Generates crypto-v1.json: language-neutral test vectors for the formats in
java/docs/COMPATIBILITY_CONTRACT.md (C1 token, C2 AAD, C3 fingerprint, C4 DEK
transport wrap, C7 Redis DEK cache value/key).

Deliberately built with Python's `cryptography` package straight from the
written spec, NOT from any of the implementations under test, so the vectors are
an independent check on every Java and Python copy.

All inputs are fixed, so re-running reproduces the same AES-GCM outputs byte for
byte. The two exceptions are randomized by design -- the RSA test key and the
RSA-OAEP wrap -- so those are kept from the existing crypto-v1.json when it
exists; delete the file only to deliberately regenerate them.

    python java/chassis/test-vectors/generate_crypto_v1.py

Vectors are a contract: if an implementation stops matching them, fix the
implementation, don't regenerate the vectors.
"""

from __future__ import annotations

import base64
import hashlib
import json
import uuid
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

OUT = Path(__file__).with_name("crypto-v1.json")
AAD_PREFIX = "hsm-svc:app_id="
TOKEN_PREFIX = "v1."
TOKEN_VERSION = 0x01


def derive(label: str, n: int) -> bytes:
    """Deterministic, obviously-synthetic key/IV material: SHA-256 of a label."""
    return hashlib.sha256(label.encode()).digest()[:n]


def b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode()


def b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


def token_case(name: str, plaintext: bytes, app_id: str, edek_id: str) -> dict:
    dek = derive(f"dek:{name}", 32)
    iv = derive(f"iv:{name}", 12)
    aad = (AAD_PREFIX + app_id).encode("utf-8")
    combined = AESGCM(dek).encrypt(iv, plaintext, aad)
    ciphertext, tag = combined[:-16], combined[-16:]
    binary = bytes([TOKEN_VERSION]) + uuid.UUID(edek_id).bytes + iv + tag + ciphertext
    token = TOKEN_PREFIX + b64url(binary)
    return {
        "name": name,
        "dek_hex": dek.hex(),
        "app_id": app_id,
        "aad_hex": aad.hex(),
        "edek_id": edek_id,
        "iv_hex": iv.hex(),
        "plaintext_hex": plaintext.hex(),
        "ciphertext_hex": ciphertext.hex(),
        "tag_hex": tag.hex(),
        "token": token,
        "token_unpadded": token.rstrip("="),
        "fingerprint": hashlib.sha256(iv + tag).hexdigest()[:16],
    }


def build_token_cases() -> list[dict]:
    return [
        # Plaintext lengths 1/2/3 put the 45-byte fixed header + payload at every
        # base64 residue: "==" padding, "=" padding, and no padding.
        token_case("one-byte-double-pad", b"A", "payments-svc", "00112233-4455-6677-8899-aabbccddeeff"),
        token_case("two-byte-single-pad", b"AB", "payments-svc", "10000000-0000-4000-8000-000000000001"),
        token_case("three-byte-no-pad", b"ABC", "payments-svc", "20000000-0000-4000-8000-000000000002"),
        token_case("utf8-text", "card 4111-1111 / naïve café ✓".encode("utf-8"), "reporting-app",
                   "30000000-0000-4000-8000-000000000003"),
        token_case("binary-all-bytes", bytes(range(256)), "payments-svc", "40000000-0000-4000-8000-000000000004"),
        token_case("long-4k", derive("long", 32) * 128, "bulk-loader", "50000000-0000-4000-8000-000000000005"),
        token_case("non-ascii-app-id", b"owner-binding", "équipe-données", "60000000-0000-4000-8000-000000000006"),
        # Base64url's '-' and '_' must survive (and must not be confused with '+' '/').
        token_case("urlsafe-alphabet", bytes([0xfb, 0xff, 0xbf] * 8), "payments-svc",
                   "ffffffff-ffff-4fff-bfff-fffffffffffe"),
    ]


def build_invalid_tokens(valid: dict) -> list[dict]:
    binary = base64.urlsafe_b64decode(valid["token"][len(TOKEN_PREFIX):])
    header_only = binary[:45]
    return [
        {"name": "wrong-prefix", "token": "v2." + valid["token"][3:]},
        {"name": "missing-prefix", "token": valid["token"][3:]},
        {"name": "not-base64url", "token": "v1.!!!not*base64!!!"},
        {"name": "too-short", "token": TOKEN_PREFIX + b64url(binary[:44])},
        {"name": "unsupported-version-byte", "token": TOKEN_PREFIX + b64url(bytes([0x02]) + binary[1:])},
        {"name": "empty-ciphertext", "token": TOKEN_PREFIX + b64url(header_only)},
    ]


def build_decrypt_failures(cases: list[dict]) -> list[dict]:
    """Well-formed tokens that must fail AES-GCM authentication."""
    c = cases[3]
    binary = bytearray(base64.urlsafe_b64decode(c["token"][len(TOKEN_PREFIX):]))
    tampered_tag = bytearray(binary)
    tampered_tag[1 + 16 + 12] ^= 0x01
    tampered_ct = bytearray(binary)
    tampered_ct[-1] ^= 0x01
    other_dek = cases[0]["dek_hex"]
    return [
        {"name": "wrong-app-id", "dek_hex": c["dek_hex"], "app_id": "payments-svc", "token": c["token"]},
        {"name": "tampered-tag", "dek_hex": c["dek_hex"], "app_id": c["app_id"],
         "token": TOKEN_PREFIX + b64url(bytes(tampered_tag))},
        {"name": "tampered-ciphertext", "dek_hex": c["dek_hex"], "app_id": c["app_id"],
         "token": TOKEN_PREFIX + b64url(bytes(tampered_ct))},
        {"name": "wrong-dek", "dek_hex": other_dek, "app_id": c["app_id"], "token": c["token"]},
    ]


def build_transport(existing: dict | None) -> dict:
    if existing:
        private_pem = existing["private_key_pem"].encode()
        key = serialization.load_pem_private_key(private_pem, password=None)
    else:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                        serialization.NoEncryption())
    public_pem = key.public_key().public_bytes(serialization.Encoding.PEM,
                                               serialization.PublicFormat.SubjectPublicKeyInfo)
    oaep = padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None)
    previous = {c["name"]: c for c in (existing or {}).get("cases", [])}
    cases = []
    for name in ("dek-a", "dek-b"):
        dek = derive(f"transport:{name}", 32)
        if name in previous and previous[name]["dek_hex"] == dek.hex():
            wrapped_b64 = previous[name]["wrapped_b64"]
        else:
            wrapped_b64 = b64(key.public_key().encrypt(dek, oaep))
        assert key.decrypt(base64.b64decode(wrapped_b64), oaep) == dek
        cases.append({"name": name, "dek_hex": dek.hex(), "wrapped_b64": wrapped_b64})
    return {
        "note": "TEST-ONLY RSA-2048 key generated for these vectors; it protects nothing.",
        "algorithm": "RSA-OAEP, SHA-256 hash, MGF1-SHA-256, no label",
        "private_key_pem": private_pem.decode(),
        "public_key_pem": public_pem.decode(),
        "cases": cases,
    }


def cache_case(name: str, slot: str, kv_version: str, edek_id: str) -> dict:
    cek = derive(f"cek:{name}", 32)
    dek = derive(f"cached-dek:{name}", 32)
    iv = derive(f"cache-iv:{name}", 12)
    ciphertext_with_tag = AESGCM(cek).encrypt(iv, dek, None)
    return {
        "name": name,
        "cek_b64": b64(cek),
        "slot": slot,
        "kv_version": kv_version,
        "edek_id": edek_id,
        "key": f"dek:{slot}:{kv_version}:{edek_id}",
        "iv_hex": iv.hex(),
        "dek_hex": dek.hex(),
        "value": b64(iv) + ":" + b64(ciphertext_with_tag),
    }


def main() -> None:
    existing = json.loads(OUT.read_text()) if OUT.exists() else {}
    tokens = build_token_cases()
    vectors = {
        "description": "Cross-implementation test vectors; see java/docs/COMPATIBILITY_CONTRACT.md "
                       "and generate_crypto_v1.py. Do not edit by hand.",
        "format_version": 1,
        "token": {
            "aad_prefix": AAD_PREFIX,
            "prefix": TOKEN_PREFIX,
            "layout": "version(1)=0x01 | edek_id(16, big-endian) | iv(12) | tag(16) | ciphertext(N>=1)",
            "cipher": "AES-256-GCM, 96-bit IV, 128-bit tag, AAD = UTF-8(aad_prefix + app_id)",
            "fingerprint": "hex(SHA-256(iv || tag))[0:16]",
            "valid": tokens,
            "invalid_format": build_invalid_tokens(tokens[0]),
            "decrypt_must_fail": build_decrypt_failures(tokens),
        },
        "transport": build_transport(existing.get("transport")),
        "dek_cache": {
            "key_format": "dek:{slot}:{kv_version}:{edek_id}",
            "value_format": "base64(iv) ':' base64(ciphertext || tag) -- standard base64, "
                            "AES-256-GCM under the CEK, 12-byte IV, no AAD",
            "cek_secret_format": "standard base64 of a 32-byte AES key",
            "cases": [
                cache_case("alpha-slot", "alpha", "0f4a6c2e9b8d4e7f8a1b2c3d4e5f6071", "70000000-0000-4000-8000-000000000007"),
                cache_case("beta-slot", "beta", "a1b2c3d4e5f60718293a4b5c6d7e8f90", "80000000-0000-4000-8000-000000000008"),
            ],
        },
    }
    OUT.write_text(json.dumps(vectors, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
