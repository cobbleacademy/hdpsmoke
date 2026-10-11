"""
Cross-implementation test vectors (java/chassis/test-vectors/crypto-v1.json).

The same file is run by every Java implementation of the token and transport
formats, so passing here means this package produces and accepts exactly what
hsm-core-service and hsm-crypto-client do -- without a running server (contrast
test_live_interop.py). If this fails after a change, the change broke
compatibility with existing ciphertexts: fix the code, never the vectors.
See java/docs/COMPATIBILITY_CONTRACT.md (C1-C4).
"""

import base64
import json
import uuid
from pathlib import Path

import pytest
from cryptography.exceptions import InvalidTag

from hsm_databricks_udf import dek_manager, transport

VECTORS_PATH = Path(__file__).resolve().parents[2] / "java" / "chassis" / "test-vectors" / "crypto-v1.json"
VECTORS = json.loads(VECTORS_PATH.read_text(encoding="utf-8"))
TOKEN = VECTORS["token"]
TRANSPORT = VECTORS["transport"]


def by_name(cases):
    return pytest.mark.parametrize("case", cases, ids=[c["name"] for c in cases])


@by_name(TOKEN["valid"])
def test_pack_token_matches_vector(case):
    token = dek_manager.pack_token(
        uuid.UUID(case["edek_id"]),
        bytes.fromhex(case["iv_hex"]),
        bytes.fromhex(case["tag_hex"]),
        bytes.fromhex(case["ciphertext_hex"]),
    )
    assert token == case["token"]


@by_name(TOKEN["valid"])
@pytest.mark.parametrize("field", ["token", "token_unpadded"])
def test_unpack_token_accepts_padded_and_unpadded(case, field):
    unpacked = dek_manager.unpack_token(case[field])
    assert unpacked.edek_id == uuid.UUID(case["edek_id"])
    assert unpacked.iv == bytes.fromhex(case["iv_hex"])
    assert unpacked.tag == bytes.fromhex(case["tag_hex"])
    assert unpacked.ciphertext == bytes.fromhex(case["ciphertext_hex"])


@by_name(TOKEN["valid"])
def test_decrypt_matches_vector(case):
    plaintext = dek_manager.decrypt(
        bytes.fromhex(case["ciphertext_hex"]),
        bytes.fromhex(case["tag_hex"]),
        bytes.fromhex(case["iv_hex"]),
        bytearray.fromhex(case["dek_hex"]),
        case["app_id"],
    )
    assert plaintext == bytes.fromhex(case["plaintext_hex"])


@by_name(TOKEN["valid"])
def test_fingerprint_matches_vector(case):
    assert dek_manager.make_fingerprint(bytes.fromhex(case["iv_hex"]), bytes.fromhex(case["tag_hex"])) == case["fingerprint"]


@by_name(TOKEN["invalid_format"])
def test_malformed_tokens_are_rejected(case):
    with pytest.raises(ValueError):
        dek_manager.unpack_token(case["token"])


@by_name(TOKEN["decrypt_must_fail"])
def test_authentication_failures_are_rejected(case):
    unpacked = dek_manager.unpack_token(case["token"])
    with pytest.raises(InvalidTag):
        dek_manager.decrypt(unpacked.ciphertext, unpacked.tag, unpacked.iv,
                            bytearray.fromhex(case["dek_hex"]), case["app_id"])


@by_name(TRANSPORT["cases"])
def test_transport_unwrap_matches_vector(case):
    private_key = transport.parse_private_key_pem(TRANSPORT["private_key_pem"])
    assert transport.unwrap(base64.b64decode(case["wrapped_b64"]), private_key) == bytes.fromhex(case["dek_hex"])


@by_name(TRANSPORT["cases"])
def test_transport_wrap_round_trips_with_vector_key(case):
    public_key = transport.parse_public_key_pem(TRANSPORT["public_key_pem"])
    private_key = transport.parse_private_key_pem(TRANSPORT["private_key_pem"])
    dek = bytes.fromhex(case["dek_hex"])
    assert transport.unwrap(transport.wrap(dek, public_key), private_key) == dek
