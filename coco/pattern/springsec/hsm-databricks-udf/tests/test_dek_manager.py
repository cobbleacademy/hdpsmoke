"""
Self-consistency tests for the vendored crypto -- no network, no Databricks,
runs in plain CI. Cross-implementation interop against the real Java server
is verified separately (see tests/test_live_interop.py, opt-in, requires a
running hsm-core-service instance) -- this suite is what stays green on
every commit.
"""

import uuid

import pytest
from cryptography.exceptions import InvalidTag

from hsm_databricks_udf import dek_manager


def test_encrypt_decrypt_round_trip():
    dek = dek_manager.generate_dek()
    plaintext = b"round trip me"
    result = dek_manager.encrypt(plaintext, dek, "app-a")
    decrypted = dek_manager.decrypt(result.ciphertext, result.tag, result.iv, dek, "app-a")
    assert decrypted == plaintext


def test_wrong_aad_app_id_fails_tag_verification():
    """The exact bug class this whole package's owner_app_id plumbing exists to avoid."""
    dek = dek_manager.generate_dek()
    result = dek_manager.encrypt(b"secret", dek, "true-owner")
    with pytest.raises(InvalidTag):
        dek_manager.decrypt(result.ciphertext, result.tag, result.iv, dek, "wrong-app-id")


def test_pack_unpack_token_round_trip():
    dek = dek_manager.generate_dek()
    edek_id = uuid.uuid4()
    result = dek_manager.encrypt(b"token round trip", dek, "app-a")
    token = dek_manager.pack_token(edek_id, result.iv, result.tag, result.ciphertext)

    assert token.startswith("v1.")
    unpacked = dek_manager.unpack_token(token)
    assert unpacked.edek_id == edek_id
    assert unpacked.iv == result.iv
    assert unpacked.tag == result.tag
    assert unpacked.ciphertext == result.ciphertext

    decrypted = dek_manager.decrypt(unpacked.ciphertext, unpacked.tag, unpacked.iv, dek, "app-a")
    assert decrypted == b"token round trip"


def test_unpack_token_rejects_bad_prefix():
    with pytest.raises(ValueError, match="unrecognised format"):
        dek_manager.unpack_token("not-a-token")


def test_unpack_token_rejects_wrong_version():
    import base64

    dek = dek_manager.generate_dek()
    result = dek_manager.encrypt(b"x", dek, "app-a")
    token = dek_manager.pack_token(uuid.uuid4(), result.iv, result.tag, result.ciphertext)

    # The "v1." on the wire is a fixed prefix, not itself the version -- the
    # real version byte lives inside the base64url-decoded binary payload.
    binary = bytearray(base64.urlsafe_b64decode(token[3:] + "=="))
    binary[0] = 0x09
    tampered = "v1." + base64.urlsafe_b64encode(bytes(binary)).decode()

    with pytest.raises(ValueError, match="unsupported version"):
        dek_manager.unpack_token(tampered)


def test_aad_matches_java_format_exactly():
    """Byte-for-byte match with com.hsm.core.crypto.DekManager.makeAad -- confirmed
    directly against that source (not assumed) before this package was built."""
    assert dek_manager._make_aad("payments-svc") == b"hsm-svc:app_id=payments-svc"
