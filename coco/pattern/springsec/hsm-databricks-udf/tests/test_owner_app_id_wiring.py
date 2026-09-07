"""
Regression coverage for the actual bug this package's design was built
around: a grant-authorized cross-app reuse must encrypt/decrypt using the
DEK's true, permanent owner_app_id as AAD -- never this worker's own
identity. Uses a stub SvcClient (no network) so this stays in the fast,
always-run suite; the real end-to-end proof against a live hsm-core-service
was run manually and is documented in DEPLOYMENT.md / DATABRICKS_UDF_DESIGN.md.
"""

import base64
import uuid

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from hsm_databricks_udf import cache, dek_manager, transport
from hsm_databricks_udf.svc_client import IssueResult, UnwrapResult


class StubSvcClient:
    """Mimics SvcClient's public surface without any network call."""

    def __init__(self, owner_app_id: str, edek_id: str, wrapped_dek_b64: str):
        self.owner_app_id = owner_app_id
        self.edek_id = edek_id
        self.wrapped_dek_b64 = wrapped_dek_b64
        self.issue_calls = 0
        self.unwrap_calls = 0

    def issue_dek(self, dek_name, data_classification=None):
        self.issue_calls += 1
        return IssueResult(edek_id=self.edek_id, wrapped_dek_b64=self.wrapped_dek_b64,
                            owner_app_id=self.owner_app_id, reused=self.issue_calls > 1)

    def unwrap_dek(self, edek_id):
        self.unwrap_calls += 1
        return UnwrapResult(edek_id=edek_id, wrapped_dek_b64=self.wrapped_dek_b64, owner_app_id=self.owner_app_id)


def _keypair_and_wrapped_dek():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    raw_dek = bytes(dek_manager.generate_dek())
    wrapped = transport.wrap(raw_dek, private_key.public_key())
    return private_key, raw_dek, base64.b64encode(wrapped).decode()


def test_encrypt_uses_returned_owner_app_id_not_a_guess():
    """The core regression: this worker's own identity is never involved --
    only the server-returned owner_app_id, exactly like hsm-core-service's
    own EncryptionService now does since the AAD fix."""
    private_key, raw_dek, wrapped_b64 = _keypair_and_wrapped_dek()
    dek_name = f"test.{uuid.uuid4()}"
    true_owner = "the-actual-owner-app"   # deliberately NOT "this worker's app"
    stub = StubSvcClient(owner_app_id=true_owner, edek_id=str(uuid.uuid4()), wrapped_dek_b64=wrapped_b64)

    edek_id, owner_app_id, dek = cache.get_or_issue_for_encrypt(dek_name, stub, private_key)

    assert owner_app_id == true_owner
    assert bytes(dek) == raw_dek

    # Encrypting with the returned owner_app_id must decrypt correctly...
    result = dek_manager.encrypt(b"payload", dek, owner_app_id)
    decrypted = dek_manager.decrypt(result.ciphertext, result.tag, result.iv, dek, true_owner)
    assert decrypted == b"payload"

    # ...and using the WRONG identity (e.g. this worker's own app_id, the bug
    # this test exists to catch) must NOT decrypt.
    from cryptography.exceptions import InvalidTag
    import pytest
    with pytest.raises(InvalidTag):
        dek_manager.decrypt(result.ciphertext, result.tag, result.iv, dek, "this-workers-own-app-id")


def test_encrypt_cache_is_per_dek_name_and_hits_svc_client_once():
    private_key, raw_dek, wrapped_b64 = _keypair_and_wrapped_dek()
    dek_name = f"test.{uuid.uuid4()}"
    stub = StubSvcClient(owner_app_id="owner", edek_id=str(uuid.uuid4()), wrapped_dek_b64=wrapped_b64)

    cache.get_or_issue_for_encrypt(dek_name, stub, private_key)
    cache.get_or_issue_for_encrypt(dek_name, stub, private_key)
    cache.get_or_issue_for_encrypt(dek_name, stub, private_key)

    assert stub.issue_calls == 1   # cached after the first call, matching HsmCryptoClient's model


def test_decrypt_uses_returned_owner_app_id():
    private_key, raw_dek, wrapped_b64 = _keypair_and_wrapped_dek()
    edek_id = str(uuid.uuid4())
    true_owner = "the-actual-owner-app"
    stub = StubSvcClient(owner_app_id=true_owner, edek_id=edek_id, wrapped_dek_b64=wrapped_b64)

    # Simulate a ciphertext genuinely produced by the true owner.
    encrypted = dek_manager.encrypt(b"owner wrote this", raw_dek, true_owner)

    owner_app_id, dek = cache.get_or_unwrap_for_decrypt(edek_id, stub, private_key)
    assert owner_app_id == true_owner

    decrypted = dek_manager.decrypt(encrypted.ciphertext, encrypted.tag, encrypted.iv, dek, owner_app_id)
    assert decrypted == b"owner wrote this"


def test_decrypt_cache_hits_svc_client_once_per_edek_id():
    private_key, raw_dek, wrapped_b64 = _keypair_and_wrapped_dek()
    edek_id = str(uuid.uuid4())
    stub = StubSvcClient(owner_app_id="owner", edek_id=edek_id, wrapped_dek_b64=wrapped_b64)

    cache.get_or_unwrap_for_decrypt(edek_id, stub, private_key)
    cache.get_or_unwrap_for_decrypt(edek_id, stub, private_key)

    assert stub.unwrap_calls == 1
