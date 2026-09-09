"""
Exercises udf.encrypt/udf.decrypt through their actual public signature --
credentials_json as an explicit argument, exactly how sql/create_functions.sql
calls them (see DEPLOYMENT.md for why: dbutils can't run inside the function
body, so the caller passes credentials in per call instead). Monkeypatches
SvcClient itself (network boundary) so this stays in the fast, no-network
suite; the real cross-implementation proof is test_live_interop.py.
"""

import json
import uuid

import pytest
from cryptography.hazmat.primitives import serialization

from hsm_databricks_udf import transport, udf
from hsm_databricks_udf.svc_client import IssueResult, UnwrapResult


def _pem(private_key) -> str:
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


class StubSvcClient:
    def __init__(self, config, owner_app_id: str, wrapped_dek_b64: str, edek_id: str):
        self.owner_app_id = owner_app_id
        self.wrapped_dek_b64 = wrapped_dek_b64
        self.edek_id = edek_id
        self.issue_calls = 0

    def issue_dek(self, dek_name, data_classification=None):
        self.issue_calls += 1
        return IssueResult(edek_id=self.edek_id, wrapped_dek_b64=self.wrapped_dek_b64,
                            owner_app_id=self.owner_app_id, reused=self.issue_calls > 1)

    def unwrap_dek(self, edek_id):
        return UnwrapResult(edek_id=edek_id, wrapped_dek_b64=self.wrapped_dek_b64, owner_app_id=self.owner_app_id)


@pytest.fixture(autouse=True)
def reset_state_cache():
    udf._state_cache.clear()
    yield
    udf._state_cache.clear()


def _credentials_json(**overrides) -> str:
    base = {
        "HSM_SERVICE_BASE_URL": "https://hsm.internal/api/sensec/hsm/v1",
        "HSM_APP_ID": "databricks-udf",
        "HSM_AUTH_MODE": "STATIC",
        "HSM_PRIVATE_KEY_PEM": "unused-by-the-stub",
        "HSM_BEARER_TOKEN": "demo-token",
    }
    base.update(overrides)
    return json.dumps(base)


def _install_stub(monkeypatch, owner_app_id: str):
    from cryptography.hazmat.primitives.asymmetric import rsa
    import base64

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    raw_dek = bytes(range(32))
    wrapped_b64 = base64.b64encode(transport.wrap(raw_dek, private_key.public_key())).decode()
    private_pem = _pem(private_key)

    stub = StubSvcClient(config=None, owner_app_id=owner_app_id, wrapped_dek_b64=wrapped_b64, edek_id=str(uuid.uuid4()))
    monkeypatch.setattr(udf, "SvcClient", lambda config: stub)
    return private_pem, stub


def test_encrypt_then_decrypt_round_trip_via_credentials_json(monkeypatch):
    private_pem, stub = _install_stub(monkeypatch, owner_app_id="payments-svc")
    creds = _credentials_json(HSM_PRIVATE_KEY_PEM=private_pem)
    dek_name = f"test.{uuid.uuid4()}"

    token = udf.encrypt("hello from a UDF call", dek_name, None, creds)
    plaintext = udf.decrypt(token, creds)

    assert plaintext == "hello from a UDF call"


def test_encrypt_requires_dek_name(monkeypatch):
    private_pem, stub = _install_stub(monkeypatch, owner_app_id="payments-svc")
    creds = _credentials_json(HSM_PRIVATE_KEY_PEM=private_pem)

    with pytest.raises(ValueError, match="dek_name is required"):
        udf.encrypt("value", "", None, creds)


def test_state_is_cached_per_credentials_json_not_reinitialized_per_call(monkeypatch):
    private_pem, stub = _install_stub(monkeypatch, owner_app_id="payments-svc")
    creds = _credentials_json(HSM_PRIVATE_KEY_PEM=private_pem)
    dek_name = f"test.{uuid.uuid4()}"

    udf.encrypt("row 1", dek_name, None, creds)
    udf.encrypt("row 2", dek_name, None, creds)
    udf.encrypt("row 3", dek_name, None, creds)

    assert stub.issue_calls == 1  # svc_client/private_key reused across calls with identical credentials_json


def test_different_credentials_json_gets_independent_state(monkeypatch):
    from cryptography.hazmat.primitives.asymmetric import rsa
    import base64

    private_key_a = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_key_b = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    raw_dek = bytes(range(32))

    stub_a = StubSvcClient(None, "app-a", base64.b64encode(transport.wrap(raw_dek, private_key_a.public_key())).decode(), str(uuid.uuid4()))
    stub_b = StubSvcClient(None, "app-b", base64.b64encode(transport.wrap(raw_dek, private_key_b.public_key())).decode(), str(uuid.uuid4()))

    stubs_by_app_id = {"app-a": stub_a, "app-b": stub_b}
    monkeypatch.setattr(udf, "SvcClient", lambda config: stubs_by_app_id[config.app_id])

    creds_a = _credentials_json(HSM_APP_ID="app-a", HSM_PRIVATE_KEY_PEM=_pem(private_key_a))
    creds_b = _credentials_json(HSM_APP_ID="app-b", HSM_PRIVATE_KEY_PEM=_pem(private_key_b))
    dek_name_a = f"dek.a.{uuid.uuid4()}"
    dek_name_b = f"dek.b.{uuid.uuid4()}"

    token_a = udf.encrypt("secret A", dek_name_a, None, creds_a)
    token_b = udf.encrypt("secret B", dek_name_b, None, creds_b)

    assert udf.decrypt(token_a, creds_a) == "secret A"
    assert udf.decrypt(token_b, creds_b) == "secret B"
