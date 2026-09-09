"""
Real, live, cross-implementation interop test against an actual running
hsm-core-service instance -- proves this package's crypto is byte-for-byte
compatible with the real Java implementation, in BOTH directions, not just
internally self-consistent (see test_dek_manager.py/test_transport.py for
the fast, always-run self-consistency suite).

Opt-in, skipped by default: set HSM_LIVE_TEST_BASE_URL to run it, e.g.
against the local demo server (see DEPLOYMENT.md):

    cd java && mvn -q -pl hsm-core-service -am package -DskipTests
    java -jar hsm-core-service/target/hsm-core-service.jar &
    HSM_LIVE_TEST_BASE_URL=http://localhost:3005/api/sensec/hsm/v1 pytest tests/test_live_interop.py -v -s

This exact script (same steps, same assertions) was run manually against a
real local demo instance while this package was built, and is what verified:
(1) Python RSA-OAEP-256 unwrap is compatible with the Java server's wrap,
(2) Python AES-256-GCM encrypt/pack_token produces a token the real /decrypt
    endpoint accepts and decrypts correctly,
(3) the real /encrypt + /dek/unwrap produces a token this package decrypts
    correctly using owner_app_id from the /dek/unwrap response,
(4) owner_app_id survives a same-app /dek/issue reuse correctly.

A true cross-app grant reuse via /dek/issue specifically could not be
exercised here: no demo app besides payments-svc holds the dek_issue scope.
That exact code path (ResolvedDek.ownerAppId) is shared with /encrypt, which
IS covered end-to-end for the cross-app case by
EncryptDecryptIntegrationTest.coarseEncryptGrantAllowsReusingAnotherAppsDekName
on the Java side.
"""

from __future__ import annotations

import base64
import json
import os
import uuid

import pytest
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from hsm_databricks_udf import dek_manager, transport, udf
from hsm_databricks_udf.config import Config
from hsm_databricks_udf.svc_client import SvcClient

BASE_URL = os.environ.get("HSM_LIVE_TEST_BASE_URL")
pytestmark = pytest.mark.skipif(not BASE_URL, reason="set HSM_LIVE_TEST_BASE_URL to run against a live hsm-core-service")


def _headers(token: str, app_id: str, full: bool = False) -> dict:
    h = {"Authorization": f"Bearer {token}", "X-App-ID": app_id, "Content-Type": "application/json"}
    if full:
        h["X-Response-Detail"] = "full"
    return h


@pytest.fixture(scope="module")
def keypair_and_registered_app():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()

    resp = requests.post(f"{BASE_URL}/admin/apps/keys",
                          headers=_headers("demo-token-ops-admin", "ops-admin"),
                          json={"app_id": "payments-svc", "encryption_public_key_pem": public_pem})
    assert resp.status_code == 200, resp.text
    return private_key


def test_python_encrypted_token_decrypts_via_real_java_endpoint(keypair_and_registered_app):
    private_key = keypair_and_registered_app
    dek_name = f"live-interop.{uuid.uuid4()}"

    resp = requests.post(f"{BASE_URL}/dek/issue",
                          headers=_headers("demo-token-payments-svc", "payments-svc"),
                          json={"items": [{"key": "1", "name": dek_name, "data_classification": None}]})
    assert resp.status_code == 200, resp.text
    item = resp.json()["items"][0]
    assert item["status"] == "success"
    assert item["owner_app_id"] == "payments-svc"

    raw_dek = transport.unwrap(base64.b64decode(item["wrapped_dek_b64"]), private_key)
    plaintext = b"encrypted entirely in python, never touched the JVM"
    result = dek_manager.encrypt(plaintext, bytearray(raw_dek), item["owner_app_id"])
    token = dek_manager.pack_token(uuid.UUID(item["edek_id"]), result.iv, result.tag, result.ciphertext)

    resp = requests.post(f"{BASE_URL}/decrypt",
                          headers=_headers("demo-token-payments-svc", "payments-svc"),
                          json={"ciphertext": token})
    assert resp.status_code == 200, resp.text
    assert resp.json()["plaintext"] == plaintext.decode()


def test_java_encrypted_token_decrypts_via_python(keypair_and_registered_app):
    private_key = keypair_and_registered_app
    dek_name = f"live-interop.{uuid.uuid4()}"
    plaintext = "encrypted by java, decrypted by python"

    resp = requests.post(f"{BASE_URL}/encrypt",
                          headers=_headers("demo-token-payments-svc", "payments-svc", full=True),
                          json={"plaintext": plaintext, "dek_name": dek_name})
    assert resp.status_code == 201, resp.text
    java_token = resp.json()["ciphertext"]
    edek_id = resp.json()["edek_id"]

    resp = requests.post(f"{BASE_URL}/dek/unwrap",
                          headers=_headers("demo-token-payments-svc", "payments-svc"),
                          json={"items": [{"key": "1", "edek_id": edek_id}]})
    assert resp.status_code == 200, resp.text
    item = resp.json()["items"][0]
    assert item["owner_app_id"] == "payments-svc"

    raw_dek = transport.unwrap(base64.b64decode(item["wrapped_dek_b64"]), private_key)
    unpacked = dek_manager.unpack_token(java_token)
    decrypted = dek_manager.decrypt(unpacked.ciphertext, unpacked.tag, unpacked.iv, bytearray(raw_dek), item["owner_app_id"])
    assert decrypted.decode() == plaintext


def test_dek_issue_reuse_reports_correct_owner(keypair_and_registered_app):
    dek_name = f"live-interop.{uuid.uuid4()}"

    first = requests.post(f"{BASE_URL}/dek/issue",
                           headers=_headers("demo-token-payments-svc", "payments-svc"),
                           json={"items": [{"key": "1", "name": dek_name, "data_classification": None}]}).json()["items"][0]
    assert first["reused"] is False
    assert first["owner_app_id"] == "payments-svc"

    second = requests.post(f"{BASE_URL}/dek/issue",
                            headers=_headers("demo-token-payments-svc", "payments-svc"),
                            json={"items": [{"key": "1", "name": dek_name, "data_classification": None}]}).json()["items"][0]
    assert second["reused"] is True
    assert second["edek_id"] == first["edek_id"]
    assert second["owner_app_id"] == "payments-svc"


def test_self_signed_jwt_accepted_by_real_server_end_to_end(monkeypatch):
    """
    Proves SelfSignedJwtTokenProvider's output is genuinely accepted by
    hsm-core-service's real SelfSignedAppKeyJwtValidator -- not just
    structurally correct in isolation (see test_auth.py for that). Registers
    a DEDICATED signing key (separate from the DEK-transport key, exercising
    the non-fallback path) via POST /admin/apps/keys, then drives the actual
    SvcClient/Config classes end to end -- the exact code path
    udf.py._ensure_initialized() uses -- with HSM_AUTH_MODE=SELF_SIGNED_JWT,
    no static demo token anywhere in this test.
    """
    signing_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    signing_public_pem = signing_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    signing_private_pem = signing_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    resp = requests.post(f"{BASE_URL}/admin/apps/keys",
                          headers=_headers("demo-token-ops-admin", "ops-admin"),
                          json={"app_id": "payments-svc", "signing_public_key_pem": signing_public_pem})
    assert resp.status_code == 200, resp.text
    assert resp.json()["has_signing_key"] is True

    # Separate DEK-transport keypair -- SELF_SIGNED_JWT auth and the
    # DEK-transport key are independent concerns, same as on the JVM side.
    transport_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    transport_public_pem = transport_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    transport_private_pem = transport_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    resp = requests.post(f"{BASE_URL}/admin/apps/keys",
                          headers=_headers("demo-token-ops-admin", "ops-admin"),
                          json={"app_id": "payments-svc", "encryption_public_key_pem": transport_public_pem})
    assert resp.status_code == 200, resp.text

    monkeypatch.setenv("HSM_SERVICE_BASE_URL", BASE_URL)
    monkeypatch.setenv("HSM_APP_ID", "payments-svc")
    monkeypatch.setenv("HSM_AUTH_MODE", "SELF_SIGNED_JWT")
    monkeypatch.setenv("HSM_PRIVATE_KEY_PEM", transport_private_pem)
    monkeypatch.setenv("HSM_SIGNING_PRIVATE_KEY_PEM", signing_private_pem)

    config = Config.from_env()
    assert config.auth_mode == "SELF_SIGNED_JWT"
    svc_client = SvcClient(config)

    dek_name = f"self-signed-jwt-interop.{uuid.uuid4()}"
    result = svc_client.issue_dek(dek_name)
    assert result.owner_app_id == "payments-svc"

    raw_dek = transport.unwrap(base64.b64decode(result.wrapped_dek_b64), transport_key)
    assert len(raw_dek) == 32


def test_udf_encrypt_decrypt_via_credentials_json_against_real_server():
    """
    Drives udf.encrypt()/udf.decrypt() through their actual public signature
    -- credentials_json as an explicit argument -- against a real
    hsm-core-service instance. This is the exact call shape
    sql/create_functions.sql now uses (see DEPLOYMENT.md): dbutils cannot run
    inside a Unity Catalog Python Function body (confirmed against
    Databricks' own docs/KB, not assumed), so credentials_json is built by
    the CALLER (a notebook/job, where dbutils does work) and passed in per
    call, never resolved by the function itself.

    Registers its own dedicated keypair rather than reusing the module-scoped
    keypair_and_registered_app fixture -- other tests in this module
    re-register payments-svc's encryption_public_key_pem with a different
    key (e.g. test_self_signed_jwt_accepted_by_real_server_end_to_end), so
    relying on that shared fixture here would be order-dependent.
    """
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    resp = requests.post(f"{BASE_URL}/admin/apps/keys",
                          headers=_headers("demo-token-ops-admin", "ops-admin"),
                          json={"app_id": "payments-svc", "encryption_public_key_pem": public_pem})
    assert resp.status_code == 200, resp.text

    credentials_json = json.dumps({
        "HSM_SERVICE_BASE_URL": BASE_URL,
        "HSM_APP_ID": "payments-svc",
        "HSM_AUTH_MODE": "STATIC",
        "HSM_PRIVATE_KEY_PEM": private_pem,
        "HSM_BEARER_TOKEN": "demo-token-payments-svc",
    })
    dek_name = f"udf-credentials-json-interop.{uuid.uuid4()}"

    token = udf.encrypt("value from a real UDF call", dek_name, None, credentials_json)
    assert udf.decrypt(token, credentials_json) == "value from a real UDF call"
