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
import os
import uuid

import pytest
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from hsm_databricks_udf import dek_manager, transport

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
