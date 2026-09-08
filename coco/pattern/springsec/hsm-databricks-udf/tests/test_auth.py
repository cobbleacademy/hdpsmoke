"""
Self-consistency tests for SelfSignedJwtTokenProvider -- no network. Real
cross-implementation proof (this Python-minted token actually accepted by
hsm-core-service's real SelfSignedAppKeyJwtValidator) lives in
test_live_interop.py.
"""

import base64
import json
import time

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from hsm_databricks_udf.auth import SelfSignedJwtTokenProvider, StaticTokenProvider


def _generate_keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


def _decode_jwt_parts(token: str):
    header_b64, claims_b64, sig_b64 = token.split(".")

    def pad(s: str) -> str:
        return s + "=" * (-len(s) % 4)

    header = json.loads(base64.urlsafe_b64decode(pad(header_b64)))
    claims = json.loads(base64.urlsafe_b64decode(pad(claims_b64)))
    signature = base64.urlsafe_b64decode(pad(sig_b64))
    signing_input = f"{header_b64}.{claims_b64}".encode("ascii")
    return header, claims, signature, signing_input


def test_static_token_provider_returns_fixed_token():
    provider = StaticTokenProvider("demo-token-payments-svc")
    assert provider.get_bearer_token() == "demo-token-payments-svc"
    assert provider.get_bearer_token() == "demo-token-payments-svc"


def test_self_signed_jwt_has_correct_header_and_claims():
    private_key, _ = _generate_keypair()
    provider = SelfSignedJwtTokenProvider(private_key, "payments-svc", "hsm-core-service")

    token = provider.get_bearer_token()
    header, claims, _, _ = _decode_jwt_parts(token)

    assert header == {"alg": "RS256", "typ": "JWT"}
    assert claims["sub"] == "payments-svc"
    assert claims["iss"] == "payments-svc"
    assert claims["aud"] == "hsm-core-service"
    assert "iat" in claims and "exp" in claims and "jti" in claims
    # Well under SelfSignedAppKeyJwtValidator's server-side 5-minute MAX_TTL.
    assert claims["exp"] - claims["iat"] == 120


def test_self_signed_jwt_signature_verifies_against_the_public_key():
    private_key, public_key = _generate_keypair()
    provider = SelfSignedJwtTokenProvider(private_key, "payments-svc")

    token = provider.get_bearer_token()
    _, _, signature, signing_input = _decode_jwt_parts(token)

    # Must not raise -- proves this is genuinely RSASSA-PKCS1-v1_5 + SHA-256
    # (RS256), matching what SelfSignedAppKeyJwtValidator verifies with.
    public_key.verify(signature, signing_input, padding.PKCS1v15(), hashes.SHA256())


def test_self_signed_jwt_signature_rejects_tampered_claims():
    private_key, public_key = _generate_keypair()
    provider = SelfSignedJwtTokenProvider(private_key, "payments-svc")
    token = provider.get_bearer_token()

    header_b64, claims_b64, sig_b64 = token.split(".")
    tampered_claims = json.loads(base64.urlsafe_b64decode(claims_b64 + "=="))
    tampered_claims["sub"] = "a-different-app"
    tampered_b64 = base64.urlsafe_b64encode(
        json.dumps(tampered_claims, separators=(",", ":")).encode()
    ).rstrip(b"=").decode()
    tampered_signing_input = f"{header_b64}.{tampered_b64}".encode("ascii")
    signature = base64.urlsafe_b64decode(sig_b64 + "==")

    with pytest.raises(InvalidSignature):
        public_key.verify(signature, tampered_signing_input, padding.PKCS1v15(), hashes.SHA256())


def test_self_signed_jwt_caches_token_within_refresh_margin():
    private_key, _ = _generate_keypair()
    provider = SelfSignedJwtTokenProvider(private_key, "payments-svc")

    first = provider.get_bearer_token()
    second = provider.get_bearer_token()
    assert first == second  # same process, well within the 2-minute TTL -- must not re-sign


def test_self_signed_jwt_resigns_after_forced_expiry():
    private_key, _ = _generate_keypair()
    provider = SelfSignedJwtTokenProvider(private_key, "payments-svc")

    first = provider.get_bearer_token()
    provider._cached_expiry = time.time() - 1  # force expiry
    second = provider.get_bearer_token()

    assert first != second
    _, claims, _, _ = _decode_jwt_parts(second)
    assert claims["sub"] == "payments-svc"
