"""
Bearer-token providers for SvcClient. Two modes today, matching a subset of
hsm-crypto-client's SvcConfig.AuthMode (STATIC, AZURE_AD, SELF_SIGNED_JWT,
MTLS) -- AZURE_AD and MTLS are not implemented here yet, see
DATABRICKS_UDF_DESIGN.md §14.

Hand-rolled JWT signing (not PyJWT) -- keeps this package's dependency
footprint to just `cryptography` + `requests`, consistent with why
dek_manager.py/transport.py are vendored rather than pulling in more of
app/'s tree. RS256 is simple enough (RSASSA-PKCS1-v1_5 + SHA-256) that
hand-rolling it isn't a real risk the way hand-rolling AES-GCM would be.
"""

from __future__ import annotations

import base64
import json
import time
import uuid
from typing import Protocol

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa


class TokenProvider(Protocol):
    def get_bearer_token(self) -> str: ...


class StaticTokenProvider:
    """A fixed bearer token, sent as-is on every call. Fine for demo/mock-mode
    tokens (never expire); a real Azure AD JWT here would expire mid-session --
    same caveat hsm-crypto-client's HsmCryptoClient.Builder.staticToken documents."""

    def __init__(self, token: str) -> None:
        self._token = token

    def get_bearer_token(self) -> str:
        return self._token


class SelfSignedJwtTokenProvider:
    """
    Locally signs a short-lived RS256 bearer assertion (RFC 7523-style) with
    this app's own private key instead of acquiring a token from an external
    IdP -- Python port of hsm-crypto-client's SelfSignedJwtTokenProvider.
    "Renewal" is pure local computation, never a network call.

    Matches hsm-core-service's SelfSignedAppKeyJwtValidator exactly (confirmed
    directly against that source, not assumed): RS256 algorithm, `sub`/`iss`
    set to app_id, `aud` must intersect the server's configured
    hsm.jwt.audience list (default "hsm-core-service"), `iat`/`exp` required,
    token lifetime capped well under the server's 5-minute MAX_TTL. The
    server does NOT check `iss` or track `jti` for replay protection (a
    known, documented gap on the server side, not something this client
    needs to compensate for) -- both are still set here for parity with the
    JVM provider and in case that changes later.

    Caches the signed token, re-signing only within REFRESH_MARGIN of expiry
    -- signing is cheap, but no reason to re-sign on every single row in a
    tight per-partition loop.
    """

    _TOKEN_TTL_SECONDS = 120       # well under SelfSignedAppKeyJwtValidator's server-side 5-minute MAX_TTL
    _REFRESH_MARGIN_SECONDS = 15

    def __init__(self, signing_key: rsa.RSAPrivateKey, app_id: str, audience: str = "hsm-core-service") -> None:
        self._signing_key = signing_key
        self._app_id = app_id
        self._audience = audience
        self._cached_token: str | None = None
        self._cached_expiry: float = 0.0

    def get_bearer_token(self) -> str:
        now = time.time()
        if self._cached_token is not None and now < self._cached_expiry - self._REFRESH_MARGIN_SECONDS:
            return self._cached_token
        return self._mint_token(now)

    def _mint_token(self, now: float) -> str:
        expiry = now + self._TOKEN_TTL_SECONDS
        header = {"alg": "RS256", "typ": "JWT"}
        claims = {
            "sub": self._app_id,
            "iss": self._app_id,
            "aud": self._audience,
            "iat": int(now),
            "exp": int(expiry),
            "jti": str(uuid.uuid4()),
        }
        signing_input = (
            f"{_b64url_json(header)}.{_b64url_json(claims)}"
        )
        signature = self._signing_key.sign(
            signing_input.encode("ascii"),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        token = f"{signing_input}.{_b64url(signature)}"
        self._cached_token = token
        self._cached_expiry = expiry
        return token


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_json(obj: dict) -> str:
    return _b64url(json.dumps(obj, separators=(",", ":")).encode("utf-8"))
