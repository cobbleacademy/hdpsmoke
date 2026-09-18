"""
Bearer-token providers for HsmCoreClient (hsm_core_batch_file.py). Covers 3
of hsm-crypto-client's 4 SvcConfig.AuthMode values -- STATIC, SELF_SIGNED_JWT,
AZURE_AD. MTLS is deliberately not ported here: it authenticates at the TLS
transport layer (a client cert/key on the connection itself), not via a
bearer token, so it doesn't fit this module's TokenProvider shape at all --
requests.Session would need its own cert= param wired through HsmCoreClient
separately if that's ever needed.

SELF_SIGNED_JWT below is a straight copy of hsm-databricks-udf's own
auth.py::SelfSignedJwtTokenProvider (same implementation, same verification
against hsm-core-service's SelfSignedAppKeyJwtValidator). Kept duplicated
rather than shared between the two example/reference packages, consistent
with how hsm-databricks-udf already vendors dek_manager.py/transport.py
instead of importing from elsewhere -- if the two ever disagree, that's a
bug in one of them, not an intentional difference.

AZURE_AD is new here (not implemented in hsm-databricks-udf -- see
DATABRICKS_UDF_DESIGN.md Sec.14, deferred there as "out of scope until a
concrete need arises"). Acquires a token via azure-identity's
DefaultAzureCredential by default (environment -> workload identity ->
managed identity -> local-dev fallbacks, in that order -- the same chain
proof-ui's own README documents for its Azure storage access), scoped to
azure_token_scope, which must match whatever hsm-core-service's own Azure AD
app registration exposes as its audience/scope -- see hsm-crypto-client's
SvcConfig.java, whose AzureAdTokenProvider is the reference this ports. This
is the natural fit for a caller that already runs under an Azure identity
(e.g. an Azure Function under its own managed identity) and would rather not
provision or rotate a signing key at all.

Dependencies, only for the mode you actually use:
  STATIC          -- none beyond requests (already required by HsmCoreClient)
  SELF_SIGNED_JWT -- pip install cryptography
  AZURE_AD        -- pip install azure-identity
"""

from __future__ import annotations

import base64
import json
import os
import time
import uuid
from pathlib import Path
from typing import Protocol


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

    Caches the signed token, re-signing only within REFRESH_MARGIN_SECONDS of
    expiry -- signing is cheap, but no reason to re-sign on every single call
    in a tight loop.
    """

    _TOKEN_TTL_SECONDS = 120       # well under SelfSignedAppKeyJwtValidator's server-side 5-minute MAX_TTL
    _REFRESH_MARGIN_SECONDS = 15

    def __init__(self, signing_key, app_id: str, audience: str = "hsm-core-service") -> None:
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
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding

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
        signing_input = f"{_b64url_json(header)}.{_b64url_json(claims)}"
        signature = self._signing_key.sign(
            signing_input.encode("ascii"),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        token = f"{signing_input}.{_b64url(signature)}"
        self._cached_token = token
        self._cached_expiry = expiry
        return token


class AzureAdTokenProvider:
    """
    Acquires a real Azure AD access token via azure-identity, scoped to
    azure_token_scope -- Python port of hsm-crypto-client's own
    AZURE_AD SvcConfig.AuthMode. Defaults to DefaultAzureCredential() --
    tries environment vars, workload identity, managed identity, then a
    handful of local-dev fallbacks (Azure CLI, etc.) in that order, the same
    chain proof-ui's own README documents for its Azure storage access. Pass
    an explicit `credential` to pin one instead (e.g.
    ManagedIdentityCredential(client_id=...) for a specific user-assigned
    identity, useful when more than one is attached to the same resource).

    Caches the acquired token, re-fetching only within REFRESH_MARGIN_SECONDS
    of its own expiry -- azure-identity's credential classes already do their
    own internal caching for most credential types, so this is a cheap
    belt-and-suspenders check on top, not a replacement for it.
    """

    _REFRESH_MARGIN_SECONDS = 60

    def __init__(self, azure_token_scope: str, credential=None) -> None:
        self._scope = azure_token_scope
        self._credential = credential
        self._cached_token: str | None = None
        self._cached_expiry: float = 0.0

    def get_bearer_token(self) -> str:
        now = time.time()
        if self._cached_token is not None and now < self._cached_expiry - self._REFRESH_MARGIN_SECONDS:
            return self._cached_token
        return self._fetch_token()

    def _fetch_token(self) -> str:
        if self._credential is None:
            from azure.identity import DefaultAzureCredential
            self._credential = DefaultAzureCredential()
        access_token = self._credential.get_token(self._scope)
        self._cached_token = access_token.token
        self._cached_expiry = access_token.expires_on
        return self._cached_token


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_json(obj: dict) -> str:
    return _b64url(json.dumps(obj, separators=(",", ":")).encode("utf-8"))


def build_token_provider_from_env(app_id: str) -> TokenProvider:
    """
    Selects and constructs a TokenProvider from HSM_CORE_* env vars, for the
    __main__ demo blocks in hsm_core_batch_file.py/hsm_bulk_file_reader.py --
    mirrors hsm-databricks-udf's Config.from_env()/_build_token_provider(),
    adapted to this directory's own HSM_CORE_* naming. Not required to use
    these providers -- construct one directly for anything this doesn't
    cover (e.g. a credential object your hosting platform already built for
    you, as in an Azure Function under its own bound managed identity).

    HSM_CORE_AUTH_MODE selects the mode -- STATIC (default), SELF_SIGNED_JWT,
    or AZURE_AD. MTLS is not supported here -- see this module's own
    docstring for why.
    """
    auth_mode = os.environ.get("HSM_CORE_AUTH_MODE", "STATIC").strip().upper()

    if auth_mode == "STATIC":
        return StaticTokenProvider(os.environ.get("HSM_CORE_TOKEN", "demo-token-payments-svc"))

    if auth_mode == "SELF_SIGNED_JWT":
        from cryptography.hazmat.primitives import serialization

        pem_path = os.environ.get("HSM_CORE_SIGNING_PRIVATE_KEY_PEM_PATH")
        pem_inline = os.environ.get("HSM_CORE_SIGNING_PRIVATE_KEY_PEM")
        if pem_path:
            pem_bytes = Path(pem_path).read_bytes()
        elif pem_inline:
            pem_bytes = pem_inline.encode("utf-8")
        else:
            raise ValueError(
                "HSM_CORE_AUTH_MODE=SELF_SIGNED_JWT requires either "
                "HSM_CORE_SIGNING_PRIVATE_KEY_PEM_PATH or HSM_CORE_SIGNING_PRIVATE_KEY_PEM"
            )
        signing_key = serialization.load_pem_private_key(pem_bytes, password=None)
        audience = os.environ.get("HSM_CORE_SELF_SIGNED_AUDIENCE", "hsm-core-service")
        return SelfSignedJwtTokenProvider(signing_key, app_id, audience)

    if auth_mode == "AZURE_AD":
        scope = os.environ.get("HSM_CORE_AZURE_TOKEN_SCOPE")
        if not scope:
            raise ValueError("HSM_CORE_AUTH_MODE=AZURE_AD requires HSM_CORE_AZURE_TOKEN_SCOPE")
        return AzureAdTokenProvider(scope)

    raise ValueError(
        f"HSM_CORE_AUTH_MODE must be STATIC, SELF_SIGNED_JWT, or AZURE_AD, got '{auth_mode}'. "
        f"MTLS isn't supported by this module -- see its own docstring."
    )
