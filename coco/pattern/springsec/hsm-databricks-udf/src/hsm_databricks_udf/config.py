"""
Configuration. Two sources:

  - Config.from_json(credentials_json) -- the one Unity Catalog Python
    Functions actually use (see sql/create_functions.sql, udf.py). Databricks
    confirms dbutils only works in the calling notebook/job's own driver
    context, never inside a UDF/function body (any LANGUAGE PYTHON function,
    this package's CREATE FUNCTION included) -- so credentials can't be
    resolved by calling dbutils.secrets.get(...) from inside the function.
    The caller fetches the secret where dbutils *does* work and passes the
    whole config in as a JSON argument instead -- Databricks' own documented
    workaround for this exact failure. See DEPLOYMENT.md for the calling
    pattern and citations.
  - Config.from_env() -- for local testing and direct, non-UDF use of this
    package (e.g. a notebook cell that imports hsm_databricks_udf directly,
    where the calling code's own process env is trustworthy).

Both read the same field names; from_json just pulls them from a parsed JSON
object instead of os.environ.

HSM_AUTH_MODE selects the auth mode -- STATIC (default) or SELF_SIGNED_JWT,
mirroring a subset of hsm-crypto-client's SvcConfig.AuthMode. AZURE_AD and
MTLS are not implemented here yet, see DATABRICKS_UDF_DESIGN.md §14.

HSM_API_VERSION selects hsm-core-service's API version -- V1 (default,
snake_case JSON) or V2 (camelCase JSON). HSM_SERVICE_BASE_URL must end in
the matching prefix (.../v1 or .../v2), and a URL ending in the other one is
refused; same endpoints, same data.

HSM_CA_CERT_PEM (optional) is the CA certificate bundle (PEM, or base64 of
PEM) that issued hsm-core-service's TLS certificate, for endpoints whose
certificate comes from an internal/corporate CA. It is trusted in addition to
the public CAs requests already trusts; certificate verification is never
turned off. It has to travel in credentials_json like everything else: Unity
Catalog functions don't see REQUESTS_CA_BUNDLE or certificates installed on
the cluster.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlsplit

from cryptography import x509

_MISSING = object()


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Config:
    base_url: str                          # e.g. "https://hsm-core-service.internal:8443/api/sensec/hsm/v1"
    app_id: str                             # X-App-ID header value; must match the bearer token's app_id/sub claim
    auth_mode: str                          # "STATIC" or "SELF_SIGNED_JWT"
    private_key_pem: str                    # this package's own DEK-transport RSA private key, PKCS#8 PEM; never sent to hsm-core-service
    bearer_token: str | None = None         # STATIC only
    signing_private_key_pem: str | None = None   # SELF_SIGNED_JWT only -- may be the same PEM as private_key_pem (legacy one-keypair fallback, same as HsmCryptoClient.Builder's)
    self_signed_audience: str = "hsm-core-service"   # SELF_SIGNED_JWT only -- must match hsm-core-service's hsm.jwt.audience (or one entry in its comma-separated list)
    request_timeout_seconds: float = 10.0
    api_version: str = "V1"                 # "V1" (snake_case) or "V2" (camelCase); base_url must be that version's prefix
    ca_cert_pem: str | None = None          # extra CA bundle (PEM) to trust for base_url's TLS certificate; None = public CAs only

    @classmethod
    def from_env(cls) -> "Config":
        return cls._build(os.environ.get, source_hint="environment variable")

    @classmethod
    def from_json(cls, credentials_json: str) -> "Config":
        """
        credentials_json is a JSON object with the same field names as the
        HSM_* environment variables, e.g.:
            {"HSM_SERVICE_BASE_URL": "...", "HSM_APP_ID": "...",
             "HSM_AUTH_MODE": "STATIC", "HSM_PRIVATE_KEY_PEM": "...",
             "HSM_BEARER_TOKEN": "..."}
        See DEPLOYMENT.md for how the caller builds this (fetched via
        dbutils.secrets.get(...) in the caller's own notebook/job, never
        hardcoded in SQL text).
        """
        try:
            data = json.loads(credentials_json)
        except (json.JSONDecodeError, TypeError) as e:
            raise ConfigError(f"credentials_json is not valid JSON: {e}") from e
        if not isinstance(data, dict):
            raise ConfigError("credentials_json must decode to a JSON object")
        return cls._build(data.get, source_hint="credentials_json field")

    @classmethod
    def _build(cls, get: Callable[[str], object], source_hint: str) -> "Config":
        base_url = _require(get, "HSM_SERVICE_BASE_URL", source_hint)
        app_id = _require(get, "HSM_APP_ID", source_hint)
        auth_mode = str(get("HSM_AUTH_MODE") or "STATIC").strip().upper()
        private_key_pem = _require(get, "HSM_PRIVATE_KEY_PEM", source_hint)
        timeout = float(get("HSM_REQUEST_TIMEOUT_SECONDS") or 10.0)
        api_version = str(get("HSM_API_VERSION") or "V1").strip().upper()
        if api_version not in ("V1", "V2"):
            raise ConfigError(f"HSM_API_VERSION must be V1 or V2, got '{api_version}'")
        _check_version_matches_url(str(base_url), api_version)
        ca_raw = get("HSM_CA_CERT_PEM")
        ca_cert_pem = parse_ca_bundle(ca_raw) if ca_raw else None

        if auth_mode == "STATIC":
            bearer_token = _require(get, "HSM_BEARER_TOKEN", source_hint)
            return cls(
                base_url=str(base_url).rstrip("/"), app_id=app_id, auth_mode=auth_mode,
                private_key_pem=private_key_pem, bearer_token=bearer_token,
                request_timeout_seconds=timeout, api_version=api_version,
                ca_cert_pem=ca_cert_pem,
            )
        elif auth_mode == "SELF_SIGNED_JWT":
            # HSM_BEARER_TOKEN is STATIC-only and never read here -- this mode
            # mints its own bearer token (a JWT) locally from the signing key,
            # never accepts one as input.
            #
            # Falls back to the DEK-transport key PEM if HSM_SIGNING_PRIVATE_KEY_PEM
            # is genuinely ABSENT -- the same legacy one-keypair switch
            # AppRegistryService.getSigningPublicKey (server) and
            # HsmCryptoClient.Builder (JVM client) both support. But a
            # present-and-empty value is NOT treated the same as absent: that
            # almost always means a wiring mistake upstream (e.g. hsm_credentials()
            # still populating HSM_BEARER_TOKEN from a STATIC-mode template
            # instead of HSM_SIGNING_PRIVATE_KEY_PEM) that would otherwise
            # silently sign with the wrong key and fail server-side with an
            # opaque "Invalid token signature" instead of a clear config error
            # -- confirmed as a real, live failure mode, not hypothetical.
            signing_raw = get("HSM_SIGNING_PRIVATE_KEY_PEM", _MISSING)
            if signing_raw is _MISSING:
                signing_private_key_pem = private_key_pem
            elif not signing_raw:
                raise ConfigError(
                    "HSM_SIGNING_PRIVATE_KEY_PEM was provided but empty (as a "
                    f"{source_hint}). This almost always means the wrong secret "
                    "was wired into it -- check hsm_credentials() is using the "
                    "SELF_SIGNED_JWT template (HSM_SIGNING_PRIVATE_KEY_PEM), not "
                    "the STATIC one (HSM_BEARER_TOKEN, which SELF_SIGNED_JWT "
                    "never reads). See DEPLOYMENT.md."
                )
            else:
                signing_private_key_pem = signing_raw
            audience = get("HSM_SELF_SIGNED_AUDIENCE") or "hsm-core-service"
            return cls(
                base_url=str(base_url).rstrip("/"), app_id=app_id, auth_mode=auth_mode,
                private_key_pem=private_key_pem, signing_private_key_pem=signing_private_key_pem,
                self_signed_audience=audience, request_timeout_seconds=timeout,
                api_version=api_version, ca_cert_pem=ca_cert_pem,
            )
        else:
            raise ConfigError(
                f"HSM_AUTH_MODE must be STATIC or SELF_SIGNED_JWT, got '{auth_mode}'. "
                f"AZURE_AD and MTLS aren't implemented in this package yet -- see "
                f"DATABRICKS_UDF_DESIGN.md §14."
            )


def _check_version_matches_url(base_url: str, api_version: str) -> None:
    """
    Refuses a URL whose last path segment is the other version. Sending V2 field
    names to a v1 URL would otherwise half work: v1 ignores unknown fields, so
    /dek/issue succeeds with data_classification silently lost, while /dek/unwrap
    fails. A URL that doesn't end in v1 or v2 (a custom gateway path) can't be
    checked and is accepted.
    """
    last_segment = urlsplit(base_url).path.rstrip("/").rsplit("/", 1)[-1].lower()
    if last_segment in ("v1", "v2") and last_segment.upper() != api_version:
        raise ConfigError(
            f"HSM_API_VERSION is {api_version} but HSM_SERVICE_BASE_URL ends in /{last_segment}: "
            "change both together. See DEPLOYMENT.md \"Switching to API v2\"."
        )


def parse_ca_bundle(value: object) -> str:
    """
    HSM_CA_CERT_PEM as normalized PEM text: one or more CERTIFICATE blocks, given
    as raw PEM or as base64 of PEM (same two forms the key fields accept). Refuses
    anything containing a private key -- a CA bundle never does, so that's the
    wrong file -- and anything that doesn't parse as certificates.
    """
    raw = value.encode() if isinstance(value, str) else bytes(value)
    if b"-----BEGIN" not in raw:
        try:
            raw = base64.b64decode(raw, validate=True)
        except (binascii.Error, ValueError) as e:
            raise ConfigError(f"HSM_CA_CERT_PEM is neither PEM nor base64 of PEM: {e}") from e
    if b"PRIVATE KEY-----" in raw:
        raise ConfigError(
            "HSM_CA_CERT_PEM contains a private key. It must hold only the CA certificate(s) "
            "(-----BEGIN CERTIFICATE-----) that issued the service's TLS certificate. See DEPLOYMENT.md."
        )
    if b"-----BEGIN CERTIFICATE-----" not in raw:
        raise ConfigError("HSM_CA_CERT_PEM has no '-----BEGIN CERTIFICATE-----' block. See DEPLOYMENT.md.")
    try:
        certs = x509.load_pem_x509_certificates(raw)
    except ValueError as e:
        raise ConfigError(f"HSM_CA_CERT_PEM does not parse as PEM certificates: {e}") from e
    if not certs:
        raise ConfigError("HSM_CA_CERT_PEM contains no certificates. See DEPLOYMENT.md.")
    return raw.decode("ascii", errors="strict")


def _require(get: Callable[[str], object], name: str, source_hint: str) -> str:
    value = get(name)
    if not value:
        raise ConfigError(f"{name} is not set (expected as a {source_hint}). See DEPLOYMENT.md.")
    return value
