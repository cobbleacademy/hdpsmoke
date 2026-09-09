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
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Callable


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

        if auth_mode == "STATIC":
            bearer_token = _require(get, "HSM_BEARER_TOKEN", source_hint)
            return cls(
                base_url=str(base_url).rstrip("/"), app_id=app_id, auth_mode=auth_mode,
                private_key_pem=private_key_pem, bearer_token=bearer_token,
                request_timeout_seconds=timeout,
            )
        elif auth_mode == "SELF_SIGNED_JWT":
            # Falls back to the DEK-transport key PEM if no dedicated signing key
            # is set -- the same legacy one-keypair switch AppRegistryService.
            # getSigningPublicKey (server) and HsmCryptoClient.Builder (JVM
            # client) both support.
            signing_private_key_pem = get("HSM_SIGNING_PRIVATE_KEY_PEM") or private_key_pem
            audience = get("HSM_SELF_SIGNED_AUDIENCE") or "hsm-core-service"
            return cls(
                base_url=str(base_url).rstrip("/"), app_id=app_id, auth_mode=auth_mode,
                private_key_pem=private_key_pem, signing_private_key_pem=signing_private_key_pem,
                self_signed_audience=audience, request_timeout_seconds=timeout,
            )
        else:
            raise ConfigError(
                f"HSM_AUTH_MODE must be STATIC or SELF_SIGNED_JWT, got '{auth_mode}'. "
                f"AZURE_AD and MTLS aren't implemented in this package yet -- see "
                f"DATABRICKS_UDF_DESIGN.md §14."
            )


def _require(get: Callable[[str], object], name: str, source_hint: str) -> str:
    value = get(name)
    if not value:
        raise ConfigError(f"{name} is not set (expected as a {source_hint}). See DEPLOYMENT.md.")
    return value
