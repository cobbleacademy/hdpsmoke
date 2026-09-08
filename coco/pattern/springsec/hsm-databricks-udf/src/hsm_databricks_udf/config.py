"""
Configuration, read from environment variables set on the cluster (job/classic,
shared) or via a dbutils.secrets.get(...) bootstrap inside the CREATE FUNCTION
body (serverless — see DATABRICKS_UDF_DESIGN.md §7/§11 and DEPLOYMENT.md for
how each compute type actually sets these). Deliberately not read from Spark
conf: Unity Catalog Python Functions don't reliably have SparkContext/SparkConf
access inside the function body the way a plain PySpark UDF does, so
environment variables are the one mechanism guaranteed to work identically
everywhere this package runs.

HSM_AUTH_MODE selects the auth mode -- STATIC (default) or SELF_SIGNED_JWT,
mirroring a subset of hsm-crypto-client's SvcConfig.AuthMode. AZURE_AD and
MTLS are not implemented here yet, see DATABRICKS_UDF_DESIGN.md §14.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


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
        base_url = _require_env("HSM_SERVICE_BASE_URL")
        app_id = _require_env("HSM_APP_ID")
        auth_mode = os.environ.get("HSM_AUTH_MODE", "STATIC").strip().upper()
        private_key_pem = _require_env("HSM_PRIVATE_KEY_PEM")
        timeout = float(os.environ.get("HSM_REQUEST_TIMEOUT_SECONDS", "10.0"))

        if auth_mode == "STATIC":
            bearer_token = _require_env("HSM_BEARER_TOKEN")
            return cls(
                base_url=base_url.rstrip("/"), app_id=app_id, auth_mode=auth_mode,
                private_key_pem=private_key_pem, bearer_token=bearer_token,
                request_timeout_seconds=timeout,
            )
        elif auth_mode == "SELF_SIGNED_JWT":
            # Falls back to the DEK-transport key PEM if no dedicated signing key
            # is set -- the same legacy one-keypair switch AppRegistryService.
            # getSigningPublicKey (server) and HsmCryptoClient.Builder (JVM
            # client) both support.
            signing_private_key_pem = os.environ.get("HSM_SIGNING_PRIVATE_KEY_PEM", private_key_pem)
            audience = os.environ.get("HSM_SELF_SIGNED_AUDIENCE", "hsm-core-service")
            return cls(
                base_url=base_url.rstrip("/"), app_id=app_id, auth_mode=auth_mode,
                private_key_pem=private_key_pem, signing_private_key_pem=signing_private_key_pem,
                self_signed_audience=audience, request_timeout_seconds=timeout,
            )
        else:
            raise ConfigError(
                f"HSM_AUTH_MODE must be STATIC or SELF_SIGNED_JWT, got '{auth_mode}'. "
                f"AZURE_AD and MTLS aren't implemented in this package yet -- see "
                f"DATABRICKS_UDF_DESIGN.md §14."
            )


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(
            f"{name} is not set. On Databricks, set it as a cluster environment "
            f"variable (job/classic, shared clusters) or read it from a "
            f"Databricks secret scope into the environment before the UDF's "
            f"first call (serverless) -- see DEPLOYMENT.md."
        )
    return value
