"""
Configuration, read from environment variables set on the cluster (job/classic,
shared) or via Databricks secret-backed environment variables (serverless —
see DATABRICKS_UDF_DESIGN.md §7/§11 and DEPLOYMENT.md for how each compute
type actually sets these). Deliberately not read from Spark conf: Unity
Catalog Python Functions don't reliably have SparkContext/SparkConf access
inside the function body the way a plain PySpark UDF does, so environment
variables are the one mechanism guaranteed to work identically everywhere
this package runs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Config:
    base_url: str            # e.g. "https://hsm-core-service.internal:8443/api/sensec/hsm/v1"
    app_id: str               # X-App-ID header value; must match the bearer token's app_id claim
    bearer_token: str         # static token today -- see DATABRICKS_UDF_DESIGN.md §7 for SELF_SIGNED_JWT/mTLS as follow-ups
    private_key_pem: str      # this package's own RSA private key, PKCS#8 PEM; never sent to hsm-core-service
    request_timeout_seconds: float = 10.0

    @classmethod
    def from_env(cls) -> "Config":
        base_url = _require_env("HSM_SERVICE_BASE_URL")
        app_id = _require_env("HSM_APP_ID")
        bearer_token = _require_env("HSM_BEARER_TOKEN")
        private_key_pem = _require_env("HSM_PRIVATE_KEY_PEM")
        timeout = float(os.environ.get("HSM_REQUEST_TIMEOUT_SECONDS", "10.0"))
        return cls(
            base_url=base_url.rstrip("/"),
            app_id=app_id,
            bearer_token=bearer_token,
            private_key_pem=private_key_pem,
            request_timeout_seconds=timeout,
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
