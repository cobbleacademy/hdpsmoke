from functools import lru_cache
from typing import Literal

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── Demo mode ─────────────────────────────────────────────────────────────
    # Bypasses Azure Key Vault / Postgres / real JWT validation with in-memory
    # fakes so the service (and its demo UI) can run with zero external deps.
    demo_mode: bool = False

    # ── Azure Key Vault ───────────────────────────────────────────────────────
    azure_keyvault_url: str = ""
    azure_kek_name: str = "hsm-master-kek"
    azure_kek_version: str = ""  # empty → latest

    # ── Database ──────────────────────────────────────────────────────────────
    database_url: str = ""
    demo_database_url: str = "sqlite+aiosqlite:///./demo_hsm.db"   # demo_mode only

    # ── JWT ───────────────────────────────────────────────────────────────────
    jwt_public_key_pem: str = ""
    jwt_jwks_url: str = ""
    jwt_audience: str = "hsm-encryption-service"
    jwt_issuer: str = ""

    @model_validator(mode="after")
    def _require_jwt_source(self) -> "Settings":
        if self.demo_mode:
            return self
        if not self.azure_keyvault_url:
            raise ValueError("AZURE_KEYVAULT_URL is required unless DEMO_MODE=true")
        if not self.database_url:
            raise ValueError("DATABASE_URL is required unless DEMO_MODE=true")
        if not self.jwt_issuer:
            raise ValueError("JWT_ISSUER is required unless DEMO_MODE=true")
        if not self.jwt_public_key_pem and not self.jwt_jwks_url:
            raise ValueError("Either JWT_PUBLIC_KEY_PEM or JWT_JWKS_URL must be set unless DEMO_MODE=true")
        return self

    # ── Service ───────────────────────────────────────────────────────────────
    service_env: Literal["development", "staging", "production"] = "development"
    log_level: str = "INFO"
    api_v1_prefix: str = "/api/sensec/hsm/v1"

    # ── Splunk HEC ────────────────────────────────────────────────────────────
    splunk_enabled: bool = False
    splunk_hec_url: str = ""
    splunk_hec_token: str = ""          # overridden at startup from Key Vault secret
    splunk_index: str = "hsm_audit"
    splunk_source: str = "hsm-encryption-service"
    splunk_sourcetype: str = "_json"
    splunk_verify_ssl: bool = True
    splunk_batch_size: int = 50
    splunk_flush_interval_seconds: int = 5

    @field_validator("splunk_hec_url")
    @classmethod
    def _splunk_url_required_when_enabled(cls, v: str, info) -> str:
        # Validated post-model via model_validator so we have splunk_enabled
        return v

    @model_validator(mode="after")
    def _require_splunk_url(self) -> "Settings":
        if self.splunk_enabled and not self.splunk_hec_url:
            raise ValueError("SPLUNK_HEC_URL is required when SPLUNK_ENABLED=true")
        return self

    # ── KEK Rotation ──────────────────────────────────────────────────────────
    kek_rotation_cron: str = "0 2 1 * *"
    kek_rotation_enabled: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
