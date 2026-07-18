from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Azure Key Vault — regular vault (vault.azure.net) that holds CEK secrets
    azure_keyvault_secret_url: str  # https://<name>.vault.azure.net/

    # Secret names for the two CEK slots
    cek_alpha_secret_name: str = "cek-alpha"
    cek_beta_secret_name: str = "cek-beta"

    # Secret whose value is "alpha" or "beta" — points to the active slot
    current_key_secret_name: str = "cek-current-key"

    # How often the rotation loop fires (hours)
    rotation_interval_hours: int = 4

    # ── Redis post-rotation ops ───────────────────────────────────────────────
    # redis_url: same URL the main service uses (rediss:// for TLS).
    # Empty → skip all Redis ops (safe default; ops are best-effort).
    redis_url: str = ""

    # What to do with existing DEK cache entries after flipping current_key:
    #   none   — do nothing; pods drain naturally via 60s TTL.
    #   flush  — delete all dek:* keys; pods take a MISS storm then re-warm.
    #   rekey  — decrypt old-slot entries, re-encrypt under new CEK, rewrite
    #            under new key; pods find entries already migrated — no MISS storm.
    redis_post_rotation_mode: Literal["none", "flush", "rekey"] = "rekey"

    # TTL (seconds) used when re-keying an entry whose original TTL is unknown.
    # Should match dek_cache_ttl_seconds on the main service (default 60).
    dek_cache_ttl_seconds: int = 60
