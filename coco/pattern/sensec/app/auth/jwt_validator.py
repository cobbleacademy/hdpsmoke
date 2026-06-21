"""
RS256 JWT validation.

Tokens must carry:
  - sub   : caller identity
  - app_id: registered application identifier (custom claim)
  - scope : space-separated list of permitted operations
  - aud   : must match settings.jwt_audience
  - iss   : must match settings.jwt_issuer
"""

from __future__ import annotations

import time
from functools import lru_cache
from typing import Any

import httpx
from jose import JWTError, jwk, jwt
from jose.utils import base64url_decode

from app.config import Settings


class TokenValidationError(Exception):
    pass


class JWTValidator:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._jwks_cache: dict[str, Any] | None = None
        self._jwks_fetched_at: float = 0
        self._jwks_ttl = 3600  # re-fetch JWKS every hour

    def validate(self, token: str) -> dict[str, Any]:
        """Decode and validate a Bearer JWT. Returns claims on success."""
        try:
            header = jwt.get_unverified_header(token)
        except JWTError as exc:
            raise TokenValidationError(f"Malformed token header: {exc}") from exc

        public_key = self._resolve_key(header)

        try:
            claims = jwt.decode(
                token,
                public_key,
                algorithms=["RS256"],
                audience=self._settings.jwt_audience,
                issuer=self._settings.jwt_issuer,
                options={"verify_exp": True, "verify_nbf": True},
            )
        except JWTError as exc:
            raise TokenValidationError(str(exc)) from exc

        if "app_id" not in claims:
            raise TokenValidationError("Missing required claim: app_id")

        return claims

    def _resolve_key(self, header: dict[str, Any]) -> Any:
        if self._settings.jwt_public_key_pem:
            return self._settings.jwt_public_key_pem

        # JWKS path — cache with TTL
        jwks = self._get_jwks()
        kid = header.get("kid")
        for key_data in jwks.get("keys", []):
            if key_data.get("kid") == kid:
                return jwk.construct(key_data)
        raise TokenValidationError(f"No matching key for kid={kid}")

    def _get_jwks(self) -> dict[str, Any]:
        now = time.monotonic()
        if self._jwks_cache and (now - self._jwks_fetched_at) < self._jwks_ttl:
            return self._jwks_cache
        resp = httpx.get(self._settings.jwt_jwks_url, timeout=5)
        resp.raise_for_status()
        self._jwks_cache = resp.json()
        self._jwks_fetched_at = now
        return self._jwks_cache
